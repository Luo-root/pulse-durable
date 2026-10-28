// Package filestore provides exclusive local checkpoint sessions backed by an
// immutable snapshot and batches, published by an atomic small head replacement.
// It requires local OS locks and same-directory atomic rename; it does not claim
// power-loss durability, network-filesystem support, or distributed fencing.
package filestore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/internal/runlock"
	"github.com/Luo-root/pulse-durable/recordio"
)

// Store holds local checkpoints. New directories use 0700, files use 0600;
// existing permissions and Windows ACLs remain the caller's responsibility.
type Store struct{ dir string }

var _ durable.Store = (*Store)(nil)

func New(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("filestore: directory is empty")
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(absolute, 0700); err != nil {
		return nil, err
	}
	return &Store{dir: absolute}, nil
}

func (s *Store) Acquire(ctx context.Context, runID string) (durable.Session, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if s == nil || s.dir == "" {
		return nil, errors.New("filestore: store was not created with New")
	}
	if runID == "" || !utf8.ValidString(runID) {
		return nil, errors.New("filestore: nonempty UTF-8 run ID required")
	}
	base := fileBase(runID)
	lock, err := runlock.Acquire(ctx, filepath.Join(s.dir, base+".lock"))
	if err != nil {
		return nil, err
	}
	// Never remove legacy data or adopt unreferenced immutable files.
	if err = removeStaleTemporaries(s.dir, base); err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	return &session{dir: s.dir, path: filepath.Join(s.dir, base+".head"), runID: runID, lock: lock}, nil
}
func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("filestore: context is nil")
	}
	return ctx.Err()
}
func fileBase(runID string) string {
	sum := sha256.Sum256([]byte(runID))
	return hex.EncodeToString(sum[:])
}
func removeStaleTemporaries(dir, base string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		random, ok := strings.CutPrefix(entry.Name(), base+"-")
		if !ok {
			continue
		}
		random, ok = strings.CutSuffix(random, ".tmp")
		if !ok {
			continue
		}
		number, err := strconv.ParseUint(random, 10, 32)
		if err != nil || strconv.FormatUint(number, 10) != random {
			continue
		}
		if err = os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// Explicit bounded replay budget. No implicit compaction or history deletion is
// performed. Once exhausted, writes stop until an explicit maintenance facility
// is available. Both count and bytes are authoritative fields checked on Load.
const maxChainBatches uint64 = 100000
const maxChainBytes uint64 = 256 << 20

type manifest struct {
	Format         int      `json:"format"`
	BaseRevision   uint64   `json:"base_revision"`
	SnapshotDigest [32]byte `json:"snapshot_digest"`
	ChainAnchor    [32]byte `json:"chain_anchor"`
	HeadRevision   uint64   `json:"head_revision"`
	HeadChain      [32]byte `json:"head_chain"`
	BatchBytes     uint64   `json:"batch_bytes"`
	SnapshotID     string   `json:"snapshot_id"`
	HeadID         string   `json:"head_id"`
}
type envelope struct {
	ParentRevision uint64   `json:"parent_revision"`
	ParentID       string   `json:"parent_id"`
	ParentChain    [32]byte `json:"parent_chain"`
	Digest         [32]byte `json:"digest"`
	Chain          [32]byte `json:"chain"`
	Wire           []byte   `json:"wire"`
}
type session struct {
	mu               sync.Mutex
	dir, path, runID string
	lock             *runlock.Lock
	closed           bool
	closeErr         error
	poisoned         bool
	loaded           bool
	head             manifest
	record           durable.RunRecord
}

var _ durable.Session = (*session)(nil)

func (s *session) ready(ctx context.Context) error {
	if s.closed {
		return os.ErrClosed
	}
	if s.poisoned {
		return errors.New("filestore: uncertain session; close and reacquire")
	}
	return checkContext(ctx)
}
func (s *session) legacy() error {
	_, err := os.Lstat(filepath.Join(s.dir, fileBase(s.runID)+".json"))
	if err == nil {
		return errors.New("filestore: unsupported legacy checkpoint format; use a new directory")
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}
func (s *session) readHead() (manifest, error) {
	var h manifest
	if err := s.legacy(); err != nil {
		return h, err
	}
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return h, durable.ErrNotFound
	}
	if err != nil {
		return h, err
	}
	if err = json.Unmarshal(data, &h); err != nil {
		return h, err
	}
	if !validID(h.SnapshotID) || (h.HeadRevision > 1 && !validID(h.HeadID)) || (h.HeadRevision == 1 && h.HeadID != "") || h.Format != 1 || h.BaseRevision != 1 || h.HeadRevision < h.BaseRevision || h.HeadRevision-h.BaseRevision > maxChainBatches || h.BatchBytes > maxChainBytes {
		return h, errors.New("filestore: invalid or unsupported manifest")
	}
	if h.ChainAnchor != durable.SeedChain(h.SnapshotDigest) {
		return h, errors.New("filestore: invalid snapshot chain anchor")
	}
	return h, nil
}
func validID(id string) bool {
	b, err := hex.DecodeString(id)
	return err == nil && len(b) == 16 && hex.EncodeToString(b) == id
}
func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func (s *session) snapshotPath(id string) string {
	return filepath.Join(s.dir, fileBase(s.runID)+".snapshot-"+id)
}
func (s *session) batchPath(id string) string {
	return filepath.Join(s.dir, fileBase(s.runID)+".batch-"+id)
}

// load validates only the files selected by head, never sorted directory entries.
func (s *session) load() (durable.RunRecord, error) {
	h, err := s.readHead()
	if err != nil {
		return durable.RunRecord{}, err
	}
	data, err := os.ReadFile(s.snapshotPath(h.SnapshotID))
	if err != nil {
		return durable.RunRecord{}, err
	}
	if sha256.Sum256(data) != h.SnapshotDigest {
		return durable.RunRecord{}, errors.New("filestore: snapshot digest mismatch")
	}
	r, err := recordio.Decode(data)
	if err != nil {
		return r, err
	}
	if r.RunID != s.runID || r.Revision != h.BaseRevision || r.Format != durable.RecordFormat {
		return r, errors.New("filestore: snapshot identity mismatch")
	}
	if err = durable.ValidateInitialRecord(r, s.runID); err != nil {
		return r, err
	}
	// Follow authenticated parent pointers backwards, then replay consecutively.
	batches := make([]durable.ChangeBatch, int(h.HeadRevision-h.BaseRevision))
	chain := h.HeadChain
	id := h.HeadID
	var total uint64
	for rev := h.HeadRevision; rev > h.BaseRevision; rev-- {
		if !validID(id) {
			return r, errors.New("filestore: invalid batch file identity")
		}
		bytes, err := os.ReadFile(s.batchPath(id))
		if err != nil {
			return r, err
		}
		total += uint64(len(bytes))
		if total > maxChainBytes {
			return r, errors.New("filestore: chain byte budget exceeded")
		}
		var e envelope
		if err = json.Unmarshal(bytes, &e); err != nil {
			return r, err
		}
		digest := sha256.Sum256(e.Wire)
		next, err := durable.NextChain(e.ParentChain, digest, rev)
		if err != nil {
			return r, err
		}
		if e.ParentRevision != rev-1 || e.Digest != digest || e.Chain != chain || next != chain {
			return r, errors.New("filestore: batch chain mismatch")
		}
		b, err := durable.UnmarshalChangeBatch(e.Wire)
		if err != nil {
			return r, err
		}
		canonical, err := durable.MarshalChangeBatch(b)
		if err != nil || string(canonical) != string(e.Wire) {
			return r, errors.New("filestore: noncanonical batch")
		}
		if b.Header().Revision != rev {
			return r, errors.New("filestore: batch revision mismatch")
		}
		batches[int(rev-h.BaseRevision-1)] = b
		chain = e.ParentChain
		id = e.ParentID
	}
	if id != "" || chain != h.ChainAnchor || total != h.BatchBytes {
		return r, errors.New("filestore: incomplete chain")
	}
	for _, b := range batches {
		if _, err = b.ApplyTo(&r); err != nil {
			return r, err
		}
	}
	s.head, s.record, s.loaded = h, r, true
	return r, nil
}
func isolate(r durable.RunRecord) (durable.RunRecord, error) {
	return durable.CloneRecord(r), nil
}
func (s *session) Load(ctx context.Context) (durable.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(ctx); err != nil {
		return durable.RunRecord{}, err
	}
	// A public Load is authoritative even if this session already has a projection.
	r, err := s.load()
	if err != nil {
		s.loaded = false
		return durable.RunRecord{}, err
	}
	if err = checkContext(ctx); err != nil {
		return durable.RunRecord{}, err
	}
	return isolate(r)
}
func (s *session) Initialize(ctx context.Context, r durable.RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(ctx); err != nil {
		return err
	}
	if _, err := s.readHead(); !errors.Is(err, durable.ErrNotFound) {
		if err == nil {
			return durable.ErrConflict
		}
		return err
	}
	if err := durable.ValidateInitialRecord(r, s.runID); err != nil {
		return err
	}
	data, err := recordio.Encode(r)
	if err != nil {
		return err
	}
	// Decode once to detach the caller's input maps and byte slices.
	private, err := recordio.Decode(data)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	anchor := durable.SeedChain(digest)
	id, err := newID()
	if err != nil {
		return err
	}
	h := manifest{Format: 1, BaseRevision: 1, SnapshotDigest: digest, ChainAnchor: anchor, HeadRevision: 1, HeadChain: anchor, SnapshotID: id}
	if err = s.writeImmutable(ctx, s.snapshotPath(id), data); err != nil {
		return err
	}
	if err = s.publish(ctx, h); err != nil {
		return err
	}
	s.head, s.record, s.loaded = h, private, true
	return nil
}
func (s *session) Apply(ctx context.Context, b durable.ChangeBatch) (ack durable.CommitAck, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err = s.ready(ctx); err != nil {
		return ack, err
	}
	// Any failure after touching the private projection fences this session. This
	// avoids a whole-record clone/rollback on every successful checkpoint.
	touched := false
	defer func() {
		if p := recover(); p != nil {
			s.poisoned = true
			panic(p)
		}
		if err != nil && touched {
			s.poisoned = true
		}
	}()
	if !s.loaded {
		if _, err = s.load(); err != nil {
			return ack, err
		}
	}
	h, err := s.readHead()
	if err != nil {
		return ack, err
	}
	if h != s.head || b.Header().ExpectedRevision != h.HeadRevision {
		return ack, durable.ErrConflict
	}
	if h.HeadRevision-h.BaseRevision >= maxChainBatches {
		return ack, errors.New("filestore: chain batch budget exceeded; explicit compaction required")
	}
	wire, err := durable.MarshalChangeBatch(b)
	if err != nil {
		return ack, err
	}
	digest := sha256.Sum256(wire)
	next, err := durable.NextChain(h.HeadChain, digest, b.Header().Revision)
	if err != nil {
		return ack, err
	}
	e := envelope{ParentRevision: h.HeadRevision, ParentChain: h.HeadChain, ParentID: h.HeadID, Digest: digest, Chain: next, Wire: wire}
	data, err := json.Marshal(e)
	if err != nil {
		return ack, err
	}
	if uint64(len(data)) > maxChainBytes-h.BatchBytes {
		return ack, errors.New("filestore: chain byte budget exceeded; explicit compaction required")
	}
	// ApplyTo validates exact typed preconditions before mutating any object.
	ack, err = b.ApplyTo(&s.record)
	if err != nil {
		return durable.CommitAck{}, err
	}
	touched = true
	id, err := newID()
	if err != nil {
		return durable.CommitAck{}, err
	}
	if err = s.writeImmutable(ctx, s.batchPath(id), data); err != nil {
		return durable.CommitAck{}, err
	}
	h.HeadRevision, h.HeadChain, h.HeadID = b.Header().Revision, next, id
	h.BatchBytes += uint64(len(data))
	if err = s.publish(ctx, h); err != nil {
		return durable.CommitAck{}, err
	}
	s.head = h
	return ack, nil
}

// writeImmutable never overwrites or adopts an orphan, even at the same digest.
func (s *session) writeImmutable(ctx context.Context, path string, data []byte) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		return errors.Join(err, f.Close())
	}
	if err = f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	if err = f.Close(); err != nil {
		return err
	}
	return syncDirectory(s.dir)
}
func (s *session) publish(ctx context.Context, h manifest) error {
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(s.dir, fileBase(s.runID)+"-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err != nil {
		return errors.Join(err, f.Close())
	}
	if err = f.Sync(); err != nil {
		return errors.Join(err, f.Close())
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = checkContext(ctx); err != nil {
		return err
	}
	if err = replaceFile(f.Name(), s.path); err != nil {
		return err
	}
	// Once published, later errors are unknown outcomes. No continued writes.
	if err = syncDirectory(s.dir); err != nil {
		s.poisoned = true
		return err
	}
	return nil
}
func (s *session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.closeErr = s.lock.Close()
	return s.closeErr
}

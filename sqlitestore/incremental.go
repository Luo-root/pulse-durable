package sqlitestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

var errCorrupt = errors.New("sqlitestore: corrupt incremental checkpoint")
var errUncertain = errors.New("sqlitestore: session invalidated; close and acquire a new session")

func revision(s string) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n == 0 || strconv.FormatUint(n, 10) != s {
		return 0, errCorrupt
	}
	return n, nil
}

func (s *session) ready(ctx context.Context) error {
	if s.closed {
		return os.ErrClosed
	}
	if s.poisoned {
		return errUncertain
	}
	return checkContext(ctx)
}

// rebuild reads every physical component in one SQLite transaction. No cache is
// published until both integrity checks and the read transaction have succeeded.
func (s *session) rebuild(ctx context.Context) error {
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var rev string
	var head, data, digest, anchor []byte
	err = tx.QueryRowContext(ctx, `SELECT revision, chain FROM durable_heads WHERE run_id=?`, s.runID).Scan(&rev, &head)
	if errors.Is(err, sql.ErrNoRows) {
		var count int
		if err = tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM durable_snapshots WHERE run_id=?) + (SELECT count(*) FROM durable_batches WHERE run_id=?)`, s.runID, s.runID).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return errCorrupt
		}
		return durable.ErrNotFound
	}
	if err != nil {
		return err
	}
	target, err := revision(rev)
	if err != nil || len(head) != 32 || target > maxChainBatches+1 {
		return errCorrupt
	}
	if err = tx.QueryRowContext(ctx, `SELECT record,digest,chain FROM durable_snapshots WHERE run_id=?`, s.runID).Scan(&data, &digest, &anchor); err != nil {
		return fmt.Errorf("%w: snapshot: %v", errCorrupt, err)
	}
	sum := sha256.Sum256(data)
	chain := durable.SeedChain(sum)
	if !bytes.Equal(sum[:], digest) || !bytes.Equal(chain[:], anchor) {
		return errCorrupt
	}
	r, err := recordio.Decode(data)
	if err != nil {
		return fmt.Errorf("%w: %v", errCorrupt, err)
	}
	if err = durable.ValidateInitialRecord(r, s.runID); err != nil {
		return fmt.Errorf("%w: %v", errCorrupt, err)
	}
	rows, err := tx.QueryContext(ctx, `SELECT revision,parent_revision,parent_chain,digest,chain,wire FROM durable_batches WHERE run_id=? ORDER BY length(revision), revision`, s.runID)
	if err != nil {
		return err
	}
	count, total := 0, 0
	for rows.Next() {
		var next, parent string
		var pc, bd, nc, wire []byte
		if err = rows.Scan(&next, &parent, &pc, &bd, &nc, &wire); err != nil {
			rows.Close()
			return err
		}
		n, e := revision(next)
		p, e2 := revision(parent)
		count++
		total += len(wire)
		if e != nil || e2 != nil || p != r.Revision || n != p+1 || n > target || count > maxChainBatches || total > maxChainBytes || !bytes.Equal(pc, chain[:]) {
			rows.Close()
			return errCorrupt
		}
		sum = sha256.Sum256(wire)
		if !bytes.Equal(sum[:], bd) {
			rows.Close()
			return errCorrupt
		}
		batch, e := durable.UnmarshalChangeBatch(wire)
		if e != nil {
			rows.Close()
			return fmt.Errorf("%w: %v", errCorrupt, e)
		}
		nextChain, e := durable.NextChain(chain, sum, n)
		if e != nil || !bytes.Equal(nextChain[:], nc) || batch.Header().Revision != n {
			rows.Close()
			return errCorrupt
		}
		if _, e = batch.ApplyTo(&r); e != nil {
			rows.Close()
			return fmt.Errorf("%w: %v", errCorrupt, e)
		}
		chain = nextChain
	}
	err = errors.Join(rows.Err(), rows.Close())
	if err != nil {
		return err
	}
	if r.Revision != target || !bytes.Equal(chain[:], head) {
		return errCorrupt
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	s.cache = &r
	s.chain = chain
	s.batchCount = count
	s.batchBytes = total
	return nil
}

func (s *session) Load(ctx context.Context) (durable.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(ctx); err != nil {
		return durable.RunRecord{}, err
	}
	if s.cache == nil {
		if err := s.rebuild(ctx); err != nil {
			return durable.RunRecord{}, err
		}
	}
	// Only an explicit Load makes a full isolated copy, never ordinary Apply.
	return durable.CloneRecord(*s.cache), nil
}

func (s *session) Initialize(ctx context.Context, r durable.RunRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(ctx); err != nil {
		return err
	}
	if err := durable.ValidateInitialRecord(r, s.runID); err != nil {
		return err
	}
	data, err := recordio.Encode(r)
	if err != nil {
		return err
	}
	copyRecord, err := recordio.Decode(data)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	chain := durable.SeedChain(sum)
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return saveContextError(ctx, err)
	}
	defer tx.Rollback()
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM durable_heads WHERE run_id=?) + (SELECT count(*) FROM durable_snapshots WHERE run_id=?) + (SELECT count(*) FROM durable_batches WHERE run_id=?)`, s.runID, s.runID, s.runID).Scan(&count); err != nil {
		return saveContextError(ctx, err)
	}
	if count != 0 {
		return durable.ErrConflict
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO durable_snapshots VALUES (?,?,?,?)`, s.runID, data, sum[:], chain[:]); err != nil {
		return saveContextError(ctx, err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO durable_heads VALUES (?,'1',?)`, s.runID, chain[:]); err != nil {
		return saveContextError(ctx, err)
	}
	if err = tx.Commit(); err != nil {
		s.poisoned = true
		s.cache = nil
		return err
	}
	s.cache = &copyRecord
	s.chain = chain
	s.batchCount = 0
	s.batchBytes = 0
	return nil
}

func (s *session) Apply(ctx context.Context, b durable.ChangeBatch) (ack durable.CommitAck, resultErr error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ready(ctx); err != nil {
		return ack, err
	}
	if s.cache == nil {
		if err := s.rebuild(ctx); err != nil {
			return ack, err
		}
	}
	wire, err := durable.MarshalChangeBatch(b)
	if err != nil {
		return ack, err
	}
	if s.batchCount >= maxChainBatches || len(wire) > maxChainBytes-s.batchBytes {
		return ack, errors.New("sqlitestore: chain budget exceeded; explicit compaction required")
	}
	sum := sha256.Sum256(wire)
	chain, err := durable.NextChain(s.chain, sum, b.Header().Revision)
	if err != nil {
		return ack, err
	}
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return ack, saveContextError(ctx, err)
	}
	defer tx.Rollback()
	var rev string
	var current []byte
	if err = tx.QueryRowContext(ctx, `SELECT revision,chain FROM durable_heads WHERE run_id=?`, s.runID).Scan(&rev, &current); err != nil {
		return ack, err
	}
	expected := strconv.FormatUint(s.cache.Revision, 10)
	if rev != expected || !bytes.Equal(current, s.chain[:]) {
		s.poisoned = true
		s.cache = nil
		return ack, durable.ErrConflict
	}
	// ApplyTo checks every typed precondition before changing the private projection.
	// Once mutated, any failure (including panic) invalidates this session. No
	// history clone or rollback projection is needed and no tentative data escapes.
	mutated := false
	defer func() {
		if p := recover(); p != nil {
			s.poisoned = true
			s.cache = nil
			panic(p)
		}
		if mutated && resultErr != nil {
			s.poisoned = true
			s.cache = nil
		}
	}()
	ack, err = b.ApplyTo(s.cache)
	if err != nil {
		return durable.CommitAck{}, err
	}
	mutated = true
	next := strconv.FormatUint(ack.Header.Revision, 10)
	if _, err = tx.ExecContext(ctx, `INSERT INTO durable_batches VALUES (?,?,?,?,?,?,?)`, s.runID, next, expected, s.chain[:], sum[:], chain[:], wire); err != nil {
		return durable.CommitAck{}, saveContextError(ctx, err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE durable_heads SET revision=?,chain=? WHERE run_id=? AND revision=? AND chain=?`, next, chain[:], s.runID, expected, s.chain[:])
	if err != nil {
		return durable.CommitAck{}, saveContextError(ctx, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return durable.CommitAck{}, err
	}
	if n != 1 {
		return durable.CommitAck{}, durable.ErrConflict
	}
	if err = tx.Commit(); err != nil {
		return durable.CommitAck{}, err
	}
	s.chain = chain
	s.batchCount++
	s.batchBytes += len(wire)
	return ack, nil
}

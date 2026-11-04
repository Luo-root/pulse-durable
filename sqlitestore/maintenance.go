package sqlitestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"unicode/utf8"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/internal/runlock"
	"github.com/Luo-root/pulse-durable/recordio"
)

var _ durable.Maintenance = (*Store)(nil)

func maintenanceArgs(ctx context.Context, s *Store, id string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil {
		return os.ErrClosed
	}
	if id == "" || !utf8.ValidString(id) {
		return errors.New("sqlitestore: run ID must be nonempty UTF-8")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.db == nil {
		return os.ErrClosed
	}
	return nil
}
func corrupt(err error) error       { return fmt.Errorf("%w: %w", durable.ErrInvalidRecord, err) }
func toChain(b []byte) (x [32]byte) { copy(x[:], b); return }

// anchorTx 读取原始不可变历史，绝不读取合成快照。
func anchorTx(ctx context.Context, tx *sql.Tx, id string, n uint64) ([32]byte, error) {
	var chain []byte
	if n == 1 {
		var data, digest []byte
		err := tx.QueryRowContext(ctx, `SELECT record,digest,chain FROM durable_snapshots WHERE run_id=? AND revision='1'`, id).Scan(&data, &digest, &chain)
		if err != nil {
			return [32]byte{}, componentError(ctx, err)
		}
		sum := sha256.Sum256(data)
		seed := durable.SeedChain(sum)
		if !bytes.Equal(sum[:], digest) || !bytes.Equal(seed[:], chain) {
			return [32]byte{}, corrupt(errCorrupt)
		}
	} else {
		if err := tx.QueryRowContext(ctx, `SELECT chain FROM durable_batches WHERE run_id=? AND revision=?`, id, strconv.FormatUint(n, 10)).Scan(&chain); err != nil {
			return [32]byte{}, componentError(ctx, err)
		}
	}
	if len(chain) != 32 {
		return [32]byte{}, corrupt(errCorrupt)
	}
	return toChain(chain), nil
}
func componentError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, sql.ErrNoRows) {
		return corrupt(errCorrupt)
	}
	return err
}
func inspectTx(ctx context.Context, tx *sql.Tx, id string) (durable.StorageState, error) {
	var gs, bs, hs string
	var bc, hc []byte
	err := tx.QueryRowContext(ctx, `SELECT m.generation,m.base_revision,m.base_chain,h.revision,h.chain FROM durable_manifests m JOIN durable_heads h ON h.run_id=m.run_id WHERE m.run_id=?`, id).Scan(&gs, &bs, &bc, &hs, &hc)
	if errors.Is(err, sql.ErrNoRows) {
		var n int
		if e := tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM durable_heads WHERE run_id=?)+(SELECT count(*) FROM durable_manifests WHERE run_id=?)+(SELECT count(*) FROM durable_snapshots WHERE run_id=?)+(SELECT count(*) FROM durable_batches WHERE run_id=?)`, id, id, id, id).Scan(&n); e != nil {
			return durable.StorageState{}, e
		}
		if n != 0 {
			return durable.StorageState{}, corrupt(errCorrupt)
		}
		return durable.StorageState{}, durable.ErrNotFound
	}
	if err != nil {
		return durable.StorageState{}, err
	}
	g, e1 := revision(gs)
	b, e2 := revision(bs)
	h, e3 := revision(hs)
	if e1 != nil || e2 != nil || e3 != nil || b > h || len(bc) != 32 || len(hc) != 32 {
		return durable.StorageState{}, corrupt(errCorrupt)
	}
	base, err := anchorTx(ctx, tx, id, b)
	if err != nil {
		return durable.StorageState{}, err
	}
	head, err := anchorTx(ctx, tx, id, h)
	if err != nil {
		return durable.StorageState{}, err
	}
	if base != toChain(bc) || head != toChain(hc) {
		return durable.StorageState{}, corrupt(errCorrupt)
	}
	_, anchor, err := readSnapshotTx(ctx, tx, id, b)
	if err != nil {
		return durable.StorageState{}, err
	}
	if anchor != base {
		return durable.StorageState{}, corrupt(errCorrupt)
	}
	return durable.StorageState{RunID: id, Generation: g, BaseRevision: b, BaseChain: base, HeadRevision: h, HeadChain: head}, nil
}
func readSnapshotTx(ctx context.Context, tx *sql.Tx, id string, n uint64) (durable.RunRecord, [32]byte, error) {
	var data, digest, chain []byte
	if err := tx.QueryRowContext(ctx, `SELECT record,digest,chain FROM durable_snapshots WHERE run_id=? AND revision=?`, id, strconv.FormatUint(n, 10)).Scan(&data, &digest, &chain); err != nil {
		return durable.RunRecord{}, [32]byte{}, componentError(ctx, err)
	}
	sum := sha256.Sum256(data)
	if !bytes.Equal(sum[:], digest) || len(chain) != 32 {
		return durable.RunRecord{}, [32]byte{}, corrupt(errCorrupt)
	}
	r, err := recordio.DecodeSnapshot(data)
	if err != nil {
		return durable.RunRecord{}, [32]byte{}, corrupt(err)
	}
	if err = validateSnapshot(r, id, n); err != nil {
		return durable.RunRecord{}, [32]byte{}, corrupt(err)
	}
	expected, err := anchorTx(ctx, tx, id, n)
	if err != nil {
		return durable.RunRecord{}, [32]byte{}, err
	}
	if expected != toChain(chain) {
		return durable.RunRecord{}, [32]byte{}, corrupt(errCorrupt)
	}
	return r, expected, nil
}
func loadRevisionTx(ctx context.Context, tx *sql.Tx, id string, target uint64, st durable.StorageState) (durable.RevisionImage, int, int, error) {
	var rev string
	// 十进制字符串先按长度、再按字典序比较，保留 uint64 精度。
	ts := strconv.FormatUint(target, 10)
	snapshotTarget := target
	if snapshotTarget > st.BaseRevision {
		snapshotTarget = st.BaseRevision
	}
	ss := strconv.FormatUint(snapshotTarget, 10)
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM durable_snapshots WHERE run_id=? AND (length(revision)<length(?) OR (length(revision)=length(?) AND revision<=?)) ORDER BY length(revision) DESC,revision DESC LIMIT 1`, id, ss, ss, ss).Scan(&rev); err != nil {
		return durable.RevisionImage{}, 0, 0, componentError(ctx, err)
	}
	base, err := revision(rev)
	if err != nil {
		return durable.RevisionImage{}, 0, 0, corrupt(err)
	}
	r, current, err := readSnapshotTx(ctx, tx, id, base)
	if err != nil {
		return durable.RevisionImage{}, 0, 0, err
	}
	expected, err := anchorTx(ctx, tx, id, target)
	if err != nil {
		return durable.RevisionImage{}, 0, 0, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT revision,parent_revision,parent_chain,digest,chain,wire FROM durable_batches WHERE run_id=? AND (length(revision)>length(?) OR (length(revision)=length(?) AND revision>?)) AND (length(revision)<length(?) OR (length(revision)=length(?) AND revision<=?)) ORDER BY length(revision),revision`, id, rev, rev, rev, ts, ts, ts)
	if err != nil {
		return durable.RevisionImage{}, 0, 0, err
	}
	defer rows.Close()
	count, total := 0, 0
	for rows.Next() {
		if err = ctx.Err(); err != nil {
			return durable.RevisionImage{}, 0, 0, err
		}
		var ns, ps string
		var pc, bd, nc, wire []byte
		if err = rows.Scan(&ns, &ps, &pc, &bd, &nc, &wire); err != nil {
			return durable.RevisionImage{}, 0, 0, err
		}
		n, e1 := revision(ns)
		p, e2 := revision(ps)
		count++
		total += len(wire)
		if count > maxChainBatches || total > maxChainBytes {
			return durable.RevisionImage{}, 0, 0, durable.ErrLimitExceeded
		}
		if e1 != nil || e2 != nil || p != r.Revision || p == math.MaxUint64 || n != p+1 || !bytes.Equal(pc, current[:]) {
			return durable.RevisionImage{}, 0, 0, corrupt(errCorrupt)
		}
		sum := sha256.Sum256(wire)
		if !bytes.Equal(sum[:], bd) {
			return durable.RevisionImage{}, 0, 0, corrupt(errCorrupt)
		}
		batch, e := durable.UnmarshalChangeBatch(wire)
		if e != nil {
			return durable.RevisionImage{}, 0, 0, corrupt(e)
		}
		next, e := durable.NextChain(current, sum, n)
		if e != nil || !bytes.Equal(next[:], nc) || batch.Header().Revision != n {
			return durable.RevisionImage{}, 0, 0, corrupt(errCorrupt)
		}
		if _, e = batch.ApplyTo(&r); e != nil {
			return durable.RevisionImage{}, 0, 0, corrupt(e)
		}
		current = next
	}
	if err = rows.Err(); err != nil {
		return durable.RevisionImage{}, 0, 0, err
	}
	if r.Revision != target || current != expected || (target == st.HeadRevision && current != st.HeadChain) {
		return durable.RevisionImage{}, 0, 0, corrupt(errCorrupt)
	}
	return durable.RevisionImage{Record: r, Chain: current}, count, total, nil
}
func (s *Store) InspectStorage(ctx context.Context, id string) (durable.StorageState, error) {
	if err := maintenanceArgs(ctx, s, id); err != nil {
		return durable.StorageState{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return durable.StorageState{}, err
	}
	defer tx.Rollback()
	st, err := inspectTx(ctx, tx, id)
	if err != nil {
		return durable.StorageState{}, err
	}
	if err = tx.Commit(); err != nil {
		return durable.StorageState{}, err
	}
	return st, nil
}
func (s *Store) LoadRevision(ctx context.Context, id string, target uint64) (durable.RevisionImage, error) {
	if err := maintenanceArgs(ctx, s, id); err != nil {
		return durable.RevisionImage{}, err
	}
	if target == 0 {
		return durable.RevisionImage{}, durable.ErrConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return durable.RevisionImage{}, err
	}
	defer tx.Rollback()
	st, err := inspectTx(ctx, tx, id)
	if err != nil {
		return durable.RevisionImage{}, err
	}
	if target > st.HeadRevision {
		return durable.RevisionImage{}, durable.ErrConflict
	}
	image, _, _, err := loadRevisionTx(ctx, tx, id, target, st)
	if err != nil {
		return durable.RevisionImage{}, err
	}
	if err = tx.Commit(); err != nil {
		return durable.RevisionImage{}, err
	}
	return image, nil
}
func (s *Store) CompactStorage(ctx context.Context, req durable.CompactStorageRequest) (result durable.StorageState, resultErr error) {
	if err := maintenanceArgs(ctx, s, req.RunID); err != nil {
		return result, err
	}
	if req.ThroughRevision == 0 || req.ExpectedGeneration == 0 {
		return result, durable.ErrConflict
	}
	sum := sha256.Sum256([]byte(req.RunID))
	lock, err := runlock.Acquire(ctx, filepath.Join(s.locks, fmt.Sprintf("%x.lock", sum)))
	if err != nil {
		return result, err
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	st, err := inspectTx(ctx, tx, req.RunID)
	if err != nil {
		return result, err
	}
	if req.ExpectedGeneration != st.Generation || req.ThroughRevision < st.BaseRevision || req.ThroughRevision > st.HeadRevision {
		return result, durable.ErrConflict
	}
	if st.Generation == math.MaxUint64 {
		return result, durable.ErrLimitExceeded
	}
	image, _, _, err := loadRevisionTx(ctx, tx, req.RunID, req.ThroughRevision, st)
	if err != nil {
		return result, err
	}
	// 只有有效前缀还不够：绝不在损坏后缀上发布维护结果，
	// 即使请求的基线早于业务头。
	if req.ThroughRevision != st.HeadRevision {
		if _, _, _, err = loadRevisionTx(ctx, tx, req.RunID, st.HeadRevision, st); err != nil {
			return result, err
		}
	}
	// 已有快照是不可变锚点。同基线请求验证上方存储的快照，
	// 但不得归一化或替换其字节。
	if req.ThroughRevision != st.BaseRevision {
		data, err := recordio.EncodeSnapshot(image.Record)
		if err != nil {
			return result, err
		}
		digest := sha256.Sum256(data)
		res, err := tx.ExecContext(ctx, `INSERT INTO durable_snapshots (run_id,revision,record,digest,chain) VALUES (?,?,?,?,?) ON CONFLICT(run_id,revision) DO NOTHING`, req.RunID, strconv.FormatUint(req.ThroughRevision, 10), data, digest[:], image.Chain[:])
		if err != nil {
			return result, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return result, err
		}
		if n != 1 {
			return result, durable.ErrConflict
		}
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE durable_manifests SET generation=?,base_revision=?,base_chain=? WHERE run_id=? AND generation=?`, strconv.FormatUint(st.Generation+1, 10), strconv.FormatUint(req.ThroughRevision, 10), image.Chain[:], req.RunID, strconv.FormatUint(st.Generation, 10))
	if err != nil {
		return result, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return result, err
	}
	if n != 1 {
		return result, durable.ErrConflict
	}
	if err = tx.Commit(); err != nil {
		return result, err
	}
	st.Generation++
	st.BaseRevision = req.ThroughRevision
	st.BaseChain = image.Chain
	return st, nil
}

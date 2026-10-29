package postgresstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/jackc/pgx/v5"
)

var _ durable.Maintenance = (*Store)(nil)

// Maintenance deliberately uses the same exclusive advisory lock as business
// sessions. It does not build concurrently with a live business session.
func (s *Store) maintenanceSession(ctx context.Context, id string) (*session, error) {
	a, err := s.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return a.(*session), nil
}

func readState(ctx context.Context, q txQuerier, id string) (durable.StorageState, error) {
	state := durable.StorageState{RunID: id}
	var head, base, generation string
	var hc, bc []byte
	err := q.QueryRow(ctx, `SELECT revision,chain,base_revision,generation FROM pulse_durable.heads WHERE run_id=$1`, id).Scan(&head, &hc, &base, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		var n int
		if err = q.QueryRow(ctx, `SELECT (SELECT count(*) FROM pulse_durable.snapshots WHERE run_id=$1)+(SELECT count(*) FROM pulse_durable.batches WHERE run_id=$1)`, id).Scan(&n); err != nil {
			return state, scrubPGError(err)
		}
		if n != 0 {
			return state, errCorrupt
		}
		return state, durable.ErrNotFound
	}
	if err != nil {
		return state, scrubPGError(err)
	}
	var e1, e2, e3 error
	state.HeadRevision, e1 = parseRevision(head)
	state.BaseRevision, e2 = parseRevision(base)
	state.Generation, e3 = parseRevision(generation)
	if e1 != nil || e2 != nil || e3 != nil || len(hc) != 32 || state.BaseRevision > state.HeadRevision {
		return state, errCorrupt
	}
	if err = q.QueryRow(ctx, `SELECT chain FROM pulse_durable.snapshots WHERE run_id=$1 AND revision=$2`, id, base).Scan(&bc); err != nil {
		return state, errors.Join(errCorrupt, scrubPGError(err))
	}
	if len(bc) != 32 {
		return state, errCorrupt
	}
	// No retention exists: exactly one immutable batch must remain per revision
	// after revision 1. This also detects orphan rows beyond the committed head.
	var batchCount uint64
	if err = q.QueryRow(ctx, `SELECT count(*) FROM pulse_durable.batches WHERE run_id=$1`, id).Scan(&batchCount); err != nil {
		return state, scrubPGError(err)
	}
	if batchCount != state.HeadRevision-1 {
		return state, errCorrupt
	}
	copy(state.HeadChain[:], hc)
	copy(state.BaseChain[:], bc)
	return state, nil
}

// committedAnchor authenticates an exact target independently of the selected
// snapshot. A missing retained component is corruption, never expired history.
func committedAnchor(ctx context.Context, q txQuerier, id string, revision uint64) ([32]byte, error) {
	var chain [32]byte
	var raw []byte
	var err error
	if revision == 1 {
		var data, digest []byte
		err = q.QueryRow(ctx, `SELECT record,digest,chain FROM pulse_durable.snapshots WHERE run_id=$1 AND revision='1'`, id).Scan(&data, &digest, &raw)
		if err == nil {
			sum := sha256.Sum256(data)
			seed := durable.SeedChain(sum)
			if !bytes.Equal(sum[:], digest) || !bytes.Equal(seed[:], raw) {
				return chain, errCorrupt
			}
		}
	} else {
		var parent string
		var pc, digest, wire []byte
		err = q.QueryRow(ctx, `SELECT parent_revision,parent_chain,digest,chain,wire FROM pulse_durable.batches WHERE run_id=$1 AND revision=$2`, id, fmt.Sprint(revision)).Scan(&parent, &pc, &digest, &raw, &wire)
		if err == nil {
			p, e := parseRevision(parent)
			if e != nil || p == math.MaxUint64 || p+1 != revision || len(pc) != 32 {
				return chain, errCorrupt
			}
			sum := sha256.Sum256(wire)
			batch, e := durable.UnmarshalChangeBatch(wire)
			if e != nil {
				return chain, errCorrupt
			}
			canonical, e := durable.MarshalChangeBatch(batch)
			if e != nil || !bytes.Equal(canonical, wire) || !bytes.Equal(sum[:], digest) || batch.Header().RunID != id || batch.Header().Revision != revision {
				return chain, errCorrupt
			}
			var previous [32]byte
			copy(previous[:], pc)
			next, e := durable.NextChain(previous, sum, revision)
			if e != nil || !bytes.Equal(next[:], raw) {
				return chain, errCorrupt
			}
		}
	}
	if err != nil {
		return chain, errors.Join(errCorrupt, scrubPGError(err))
	}
	if len(raw) != 32 {
		return chain, errCorrupt
	}
	copy(chain[:], raw)
	return chain, nil
}

func readImage(ctx context.Context, q txQuerier, state durable.StorageState, target, base uint64) (durable.RevisionImage, int, int, error) {
	var image durable.RevisionImage
	fail := func(err error) (durable.RevisionImage, int, int, error) { return durable.RevisionImage{}, 0, 0, err }
	if target == 0 || target > state.HeadRevision || base == 0 || base > target {
		return fail(durable.ErrConflict)
	}
	var data, digest, anchor []byte
	if err := q.QueryRow(ctx, `SELECT record,digest,chain FROM pulse_durable.snapshots WHERE run_id=$1 AND revision=$2`, state.RunID, fmt.Sprint(base)).Scan(&data, &digest, &anchor); err != nil {
		return fail(errors.Join(errCorrupt, scrubPGError(err)))
	}
	if len(data) > MaxChainBytes {
		return fail(durable.ErrLimitExceeded)
	}
	sum := sha256.Sum256(data)
	expected, err := committedAnchor(ctx, q, state.RunID, base)
	if err != nil {
		return fail(err)
	}
	if !bytes.Equal(sum[:], digest) || !bytes.Equal(expected[:], anchor) {
		return fail(errCorrupt)
	}
	r, err := decodeSnapshot(data)
	if err != nil {
		return fail(errors.Join(errCorrupt, err))
	}
	canonical, err := encodeSnapshot(r)
	if err != nil || !bytes.Equal(canonical, data) || r.RunID != state.RunID || r.Revision != base || r.Format != durable.RecordFormat {
		return fail(errCorrupt)
	}
	// Validate immutable identity against the retained initial snapshot, even when
	// starting at a synthesized revision.
	var initialData []byte
	if err = q.QueryRow(ctx, `SELECT record FROM pulse_durable.snapshots WHERE run_id=$1 AND revision='1'`, state.RunID).Scan(&initialData); err != nil {
		return fail(errors.Join(errCorrupt, scrubPGError(err)))
	}
	initial, err := decodeSnapshot(initialData)
	if err != nil {
		return fail(errCorrupt)
	}
	if err = durable.ValidateInitialRecord(initial, state.RunID); err != nil {
		return fail(errCorrupt)
	}
	if _, err = committedAnchor(ctx, q, state.RunID, 1); err != nil {
		return fail(err)
	}
	if r.Version != initial.Version || r.Definition != initial.Definition || r.Inputs != initial.Inputs || r.RootScope != initial.RootScope {
		return fail(errCorrupt)
	}
	chain := expected
	rows, err := q.Query(ctx, `SELECT revision,parent_revision,parent_chain,digest,chain,wire FROM pulse_durable.batches WHERE run_id=$1 AND (length(revision)>length($2::text) OR (length(revision)=length($2::text) AND revision>$2)) AND (length(revision)<length($3::text) OR (length(revision)=length($3::text) AND revision<=$3)) ORDER BY length(revision),revision`, state.RunID, fmt.Sprint(base), fmt.Sprint(target))
	if err != nil {
		return fail(scrubPGError(err))
	}
	defer rows.Close()
	count, total := 0, 0
	for rows.Next() {
		if err = ctx.Err(); err != nil {
			return fail(err)
		}
		var next, parent string
		var pc, bd, nc, wire []byte
		if err = rows.Scan(&next, &parent, &pc, &bd, &nc, &wire); err != nil {
			return fail(scrubPGError(err))
		}
		n, e1 := parseRevision(next)
		p, e2 := parseRevision(parent)
		count++
		total += len(wire)
		if count > MaxChainBatches || total > MaxChainBytes {
			return fail(durable.ErrLimitExceeded)
		}
		if e1 != nil || e2 != nil || p != r.Revision || p == math.MaxUint64 || n != p+1 || !bytes.Equal(pc, chain[:]) {
			return fail(errCorrupt)
		}
		sum = sha256.Sum256(wire)
		if !bytes.Equal(sum[:], bd) {
			return fail(errCorrupt)
		}
		batch, e := durable.UnmarshalChangeBatch(wire)
		if e != nil {
			return fail(errCorrupt)
		}
		wire2, e := durable.MarshalChangeBatch(batch)
		if e != nil || !bytes.Equal(wire2, wire) {
			return fail(errCorrupt)
		}
		nextChain, e := durable.NextChain(chain, sum, n)
		if e != nil || !bytes.Equal(nextChain[:], nc) || batch.Header().Revision != n {
			return fail(errCorrupt)
		}
		if _, e = batch.ApplyTo(&r); e != nil {
			return fail(errCorrupt)
		}
		chain = nextChain
	}
	if err = rows.Err(); err != nil {
		return fail(scrubPGError(err))
	}
	rows.Close()
	targetChain, err := committedAnchor(ctx, q, state.RunID, target)
	if err != nil {
		return fail(err)
	}
	if r.Revision != target || chain != targetChain || (target == state.HeadRevision && chain != state.HeadChain) {
		return fail(errCorrupt)
	}
	image.Record, image.Chain = r, chain
	return image, count, total, nil
}

func (s *Store) InspectStorage(ctx context.Context, id string) (state durable.StorageState, resultErr error) {
	x, err := s.maintenanceSession(ctx, id)
	if err != nil {
		return state, err
	}
	defer func() { resultErr = errors.Join(resultErr, x.Close()) }()
	x.mu.Lock()
	defer x.mu.Unlock()
	if err = x.ready(ctx); err != nil {
		return state, err
	}
	tx, err := x.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return state, scrubPGError(err)
	}
	defer rollback(tx)
	state, err = readState(ctx, tx, id)
	if err != nil {
		return durable.StorageState{}, err
	}
	if _, _, _, err = readImage(ctx, tx, state, state.HeadRevision, state.BaseRevision); err != nil {
		return durable.StorageState{}, err
	}
	return state, scrubPGError(tx.Commit(ctx))
}

func (s *Store) LoadRevision(ctx context.Context, id string, revision uint64) (image durable.RevisionImage, resultErr error) {
	x, err := s.maintenanceSession(ctx, id)
	if err != nil {
		return image, err
	}
	defer func() { resultErr = errors.Join(resultErr, x.Close()) }()
	x.mu.Lock()
	defer x.mu.Unlock()
	if err = x.ready(ctx); err != nil {
		return image, err
	}
	tx, err := x.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return image, scrubPGError(err)
	}
	defer rollback(tx)
	state, err := readState(ctx, tx, id)
	if err != nil {
		return image, err
	}
	if revision == 0 || revision > state.HeadRevision {
		return image, durable.ErrConflict
	}
	var baseText string
	if err = tx.QueryRow(ctx, `SELECT revision FROM pulse_durable.snapshots WHERE run_id=$1 AND (length(revision)<length($2::text) OR (length(revision)=length($2::text) AND revision<=$2)) ORDER BY length(revision) DESC,revision DESC LIMIT 1`, id, fmt.Sprint(revision)).Scan(&baseText); err != nil {
		return image, errors.Join(errCorrupt, scrubPGError(err))
	}
	base, err := parseRevision(baseText)
	if err != nil {
		return image, errCorrupt
	}
	image, _, _, err = readImage(ctx, tx, state, revision, base)
	if err != nil {
		return durable.RevisionImage{}, err
	}
	return image, scrubPGError(tx.Commit(ctx))
}

func (s *Store) CompactStorage(ctx context.Context, request durable.CompactStorageRequest) (state durable.StorageState, resultErr error) {
	x, err := s.maintenanceSession(ctx, request.RunID)
	if err != nil {
		return state, err
	}
	defer func() { resultErr = errors.Join(resultErr, x.Close()) }()
	x.mu.Lock()
	defer x.mu.Unlock()
	if err = x.ready(ctx); err != nil {
		return state, err
	}
	tx, err := x.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return state, scrubPGError(err)
	}
	defer rollback(tx)
	state, err = readState(ctx, tx, request.RunID)
	if err != nil {
		return durable.StorageState{}, err
	}
	if request.ExpectedGeneration == 0 || request.ExpectedGeneration != state.Generation || request.ThroughRevision < state.BaseRevision || request.ThroughRevision > state.HeadRevision {
		return durable.StorageState{}, durable.ErrConflict
	}
	if state.Generation == math.MaxUint64 {
		return durable.StorageState{}, durable.ErrLimitExceeded
	}
	// Validate the suffix too; synthesis must not bless an inconsistent head.
	if _, _, _, err = readImage(ctx, tx, state, state.HeadRevision, state.BaseRevision); err != nil {
		return durable.StorageState{}, err
	}
	image, _, _, err := readImage(ctx, tx, state, request.ThroughRevision, state.BaseRevision)
	if err != nil {
		return durable.StorageState{}, err
	}
	data, err := encodeSnapshot(image.Record)
	if err != nil {
		return durable.StorageState{}, err
	}
	if len(data) > MaxChainBytes {
		return durable.StorageState{}, durable.ErrLimitExceeded
	}
	sum := sha256.Sum256(data)
	// Same-base synthesis must preserve, not replace, an existing immutable row.
	if request.ThroughRevision != state.BaseRevision {
		if _, err = tx.Exec(ctx, `INSERT INTO pulse_durable.snapshots(run_id,revision,record,digest,chain) VALUES($1,$2,$3,$4,$5)`, state.RunID, fmt.Sprint(request.ThroughRevision), data, sum[:], image.Chain[:]); err != nil {
			return durable.StorageState{}, scrubPGError(err)
		}
	}
	if err = ctx.Err(); err != nil {
		return durable.StorageState{}, err
	}
	tag, err := tx.Exec(ctx, `UPDATE pulse_durable.heads SET base_revision=$1,generation=$2 WHERE run_id=$3 AND generation=$4 AND revision=$5 AND chain=$6`, fmt.Sprint(request.ThroughRevision), fmt.Sprint(state.Generation+1), state.RunID, fmt.Sprint(state.Generation), fmt.Sprint(state.HeadRevision), state.HeadChain[:])
	if err != nil {
		return durable.StorageState{}, scrubPGError(err)
	}
	if tag.RowsAffected() != 1 {
		return durable.StorageState{}, durable.ErrConflict
	}
	if err = tx.Commit(ctx); err != nil {
		return durable.StorageState{}, scrubPGError(err)
	}
	state.Generation++
	state.BaseRevision = request.ThroughRevision
	state.BaseChain = image.Chain
	return state, nil
}

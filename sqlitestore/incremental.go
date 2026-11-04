package sqlitestore

import (
	"bytes"
	"context"
	"crypto/sha256"
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

// rebuild 在一个 SQLite 事务中读取所有物理组件。完整性检查
// 和读取事务均成功前，不发布任何缓存。
func (s *session) rebuild(ctx context.Context) error {
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	st, err := inspectTx(ctx, tx, s.runID)
	if err != nil {
		return err
	}
	image, count, total, err := loadRevisionTx(ctx, tx, s.runID, st.HeadRevision, st)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	s.cache = &image.Record
	s.chain = image.Chain
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
	// 只有显式 Load 创建完整隔离副本，普通 Apply 绝不如此。
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
	data, err := recordio.EncodeSnapshot(r)
	if err != nil {
		return err
	}
	copyRecord, err := recordio.DecodeSnapshot(data)
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
	if _, err = tx.ExecContext(ctx, `INSERT INTO durable_snapshots (run_id,revision,record,digest,chain) VALUES (?,?,?,?,?)`, s.runID, "1", data, sum[:], chain[:]); err != nil {
		return saveContextError(ctx, err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO durable_heads (run_id,revision,chain) VALUES (?,'1',?)`, s.runID, chain[:]); err != nil {
		return saveContextError(ctx, err)
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO durable_manifests (run_id,generation,base_revision,base_chain) VALUES (?,'1','1',?)`, s.runID, chain[:]); err != nil {
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
		return ack, fmt.Errorf("%w: sqlitestore chain budget; explicit compaction required", durable.ErrLimitExceeded)
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
	// ApplyTo 在修改私有投影前检查每个类型化前置条件。
	// 一旦修改，任何失败（包括 panic）均使此会话失效。无须
	// 历史克隆或回滚投影，且不泄露任何暂定数据。
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
	if _, err = tx.ExecContext(ctx, `INSERT INTO durable_batches (run_id,revision,parent_revision,parent_chain,digest,chain,wire) VALUES (?,?,?,?,?,?,?)`, s.runID, next, expected, s.chain[:], sum[:], chain[:], wire); err != nil {
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

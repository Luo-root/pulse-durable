package postgresstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/jackc/pgx/v5"
)

// 这些硬限制约束重放及快照合成后的活跃后缀。
// 不承诺无限流保留。
const MaxChainBatches = 100000
const MaxChainBytes = 256 << 20

var ErrBudget = errors.New("postgresstore: uncompacted chain budget exceeded")
var errCorrupt = fmt.Errorf("postgresstore: corrupt incremental checkpoint: %w", durable.ErrInvalidRecord)
var errUncertain = errors.New("postgresstore: session invalidated; close and acquire again")

const schemaDDL = `CREATE TABLE pulse_durable.snapshots (run_id text NOT NULL, revision text NOT NULL, record bytea NOT NULL, digest bytea NOT NULL, chain bytea NOT NULL, PRIMARY KEY(run_id,revision));
CREATE TABLE pulse_durable.heads (run_id text PRIMARY KEY NOT NULL, revision text NOT NULL, chain bytea NOT NULL, base_revision text NOT NULL, generation text NOT NULL);
CREATE TABLE pulse_durable.batches (run_id text NOT NULL, revision text NOT NULL, parent_revision text NOT NULL, parent_chain bytea NOT NULL, digest bytea NOT NULL, chain bytea NOT NULL, wire bytea NOT NULL, PRIMARY KEY(run_id,revision));`

// 已有模式在精确布局及版本通过检查前仅可读。特别是，
// New 绝不补全、升级或清空 v1 数据库。
func validateSchemaV3(ctx context.Context, q txQuerier) error {
	var count int
	err := q.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pulse_durable' AND c.relkind IN ('r','p','v','m','f','S')`).Scan(&count)
	if err != nil {
		return scrubPGError(err)
	}
	if count != 4 {
		return errors.New("postgresstore: unsupported schema; use a separate new database (no online migration)")
	}
	columns := map[string]map[string]string{
		"schema_meta": {"version": "integer"},
		"snapshots":   {"run_id": "text", "revision": "text", "record": "bytea", "digest": "bytea", "chain": "bytea"},
		"heads":       {"run_id": "text", "revision": "text", "chain": "bytea", "base_revision": "text", "generation": "text"},
		"batches":     {"run_id": "text", "revision": "text", "parent_revision": "text", "parent_chain": "bytea", "digest": "bytea", "chain": "bytea", "wire": "bytea"},
	}
	rows, err := q.Query(ctx, `SELECT table_name,column_name,data_type,is_nullable FROM information_schema.columns WHERE table_schema='pulse_durable'`)
	if err != nil {
		return scrubPGError(err)
	}
	total := 0
	for rows.Next() {
		var table, name, kind, nullable string
		if err = rows.Scan(&table, &name, &kind, &nullable); err != nil {
			rows.Close()
			return scrubPGError(err)
		}
		if columns[table][name] != kind || nullable != "NO" {
			rows.Close()
			return errors.New("postgresstore: incompatible schema columns")
		}
		total++
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return scrubPGError(err)
	}
	if total != 18 {
		return errors.New("postgresstore: incompatible schema columns")
	}
	if err = q.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pulse_durable' AND c.relkind='i'`).Scan(&count); err != nil {
		return scrubPGError(err)
	}
	if count != 3 {
		return errors.New("postgresstore: incompatible schema indexes")
	}
	for table, keys := range map[string][]string{"snapshots": {"run_id", "revision"}, "heads": {"run_id"}, "batches": {"run_id", "revision"}} {
		var actual []string
		err = q.QueryRow(ctx, `SELECT array_agg(a.attname::text ORDER BY k.ord) FROM pg_constraint con JOIN pg_class c ON c.oid=con.conrelid JOIN pg_namespace ns ON ns.oid=c.relnamespace CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY k(num,ord) JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.num WHERE ns.nspname='pulse_durable' AND c.relname=$1 AND con.contype='p'`, table).Scan(&actual)
		if err != nil {
			return scrubPGError(err)
		}
		if len(actual) != len(keys) {
			return errors.New("postgresstore: incompatible primary key")
		}
		for i := range keys {
			if keys[i] != actual[i] {
				return errors.New("postgresstore: incompatible primary key")
			}
		}
	}
	var version int
	if err = q.QueryRow(ctx, `SELECT count(*) FROM pulse_durable.schema_meta`).Scan(&count); err != nil {
		return scrubPGError(err)
	}
	if count != 1 {
		return errors.New("postgresstore: unsupported schema version")
	}
	if err = q.QueryRow(ctx, `SELECT version FROM pulse_durable.schema_meta`).Scan(&version); err != nil {
		return scrubPGError(err)
	}
	if version != schemaVersion {
		return errors.New("postgresstore: unsupported schema version")
	}
	return nil
}

func (x *session) ready(ctx context.Context) error {
	if x.closed {
		return os.ErrClosed
	}
	if x.poisoned {
		return errUncertain
	}
	if ctx == nil {
		return errors.New("postgresstore: nil context")
	}
	return ctx.Err()
}
func (x *session) invalidate() { x.poisoned = true; x.cache = nil }
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

// rebuild 使用单个可重复读快照，包括不存在及孤立对象检查。
func (x *session) rebuild(ctx context.Context) error {
	tx, err := x.conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return scrubPGError(err)
	}
	defer rollback(tx)
	state, err := readState(ctx, tx, x.runID)
	if err != nil {
		return err
	}
	image, count, total, err := readImage(ctx, tx, state, state.HeadRevision, state.BaseRevision)
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return scrubPGError(err)
	}
	x.cache, x.chain = &image.Record, image.Chain
	x.batchCount, x.batchBytes = count, total
	return nil
}

func (x *session) Load(ctx context.Context) (durable.RunRecord, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if err := x.ready(ctx); err != nil {
		return durable.RunRecord{}, err
	}
	// 显式 Load 始终读取权威存储，而非过期缓存头。
	x.cache = nil
	if err := x.rebuild(ctx); err != nil {
		return durable.RunRecord{}, err
	}
	return durable.CloneRecord(*x.cache), nil
}
func (x *session) Initialize(ctx context.Context, r durable.RunRecord) (resultErr error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if err := x.ready(ctx); err != nil {
		return err
	}
	if err := durable.ValidateInitialRecord(r, x.runID); err != nil {
		return err
	}
	data, err := encodeSnapshot(r)
	if err != nil {
		return err
	}
	if len(data) > durable.MaxChangeBatchBytes {
		return ErrBudget
	}
	copyRecord, err := decodeSnapshot(data)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	chain := durable.SeedChain(sum)
	tx, err := x.conn.Begin(ctx)
	if err != nil {
		return scrubPGError(err)
	}
	defer rollback(tx)
	completed := false
	defer func() {
		if !completed {
			x.invalidate()
		}
	}()
	var count int
	if err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM pulse_durable.heads WHERE run_id=$1)+(SELECT count(*) FROM pulse_durable.snapshots WHERE run_id=$1)+(SELECT count(*) FROM pulse_durable.batches WHERE run_id=$1)`, x.runID).Scan(&count); err != nil {
		return scrubPGError(err)
	}
	if count != 0 {
		completed = true
		return durable.ErrConflict
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pulse_durable.snapshots(run_id,revision,record,digest,chain) VALUES($1,'1',$2,$3,$4)`, x.runID, data, sum[:], chain[:]); err != nil {
		return scrubPGError(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pulse_durable.heads(run_id,revision,chain,base_revision,generation) VALUES($1,'1',$2,'1','1')`, x.runID, chain[:]); err != nil {
		return scrubPGError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return scrubPGError(err)
	}
	x.cache = &copyRecord
	x.chain = chain
	x.batchCount = 0
	x.batchBytes = 0
	completed = true
	return nil
}
func (x *session) Apply(ctx context.Context, b durable.ChangeBatch) (ack durable.CommitAck, resultErr error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if err := x.ready(ctx); err != nil {
		return ack, err
	}
	if x.cache == nil {
		if err := x.rebuild(ctx); err != nil {
			return ack, err
		}
	}
	wire, err := durable.MarshalChangeBatch(b)
	if err != nil {
		return ack, err
	}
	if x.batchCount >= MaxChainBatches || len(wire) > MaxChainBytes-x.batchBytes {
		return ack, ErrBudget
	}
	sum := sha256.Sum256(wire)
	chain, err := durable.NextChain(x.chain, sum, b.Header().Revision)
	if err != nil {
		return ack, err
	}
	tx, err := x.conn.Begin(ctx)
	if err != nil {
		return ack, scrubPGError(err)
	}
	defer rollback(tx)
	var rev string
	var current []byte
	if err = tx.QueryRow(ctx, `SELECT revision,chain FROM pulse_durable.heads WHERE run_id=$1 FOR UPDATE`, x.runID).Scan(&rev, &current); err != nil {
		return ack, scrubPGError(err)
	}
	expected := fmt.Sprint(x.cache.Revision)
	if rev != expected || !bytes.Equal(current, x.chain[:]) {
		x.invalidate()
		return ack, durable.ErrConflict
	}
	// 不克隆提案：修改后的所有失败或 panic 均使缓存失效。
	completed := false
	defer func() {
		if !completed {
			x.invalidate()
		}
	}()
	ack, err = b.ApplyTo(x.cache)
	if err != nil {
		completed = true
		return durable.CommitAck{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pulse_durable.batches(run_id,revision,parent_revision,parent_chain,digest,chain,wire) VALUES($1,$2,$3,$4,$5,$6,$7)`, x.runID, fmt.Sprint(b.Header().Revision), expected, x.chain[:], sum[:], chain[:], wire); err != nil {
		return durable.CommitAck{}, scrubPGError(err)
	}
	tag, err := tx.Exec(ctx, `UPDATE pulse_durable.heads SET revision=$1,chain=$2 WHERE run_id=$3 AND revision=$4 AND chain=$5`, fmt.Sprint(b.Header().Revision), chain[:], x.runID, expected, x.chain[:])
	if err != nil {
		return durable.CommitAck{}, scrubPGError(err)
	}
	if tag.RowsAffected() != 1 {
		return durable.CommitAck{}, durable.ErrConflict
	}
	if err = tx.Commit(ctx); err != nil {
		return durable.CommitAck{}, scrubPGError(err)
	}
	x.chain = chain
	x.batchCount++
	x.batchBytes += len(wire)
	completed = true
	return ack, nil
}

// Usage 报告此会话最后验证的头所对应的保留链开销。
// 它绝不加载存储，也不允许失效会话继续写入。
type Usage struct{ Batches, WireBytes, BatchLimit, ByteLimit int }

func (x *session) Usage() Usage {
	x.mu.Lock()
	defer x.mu.Unlock()
	return Usage{x.batchCount, x.batchBytes, MaxChainBatches, MaxChainBytes}
}

package filestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

const manifestFormat = 3
const batchIndexSize = 256

var batchMagic = [8]byte{'P', 'D', 'B', 'A', 'T', 'C', 'H', '3'}

// batchRef 的摘要认证固定路由头；路由头另存 payload 摘要。
// Bytes 是从 origin 到此批的累计物理批字节数，支持不读 suffix 合成。
type batchRef struct {
	Revision uint64   `json:"revision"`
	ID       string   `json:"id"`
	Chain    [32]byte `json:"chain"`
	Digest   [32]byte `json:"digest"`
	Bytes    uint64   `json:"bytes"`
}

type batchIndex struct {
	Parent     batchRef
	Skip       batchRef
	WireDigest [32]byte
	WireSize   uint64
}

type snapshotRef struct {
	Generation uint64   `json:"generation"`
	Revision   uint64   `json:"revision"`
	ID         string   `json:"id"`
	Digest     [32]byte `json:"digest"`
}

type snapshotImage struct {
	ID     string   `json:"id"`
	Digest [32]byte `json:"digest"`
	Base   batchRef `json:"base"`
}

// 每次合成只追加一个常量大小的节点，不复制此前快照目录。
// 两类索引均跳到 n-lowbit(n)，另留紧邻父节点；寻址最坏 O(log² n)，
// 追加构造最坏 O(log n) 元数据读取、摊销 O(1)，不读取跳过的 payload。
type snapshotIndex struct {
	ID         string        `json:"id"`
	Generation uint64        `json:"generation"`
	Image      snapshotImage `json:"image"`
	Previous   snapshotRef   `json:"previous"`
	Skip       snapshotRef   `json:"skip"`
}

func invalidRecord(err error) error { return errors.Join(durable.ErrInvalidRecord, err) }
func indexError(text string) error  { return invalidRecord(errors.New("filestore: " + text)) }
func lowbit(n uint64) uint64        { return n & -n }

func originBatch(h manifest) batchRef {
	return batchRef{Revision: 1, Chain: durable.SeedChain(h.OriginDigest)}
}
func headBatch(h manifest) batchRef {
	return batchRef{Revision: h.HeadRevision, ID: h.HeadID, Chain: h.HeadChain, Digest: h.HeadIndexDigest, Bytes: h.HistoryBytes}
}
func baseSnapshot(h manifest) snapshotImage {
	return snapshotImage{ID: h.SnapshotID, Digest: h.SnapshotDigest, Base: batchRef{Revision: h.BaseRevision, ID: h.BaseID, Chain: h.ChainAnchor, Digest: h.BaseIndexDigest, Bytes: h.HistoryBytes - h.BatchBytes}}
}
func originSnapshot(h manifest) snapshotImage {
	return snapshotImage{ID: h.OriginID, Digest: h.OriginDigest, Base: originBatch(h)}
}
func currentSnapshotRef(h manifest) snapshotRef {
	return snapshotRef{Generation: h.Generation, Revision: h.BaseRevision, ID: h.SnapshotIndexID, Digest: h.SnapshotIndexDigest}
}
func initialSnapshotRef() snapshotRef { return snapshotRef{Generation: 1, Revision: 1} }
func validBatchRef(ref, origin batchRef) bool {
	if ref.Revision == 1 {
		return ref == origin
	}
	return ref.Revision > 1 && validID(ref.ID) && ref.Bytes > batchIndexSize
}
func validSnapshotRef(ref snapshotRef) bool {
	if ref.Generation == 1 {
		return ref == initialSnapshotRef()
	}
	return ref.Generation > 1 && ref.Revision > 1 && validID(ref.ID)
}
func (s *session) snapshotIndexPath(id string) string {
	return filepath.Join(s.dir, fileBase(s.runID)+".history-"+id)
}

func putBatchRef(dst []byte, ref batchRef) {
	binary.BigEndian.PutUint64(dst[:8], ref.Revision)
	if ref.ID != "" {
		id, _ := hex.DecodeString(ref.ID)
		copy(dst[8:24], id)
	}
	copy(dst[24:56], ref.Chain[:])
	copy(dst[56:88], ref.Digest[:])
	binary.BigEndian.PutUint64(dst[88:96], ref.Bytes)
}
func getBatchRef(src []byte) batchRef {
	ref := batchRef{Revision: binary.BigEndian.Uint64(src[:8]), Bytes: binary.BigEndian.Uint64(src[88:96])}
	if ref.Revision != 1 || !bytes.Equal(src[8:24], make([]byte, 16)) {
		ref.ID = hex.EncodeToString(src[8:24])
	}
	copy(ref.Chain[:], src[24:56])
	copy(ref.Digest[:], src[56:88])
	return ref
}
func encodeBatch(id string, index batchIndex, wire []byte) []byte {
	data := make([]byte, batchIndexSize+len(wire))
	copy(data[:8], batchMagic[:])
	identity, _ := hex.DecodeString(id)
	copy(data[8:24], identity)
	putBatchRef(data[24:120], index.Parent)
	putBatchRef(data[120:216], index.Skip)
	copy(data[216:248], index.WireDigest[:])
	binary.BigEndian.PutUint64(data[248:256], index.WireSize)
	copy(data[256:], wire)
	return data
}

// readBatchHeader 只读取固定 256 字节；任何所需路由头损坏均报错。
// 它不把被跳过的 payload 完整性当作本次读取的承诺。
func readBatchHeader(f *os.File, ref, origin batchRef) (index batchIndex, err error) {
	if !validBatchRef(ref, origin) || ref.Revision == 1 {
		return index, indexError("invalid batch reference")
	}
	var data [batchIndexSize]byte
	if _, err = io.ReadFull(f, data[:]); err != nil {
		return index, invalidRecord(err)
	}
	if sha256.Sum256(data[:]) != ref.Digest || !bytes.Equal(data[:8], batchMagic[:]) || hex.EncodeToString(data[8:24]) != ref.ID {
		return index, indexError("batch index digest or identity mismatch")
	}
	index.Parent, index.Skip = getBatchRef(data[24:120]), getBatchRef(data[120:216])
	copy(index.WireDigest[:], data[216:248])
	index.WireSize = binary.BigEndian.Uint64(data[248:256])
	if !validBatchRef(index.Parent, origin) || !validBatchRef(index.Skip, origin) || index.Parent.Revision != ref.Revision-1 || index.Skip.Revision != ref.Revision-lowbit(ref.Revision-1) || index.Skip.Bytes > index.Parent.Bytes || (index.Skip.Revision == index.Parent.Revision && index.Skip != index.Parent) {
		return index, indexError("invalid batch index links")
	}
	if index.WireSize == 0 || index.WireSize > maxChainBytes-batchIndexSize || index.Parent.Bytes > ^uint64(0)-batchIndexSize-index.WireSize || ref.Bytes != index.Parent.Bytes+batchIndexSize+index.WireSize {
		return index, indexError("invalid batch byte accounting")
	}
	chain, err := durable.NextChain(index.Parent.Chain, index.WireDigest, ref.Revision)
	if err != nil || chain != ref.Chain {
		return index, indexError("batch chain mismatch")
	}
	return index, nil
}
func (s *session) readBatchIndex(ref, origin batchRef) (batchIndex, error) {
	if !validID(ref.ID) {
		return batchIndex{}, indexError("invalid batch file identity")
	}
	f, err := os.Open(s.batchPath(ref.ID))
	if err != nil {
		return batchIndex{}, invalidRecord(err)
	}
	defer f.Close()
	return readBatchHeader(f, ref, origin)
}
func (s *session) readBatch(ref, origin batchRef) (b durable.ChangeBatch, index batchIndex, err error) {
	if !validID(ref.ID) {
		return b, index, indexError("invalid batch file identity")
	}
	f, err := os.Open(s.batchPath(ref.ID))
	if err != nil {
		return b, index, invalidRecord(err)
	}
	defer f.Close()
	index, err = readBatchHeader(f, ref, origin)
	if err != nil {
		return b, index, err
	}
	wire, err := io.ReadAll(io.LimitReader(f, int64(index.WireSize)+1))
	if err != nil {
		return b, index, invalidRecord(err)
	}
	if uint64(len(wire)) != index.WireSize || sha256.Sum256(wire) != index.WireDigest {
		return b, index, indexError("batch payload digest or size mismatch")
	}
	b, err = durable.UnmarshalChangeBatch(wire)
	if err != nil {
		return b, index, invalidRecord(err)
	}
	canonical, err := durable.MarshalChangeBatch(b)
	if err != nil || !bytes.Equal(canonical, wire) || b.Header().Revision != ref.Revision {
		return b, index, indexError("noncanonical batch or revision mismatch")
	}
	return b, index, nil
}
func (s *session) locateBatch(ctx context.Context, h manifest, from batchRef, target uint64) (batchRef, error) {
	origin := originBatch(h)
	if target == 1 {
		return origin, nil
	}
	for from.Revision > target {
		if err := checkContext(ctx); err != nil {
			return batchRef{}, err
		}
		index, err := s.readBatchIndex(from, origin)
		if err != nil {
			return batchRef{}, err
		}
		if index.Skip.Revision >= target {
			from = index.Skip
		} else {
			from = index.Parent
		}
	}
	if from.Revision != target || !validBatchRef(from, origin) {
		return batchRef{}, indexError("batch target is not referenced")
	}
	return from, nil
}

func (s *session) readSnapshotIndex(ref snapshotRef, h manifest) (index snapshotIndex, err error) {
	if !validSnapshotRef(ref) || ref.Generation == 1 {
		return index, indexError("invalid snapshot index reference")
	}
	data, err := os.ReadFile(s.snapshotIndexPath(ref.ID))
	if err != nil {
		return index, invalidRecord(err)
	}
	if sha256.Sum256(data) != ref.Digest {
		return index, indexError("snapshot index digest mismatch")
	}
	if err = json.Unmarshal(data, &index); err != nil {
		return index, invalidRecord(err)
	}
	if index.ID != ref.ID || index.Generation != ref.Generation || index.Image.Base.Revision != ref.Revision || !validID(index.Image.ID) || !validBatchRef(index.Image.Base, originBatch(h)) || !validSnapshotRef(index.Previous) || !validSnapshotRef(index.Skip) || index.Previous.Generation != ref.Generation-1 || index.Previous.Revision >= ref.Revision || index.Skip.Generation != ref.Generation-lowbit(ref.Generation-1) || index.Skip.Revision > index.Previous.Revision || (index.Skip.Generation == index.Previous.Generation && index.Skip != index.Previous) {
		return index, indexError("invalid snapshot index identity or links")
	}
	return index, nil
}
func (s *session) locateSnapshotGeneration(ctx context.Context, h manifest, target uint64) (snapshotRef, error) {
	if target == 1 {
		return initialSnapshotRef(), nil
	}
	ref := currentSnapshotRef(h)
	for ref.Generation > target {
		if err := checkContext(ctx); err != nil {
			return snapshotRef{}, err
		}
		index, err := s.readSnapshotIndex(ref, h)
		if err != nil {
			return snapshotRef{}, err
		}
		if index.Skip.Generation >= target {
			ref = index.Skip
		} else {
			ref = index.Previous
		}
	}
	if ref.Generation != target {
		return snapshotRef{}, indexError("snapshot generation is not referenced")
	}
	return ref, nil
}
func (s *session) selectSnapshot(ctx context.Context, h manifest, target uint64) (snapshotImage, error) {
	if target == 1 {
		return originSnapshot(h), nil
	}
	ref := currentSnapshotRef(h)
	for ref.Generation > 1 {
		if err := checkContext(ctx); err != nil {
			return snapshotImage{}, err
		}
		index, err := s.readSnapshotIndex(ref, h)
		if err != nil {
			return snapshotImage{}, err
		}
		if ref == currentSnapshotRef(h) && index.Image != baseSnapshot(h) {
			return snapshotImage{}, indexError("current snapshot index mismatch")
		}
		if ref.Revision <= target {
			return index.Image, nil
		}
		if index.Skip.Revision > target {
			ref = index.Skip
		} else {
			ref = index.Previous
		}
	}
	return originSnapshot(h), nil
}
func (s *session) readSnapshot(image snapshotImage) (durable.RunRecord, error) {
	data, err := os.ReadFile(s.snapshotPath(image.ID))
	if err != nil {
		return durable.RunRecord{}, invalidRecord(err)
	}
	if sha256.Sum256(data) != image.Digest {
		return durable.RunRecord{}, indexError("snapshot digest mismatch")
	}
	r, err := recordio.DecodeSnapshot(data)
	if err != nil {
		return r, invalidRecord(err)
	}
	if r.RunID != s.runID || r.Revision != image.Base.Revision || r.Format != durable.RecordFormat {
		return r, indexError("snapshot identity mismatch")
	}
	if r.Revision == 1 {
		if err = durable.ValidateInitialRecord(r, s.runID); err != nil {
			return r, invalidRecord(err)
		}
	}
	return r, nil
}

// replay 仅解码所选快照到目标之间的连续批，保留原始 C(R)。
func (s *session) replay(ctx context.Context, h manifest, snapshot snapshotImage, target batchRef) (durable.RevisionImage, error) {
	r, err := s.readSnapshot(snapshot)
	if err != nil {
		return durable.RevisionImage{}, err
	}
	var reverse []durable.ChangeBatch
	ref := target
	for ref.Revision > snapshot.Base.Revision {
		if err = checkContext(ctx); err != nil {
			return durable.RevisionImage{}, err
		}
		b, index, err := s.readBatch(ref, originBatch(h))
		if err != nil {
			return durable.RevisionImage{}, err
		}
		reverse = append(reverse, b)
		ref = index.Parent
	}
	if ref != snapshot.Base {
		return durable.RevisionImage{}, indexError("incomplete selected history path")
	}
	for i := len(reverse) - 1; i >= 0; i-- {
		if err = checkContext(ctx); err != nil {
			return durable.RevisionImage{}, err
		}
		if _, err = reverse[i].ApplyTo(&r); err != nil {
			return durable.RevisionImage{}, invalidRecord(err)
		}
	}
	if err = checkContext(ctx); err != nil {
		return durable.RevisionImage{}, err
	}
	return durable.RevisionImage{Record: r, Chain: target.Chain}, nil
}

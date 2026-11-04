package durable

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const (
	chainSeedDomain  = "pulse-durable/chain/seed/v1\x00"
	chainBatchDomain = "pulse-durable/chain/batch/v1\x00"
)

// SeedChain 返回新检查点的 revision-1 链锚点。
// snapshotDigest 必须是实际持久化快照字节的 SHA-256，由调用方计算，不能通过重新编码 RunRecord 得到。
// 快照合成必须保留已有的已提交链锚点，而不是创建新的业务链。
func SeedChain(snapshotDigest [32]byte) [32]byte {
	var input [len(chainSeedDomain) + 32 + 8]byte
	n := copy(input[:], chainSeedDomain)
	n += copy(input[n:], snapshotDigest[:])
	binary.BigEndian.PutUint64(input[n:], 1)
	return sha256.Sum256(input[:])
}

// NextChain 返回追加规范批次摘要后的链锚点。
// parent 是前一个已提交的链锚点；batchDigest 是规范批次线格式的 SHA-256（由 ChangeBatchDigest 返回）。
// revision 必须大于 1。调用方必须单独校验连续 revision、身份和所有权；摘要只能证明完整性，不能证明权威性。
func NextChain(parent, batchDigest [32]byte, revision uint64) ([32]byte, error) {
	if revision <= 1 {
		return [32]byte{}, fmt.Errorf("durable: chain batch revision must exceed 1: %d", revision)
	}
	var input [len(chainBatchDomain) + 32 + 32 + 8]byte
	n := copy(input[:], chainBatchDomain)
	n += copy(input[n:], parent[:])
	n += copy(input[n:], batchDigest[:])
	binary.BigEndian.PutUint64(input[n:], revision)
	return sha256.Sum256(input[:]), nil
}

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

// SeedChain returns the revision-1 chain anchor for a new checkpoint.
// snapshotDigest must be SHA-256 of the actual persisted snapshot bytes, computed
// by the caller, not a re-encoding of a RunRecord. Snapshot synthesis must retain
// the existing committed chain anchor, rather than seed a new business chain.
func SeedChain(snapshotDigest [32]byte) [32]byte {
	var input [len(chainSeedDomain) + 32 + 8]byte
	n := copy(input[:], chainSeedDomain)
	n += copy(input[n:], snapshotDigest[:])
	binary.BigEndian.PutUint64(input[n:], 1)
	return sha256.Sum256(input[:])
}

// NextChain returns the chain anchor after appending a canonical batch digest.
// parent is the previous committed chain anchor; batchDigest is SHA-256 of the
// canonical batch wire (as returned by ChangeBatchDigest). revision must exceed
// 1. The caller must separately validate consecutive revisions, identity and
// ownership; a digest proves integrity, not authority.
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

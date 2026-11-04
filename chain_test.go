package durable_test

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

// 固定期望值由 Python hashlib.sha256 和 struct.pack('>Q', revision) 独立计算，
// 并非使用 Go 链实现计算。
func chainDigestHex(t *testing.T, text string) [32]byte {
	t.Helper()
	b, err := hex.DecodeString(text)
	if err != nil || len(b) != 32 {
		t.Fatalf("invalid test digest %q: %v", text, err)
	}
	var digest [32]byte
	copy(digest[:], b)
	return digest
}

func TestChainSeedVectors(t *testing.T) {
	for _, tc := range []struct {
		name, snapshot, digest, want string
	}{
		{"empty", "", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "2b7fd0ed6934a37bebd2c14dc672101b484fbdba45ca5c0962793350d381e4d5"},
		{"nonempty", "snapshot-v1", "ed0baf13f9a5bcb30b84e3020e0b5b8a181ab71bc44ea5642304e9abcacc0836", "3269c0c55d5ba121a0c4204c07fcfec4c2e4e7cefb848eb8f0507f53652ac0c3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			digest := sha256.Sum256([]byte(tc.snapshot))
			if digest != chainDigestHex(t, tc.digest) {
				t.Fatalf("snapshot SHA-256 = %x, want %s", digest, tc.digest)
			}
			if got := durable.SeedChain(digest); got != chainDigestHex(t, tc.want) {
				t.Fatalf("SeedChain = %x, want %s", got, tc.want)
			}
		})
	}
}

func TestChainNextVectors(t *testing.T) {
	parent := chainDigestHex(t, "3269c0c55d5ba121a0c4204c07fcfec4c2e4e7cefb848eb8f0507f53652ac0c3")
	batch := chainDigestHex(t, "dc1c62448a6a9ef14044f2be55e5bbb3faa55ca501bece69701c503ed20db5a0")
	if got := sha256.Sum256([]byte("batch-v1")); got != batch {
		t.Fatalf("batch SHA-256 = %x, want %x", got, batch)
	}
	for _, tc := range []struct {
		name     string
		revision uint64
		want     string
	}{
		{"revision2", 2, "5fa00d138fb0c2e9a07fd1f3b3cc4516433be640cd3c0aa18d0cb40725c7aa04"},
		{"uint64max", math.MaxUint64, "feb4ec1f8357249c20165adf30c1a84d05370436a35b68c05add27cef71d7dd1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := durable.NextChain(parent, batch, tc.revision)
			if err != nil {
				t.Fatal(err)
			}
			if got != chainDigestHex(t, tc.want) {
				t.Fatalf("NextChain = %x, want %s", got, tc.want)
			}
		})
	}
}

func TestChainNextRejectsSeedRevisions(t *testing.T) {
	for _, revision := range []uint64{0, 1} {
		got, err := durable.NextChain([32]byte{}, [32]byte{}, revision)
		if err == nil || got != ([32]byte{}) {
			t.Fatalf("revision %d: got %x, %v; want zero digest and error", revision, got, err)
		}
	}
}

func TestChainInputSensitivity(t *testing.T) {
	snapshot := sha256.Sum256([]byte("snapshot-v1"))
	parent := durable.SeedChain(snapshot)
	batch := sha256.Sum256([]byte("batch-v1"))
	want, err := durable.NextChain(parent, batch, 2)
	if err != nil {
		t.Fatal(err)
	}
	changedParent, changedBatch := parent, batch
	changedParent[0] ^= 1
	changedBatch[31] ^= 1
	for _, tc := range []struct {
		name          string
		parent, batch [32]byte
		revision      uint64
	}{
		{"order", batch, parent, 2},
		{"parent", changedParent, batch, 2},
		{"batch", parent, changedBatch, 2},
		{"revision", parent, batch, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := durable.NextChain(tc.parent, tc.batch, tc.revision)
			if err != nil || got == want {
				t.Fatalf("changed input: got %x, %v; must differ from %x", got, err, want)
			}
		})
	}
	snapshot[0] ^= 1
	if durable.SeedChain(snapshot) == parent {
		t.Fatal("changed snapshot digest did not change seed")
	}
}

func TestChainSynthesisDoesNotReseedBusinessAnchor(t *testing.T) {
	// 在 revision 2 时，合成过程单独保存 Snew，并保留 C2 作为业务锚点。
	// Snew 和 SeedChain(Snew) 都不会替换 C2。本测试展示算法契约，不实现 manifest。
	anchor := chainDigestHex(t, "5fa00d138fb0c2e9a07fd1f3b3cc4516433be640cd3c0aa18d0cb40725c7aa04")
	snapshot := sha256.Sum256([]byte("synthesized-snapshot-at-2"))
	if snapshot != chainDigestHex(t, "5cdb239ddccbea9304edc6bfb675295a877297d5a797594c3e3c82d8015273ac") {
		t.Fatalf("synthesized snapshot digest = %x", snapshot)
	}
	reseed := durable.SeedChain(snapshot)
	if reseed != chainDigestHex(t, "716edf05324d1a3a9f3218b3d7eb2353a88ec5a7be7933ac3cc798adcc0890ad") {
		t.Fatalf("synthesized reseed = %x", reseed)
	}
	if snapshot == anchor || reseed == anchor {
		t.Fatal("synthesized snapshot digest/reseed must not replace the committed business anchor")
	}
}

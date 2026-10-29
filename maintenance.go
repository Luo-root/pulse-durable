package durable

import (
	"context"
	"errors"
)

var (
	// ErrInvalidRecord means that a retained record or its history is corrupt.
	ErrInvalidRecord = errors.New("durable: invalid record")
	// ErrUnsupportedCapability means that a backend lacks the requested capability.
	ErrUnsupportedCapability = errors.New("durable: unsupported capability")
	// ErrHistoryExpired is reserved for history removed by an explicit retention
	// operation, never for missing or corrupt components of a retained chain.
	ErrHistoryExpired = errors.New("durable: requested history expired")
	// ErrPinned means that protected history prevents reclamation.
	ErrPinned = errors.New("durable: history is pinned")
	// ErrLimitExceeded means that a configured work or representation budget was exceeded.
	ErrLimitExceeded = errors.New("durable: maintenance limit exceeded")
)

// Maintenance describes store-level history and physical snapshot synthesis.
// These operations never execute business code. Ordinary business writes remain
// on Session. Every official store implements these semantics, including the
// non-durable MemoryStore reference. Third-party stores may omit this interface.
// Distributed implementations additionally require namespace-qualified identity
// and database maintenance authorization at their own entry points.
//
// Synthesis must preserve every retained historical revision and its chain.
// It is NOT retention: only an independently authorized retention operation may
// reclaim old representations. Errors after publication may have unknown
// outcomes; inspect the generation and base anchor before planning another call.
type Maintenance interface {
	InspectStorage(context.Context, string) (StorageState, error)
	LoadRevision(context.Context, string, uint64) (RevisionImage, error)
	CompactStorage(context.Context, CompactStorageRequest) (StorageState, error)
}

// StorageState is one atomic observation, not permission for a later operation.
// Generation starts at 1 and changes only on representation publication.
// Apply changes HeadRevision and HeadChain, not Generation. Digests authenticate
// bytes and ordering but do not authorize a caller.
type StorageState struct {
	RunID        string
	Generation   uint64
	BaseRevision uint64
	BaseChain    [32]byte
	HeadRevision uint64
	HeadChain    [32]byte
}

// RevisionImage contains an isolated projection of exactly the requested
// revision and its committed chain anchor, never a substituted newer record.
// LoadRevision is an atomic read; it creates no persistent pin and therefore
// cannot promise that a subsequent call can still load the same history.
type RevisionImage struct {
	Record RunRecord
	Chain  [32]byte
}

// CompactStorageRequest synthesizes a snapshot at ThroughRevision (>0).
// ExpectedGeneration is obtained from InspectStorage and must be positive.
// Local backends may synthesize under their existing exclusive storage lock
// and return ErrBusy while a session is active. Distributed backends capture a
// pinned path, build outside the publication lock, and CAS-publish only if the
// representation generation is unchanged. MemoryStore also builds outside its
// lock. Concurrent business suffixes remain intact. A revision below the current base
// or above the current head is a conflict; historical reads use LoadRevision.
// A cancelled build never publishes; cancellation after publication is an
// unknown outcome. Builds never implicitly reclaim retained history.
type CompactStorageRequest struct {
	RunID              string
	ThroughRevision    uint64
	ExpectedGeneration uint64
}

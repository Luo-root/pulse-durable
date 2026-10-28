package durable

import (
	"fmt"
	"unicode/utf8"
)

// ValidateInitialRecord checks the shared initialization boundary. It does not
// grant ownership or check checkpoint absence: the backend must do both atomically.
// Graph and codec validation remain Runtime responsibilities.
func ValidateInitialRecord(r RunRecord, runID string) error {
	if runID == "" || r.RunID != runID || !utf8.ValidString(runID) ||
		r.Format != RecordFormat || r.Revision != 1 || r.Version == "" || !utf8.ValidString(r.Version) ||
		!validDigest(r.Definition) || !validDigest(r.Inputs) || r.RootScope == "" || !utf8.ValidString(r.RootScope) ||
		len(r.Scopes) != 1 || len(r.Attempts) != 0 || len(r.Streams) != 0 || len(r.Messages) != 0 || len(r.Managed) != 0 {
		return fmt.Errorf("%w: invalid initial checkpoint identity or execution state", ErrConflict)
	}
	root, ok := r.Scopes[r.RootScope]
	if !ok || root.Graph == "" || root.Inputs != r.Inputs || len(root.Nodes) != 0 {
		return fmt.Errorf("%w: invalid initial root scope", ErrConflict)
	}
	return nil
}

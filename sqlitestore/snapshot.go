package sqlitestore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"unicode/utf8"

	durable "github.com/Luo-root/pulse-durable"
)

// This validates persisted structure, not graph definitions or codec behavior.
func validateSnapshot(r durable.RunRecord, id string, rev uint64) error {
	if rev == 1 {
		return durable.ValidateInitialRecord(r, id)
	}
	bad := func() error { return fmt.Errorf("%w: invalid snapshot structure", durable.ErrInvalidRecord) }
	digest := func(s string) bool {
		b, e := hex.DecodeString(s)
		return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
	}
	text := func(s string) bool { return s != "" && utf8.ValidString(s) }
	root, ok := r.Scopes[r.RootScope]
	if r.RunID != id || r.Revision != rev || rev == 0 || r.Format != durable.RecordFormat || !text(r.Version) || !digest(r.Definition) || !digest(r.Inputs) || !ok || root.Graph == "" || root.Inputs != r.Inputs {
		return bad()
	}
	validValues := func(values []durable.ValueRecord) bool {
		names := map[string]bool{}
		for _, v := range values {
			if !text(v.Name) || !text(v.TypeID) || !text(v.Version) || names[v.Name] {
				return false
			}
			names[v.Name] = true
			switch v.State {
			case "ready":
				if v.Stream == nil {
					sum := sha256.Sum256(v.Data)
					if v.Digest != hex.EncodeToString(sum[:]) {
						return false
					}
				} else if v.Data != nil || v.Digest != "" {
					return false
				}
			case "skipped":
				if v.Data != nil || v.Digest != "" || v.Stream != nil {
					return false
				}
			default:
				return false
			}
		}
		return true
	}
	validState := func(state durable.AttemptState) bool {
		return state == durable.AttemptActive || state == durable.AttemptFailed || state == durable.AttemptConfirmed
	}
	for key, scope := range r.Scopes {
		if !text(key) || !text(scope.Graph) || !digest(scope.Inputs) {
			return bad()
		}
		if key != r.RootScope {
			if _, ok := r.Attempts[key]; !ok {
				return bad()
			}
		}
		for nodeID, node := range scope.Nodes {
			a, ok := r.Attempts[node.Attempt]
			if !text(nodeID) || node.NodeID != nodeID || !ok || a.NodeID != nodeID || a.State != durable.AttemptConfirmed || (node.Reason != "completed" && node.Reason != "skipped") || !validValues(node.Outputs) {
				return bad()
			}
		}
	}
	for key, a := range r.Attempts {
		if !text(key) || !text(a.NodeID) || a.Index <= 0 || a.Limit < a.Index || !validState(a.State) {
			return bad()
		}
		if _, ok := r.Scopes[a.Parent]; !ok {
			if _, ok = r.Attempts[a.Parent]; !ok {
				return bad()
			}
		}
		seen := map[string]bool{key: true}
		parent := a.Parent
		for {
			next, ok := r.Attempts[parent]
			if !ok {
				break
			}
			if seen[parent] {
				return bad()
			}
			seen[parent] = true
			parent = next.Parent
		}
	}
	for key, stream := range r.Streams {
		d := stream.Stream
		if !text(key) || !text(d.ID.Scope) || !text(d.ID.NodeID) || !text(d.ID.Key) || !text(d.ID.Group) || d.ID.Generation == 0 || !text(d.TypeID) || !text(d.Version) || d.Capacity < 0 || stream.AckedThrough > stream.LastSequence {
			return bad()
		}
		if _, ok := r.Scopes[d.ID.Scope]; !ok {
			return bad()
		}
		previous := stream.AckedThrough
		for _, message := range stream.Pending {
			if message.Sequence <= previous || message.Sequence > stream.LastSequence || !validValues([]durable.ValueRecord{message.Value}) {
				return bad()
			}
			previous = message.Sequence
		}
		if stream.Cursor != nil && !validValues([]durable.ValueRecord{*stream.Cursor}) {
			return bad()
		}
	}
	for key, message := range r.Messages {
		if !text(key) || message.Index <= 0 || message.Limit < message.Index || message.Sequence == 0 || !validState(message.State) {
			return bad()
		}
		if _, ok := r.Scopes[message.Scope]; !ok {
			return bad()
		}
		if _, ok := r.Attempts[message.Parent]; !ok {
			return bad()
		}
	}
	for key, managed := range r.Managed {
		if !text(key) || !text(managed.NodeID) || !validValues(managed.State) || !validValues(managed.Values) {
			return bad()
		}
		if _, ok := r.Scopes[managed.Scope]; !ok {
			return bad()
		}
		if _, ok := r.Attempts[managed.Parent]; !ok {
			return bad()
		}
	}
	return nil
}

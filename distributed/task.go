// Package distributed defines experimental portable task and coordination contracts.
// It contains no database backend and does not start worker loops.
package distributed

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

const (
	Protocol           = 1
	PayloadFormat      = 1
	MaxTextBytes       = 256
	MaxManifestEntries = 4096
	MaxPayloadBytes    = 4 << 20
	MaxEnvelopeBytes   = 8 << 20
)

var (
	ErrInvalid          = errors.New("distributed: invalid task")
	ErrMismatch         = errors.New("distributed: incompatible task")
	ErrDuplicate        = errors.New("distributed: duplicate registration")
	ErrSubmitConflict   = errors.New("distributed: submission conflict")
	ErrNotFound         = errors.New("distributed: task not found")
	ErrLeaseLost        = errors.New("distributed: lease lost")
	ErrRevisionConflict = errors.New("distributed: revision conflict")
	ErrTerminal         = errors.New("distributed: terminal task")
	ErrUnknown          = errors.New("distributed: operation commit unknown")
)

type Identity struct {
	Namespace string `json:"namespace"`
	RunID     string `json:"run_id"`
}

// Route is an exact capability, not a version range. ManifestDigest covers the
// complete ordered graph-key manifest, including internal and stream keys.
type Route struct {
	TaskType        string `json:"task_type"`
	BusinessVersion string `json:"business_version"`
	Definition      string `json:"definition"`
	ManifestDigest  string `json:"manifest_digest"`
	Protocol        int    `json:"protocol"`
	RecordFormat    int    `json:"record_format"`
	PayloadFormat   int    `json:"payload_format"`
}

type descriptor struct {
	Identity      Identity               `json:"identity"`
	Route         Route                  `json:"route"`
	Manifest      []durable.CodecBinding `json:"codec_manifest"`
	Inputs        string                 `json:"inputs"`
	Payload       []byte                 `json:"payload"`
	PayloadDigest string                 `json:"payload_digest"`
}

// Task owns a canonical immutable snapshot. Accessors return copies; zero is invalid.
// Wire: encoding/json fixed-field-order structs, no whitespace, sorted manifest
// by each binding's canonical JSON bytes, base64 payload, lowercase SHA-256 hex.
// Unknown/duplicate fields, alternate whitespace/escaping and trailing data are
// rejected by decode-and-byte-compare. Codec bytes themselves must be deterministic.
type Task struct {
	wire   string
	digest string
}

func hash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func text(s string) bool   { return len(s) > 0 && len(s) <= MaxTextBytes && utf8.ValidString(s) }
func fingerprint(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

// NewTask prepares actual inputs without Run, store calls or business execution.
func NewTask(ctx context.Context, id Identity, taskType, version string, g *pulse.Graph, in pulse.Inputs, codecs *durable.Codecs) (Task, error) {
	if !text(id.Namespace) || !text(id.RunID) || !text(taskType) || !text(version) {
		return Task{}, ErrInvalid
	}
	p, err := durable.Prepare(ctx, g, in, codecs)
	if err != nil {
		return Task{}, ErrInvalid
	}
	m, _ := json.Marshal(p.CodecManifest)
	d := descriptor{id, Route{taskType, version, p.Definition, hash(m), Protocol, durable.RecordFormat, PayloadFormat}, p.CodecManifest, p.Inputs, p.Payload, hash(p.Payload)}
	wire, err := json.Marshal(d)
	if err != nil {
		return Task{}, ErrInvalid
	}
	return ParseTask(wire)
}

// ParseTask bounds and validates the envelope before any constructor or codec.
// It checks structure and checksums, not local graph/codec compatibility: Registry
// validates those before returning graph/inputs for an execution bridge.
func ParseTask(wire []byte) (Task, error) {
	if len(wire) == 0 || len(wire) > MaxEnvelopeBytes || !utf8.Valid(wire) {
		return Task{}, ErrInvalid
	}
	var d descriptor
	dec := json.NewDecoder(bytes.NewReader(wire))
	dec.DisallowUnknownFields()
	if dec.Decode(&d) != nil {
		return Task{}, ErrInvalid
	}
	canonical, err := json.Marshal(d)
	if err != nil || !bytes.Equal(wire, canonical) {
		return Task{}, ErrInvalid
	}
	r := d.Route
	if !text(d.Identity.Namespace) || !text(d.Identity.RunID) || !text(r.TaskType) || !text(r.BusinessVersion) || r.Protocol != Protocol || r.PayloadFormat != PayloadFormat || r.RecordFormat != durable.RecordFormat || !fingerprint(r.Definition) || !fingerprint(d.Inputs) || !fingerprint(r.ManifestDigest) || !fingerprint(d.PayloadDigest) {
		return Task{}, ErrInvalid
	}
	if len(d.Payload) > MaxPayloadBytes || len(d.Manifest) > MaxManifestEntries || d.Manifest == nil || hash(d.Payload) != d.PayloadDigest {
		return Task{}, ErrInvalid
	}
	previous := []byte(nil)
	for _, b := range d.Manifest {
		if !text(b.Graph) || len(b.Path) > MaxTextBytes || !utf8.ValidString(b.Path) || !text(b.Name) || !text(b.TypeID) || !text(b.Version) {
			return Task{}, ErrInvalid
		}
		encoded, _ := json.Marshal(b)
		if previous != nil && bytes.Compare(previous, encoded) >= 0 {
			return Task{}, ErrInvalid
		}
		previous = encoded
	}
	m, _ := json.Marshal(d.Manifest)
	if hash(m) != r.ManifestDigest {
		return Task{}, ErrInvalid
	}
	return Task{string(wire), hash(wire)}, nil
}
func (t Task) data() descriptor                      { var d descriptor; _ = json.Unmarshal([]byte(t.wire), &d); return d }
func (t Task) Identity() Identity                    { return t.data().Identity }
func (t Task) Route() Route                          { return t.data().Route }
func (t Task) Bytes() []byte                         { return []byte(t.wire) }
func (t Task) SubmissionDigest() string              { return t.digest }
func (t Task) Payload() []byte                       { return t.data().Payload }
func (t Task) InputsFingerprint() string             { return t.data().Inputs }
func (t Task) CodecManifest() []durable.CodecBinding { return t.data().Manifest }

// CompareSubmission requires digest AND exact canonical fields (wire) equality.
// Backends must call this against the authoritative existing row under the identity
// unique constraint, never replace it or reset status/checkpoints on duplication.
func CompareSubmission(existing, submitted Task) (bool, error) {
	a, e := ParseTask(existing.Bytes())
	if e != nil {
		return false, e
	}
	b, e := ParseTask(submitted.Bytes())
	if e != nil {
		return false, e
	}
	if a.Identity() != b.Identity() {
		return false, ErrInvalid
	}
	if a.digest != b.digest || a.wire != b.wire {
		return false, ErrSubmitConflict
	}
	return true, nil
}

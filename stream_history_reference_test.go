package durable

import (
	"testing"

	"github.com/Luo-root/pulse"
)

func TestHistoricalStreamReferencePreservesSourceConstraints(t *testing.T) {
	for _, terminal := range []pulse.StreamTerminal{"", pulse.StreamFailed, pulse.StreamCanceled} {
		t.Run(string(terminal), func(t *testing.T) {
			protocol := newStreamProtocol(t)
			logical := streamKey(streamID(protocol.input.Stream.ID))
			stream := protocol.s.record.Streams[logical]
			history := stream.Stream // Sub was confirmed before its actual source ended.
			stream.Stream.Terminal = string(terminal)
			protocol.s.record.Streams[logical] = stream
			owner := protocol.s.record.Attempts[history.Attempt]
			owner.State = AttemptFailed
			protocol.s.record.Attempts[history.Attempt] = owner
			if err := protocol.s.checkStreamReferenceRecord(protocol.s.record, history, true); err != nil {
				t.Fatalf("valid historical association rejected: %v", err)
			}
			if err := protocol.s.checkStreamReference(protocol.s.record, history); err == nil {
				t.Fatal("historical shape validation authorized an invalid live source")
			}
			for name, mutate := range map[string]func(*StreamDescriptorRecord){
				"source":     func(d *StreamDescriptorRecord) { d.SourceID = "forged" },
				"attempt":    func(d *StreamDescriptorRecord) { d.Attempt = protocol.processor.Scope },
				"graph":      func(d *StreamDescriptorRecord) { d.Graph = "forged" },
				"path":       func(d *StreamDescriptorRecord) { d.Path = "forged" },
				"capacity":   func(d *StreamDescriptorRecord) { d.Capacity++ },
				"generation": func(d *StreamDescriptorRecord) { d.ID.Generation++ },
				"terminal":   func(d *StreamDescriptorRecord) { d.Terminal = string(pulse.StreamCompleted) },
				"stop":       func(d *StreamDescriptorRecord) { d.Stopped = true },
			} {
				t.Run(name, func(t *testing.T) {
					bad := history
					mutate(&bad)
					if err := protocol.s.checkStreamReferenceRecord(protocol.s.record, bad, true); err == nil {
						t.Fatal("structural history check discarded a source constraint")
					}
				})
			}
		})
	}
}

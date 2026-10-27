package durable

import "testing"

func FuzzUnmarshalChangeBatchNeverPanics(f *testing.F) {
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"version":1,"header":{},"ops":[]}{}`))
	f.Add([]byte(`{"version":1,"header":{"batch_format":"1","logical_format":"2"},"ops":[{"kind":"managed_entry","key":"x","scope":"m","name":"x","expected":null,"value":null}]}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = UnmarshalChangeBatch(data)
	})
}

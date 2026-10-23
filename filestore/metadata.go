package filestore

import (
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

// validMetadata retains the package-local validation used by metadata tests.
func validMetadata(record durable.RunRecord) bool {
	_, err := recordio.Encode(record)
	return err == nil
}

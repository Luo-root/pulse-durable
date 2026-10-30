package filestore

import (
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

// validMetadata 保留元数据测试使用的包内验证。
func validMetadata(record durable.RunRecord) bool {
	_, err := recordio.Encode(record)
	return err == nil
}

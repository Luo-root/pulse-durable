package durable

import (
	"fmt"

	"github.com/Luo-root/pulse"
)

// 先逐条核验历史引用，再给同一逻辑流返回一份权威引用。历史节点成果不会因
// Open 提升代际或稍后的 Stop 被倒改；差异不是两条来源，也不能靠放宽历史校验抹平。
// 调用方持 runState.mu，或处于业务尚未启动的根恢复阶段。
func (s *runState) restoreStreamReference(key pulse.CheckpointKey, historical StreamDescriptorRecord) (pulse.CheckpointStreamDescriptor, error) {
	if err := s.checkStreamReference(s.record, historical); err != nil {
		return pulse.CheckpointStreamDescriptor{}, err
	}
	logical := streamKey(historical.ID)
	current := s.record.Streams[logical].Stream
	if current.Terminal == "" && !current.Resumable {
		return pulse.CheckpointStreamDescriptor{}, fmt.Errorf("durable: active stream source is not resumable")
	}
	descriptor, exists := s.restoreStreams[logical]
	if !exists {
		descriptor = s.record.Streams[logical].Stream
		if s.restoreStreams == nil {
			s.restoreStreams = make(map[string]StreamDescriptorRecord)
		}
		s.restoreStreams[logical] = descriptor
	}
	// 这份恢复计划身份在本次运行内固定。较早安装的端点可能已经 Open 并提升了
	// 当前记录代际；晚开的子图仍须命中 Core 的同一别名缓存，再由共享端点认回最新账。
	return s.def.decodeDescriptor(key, descriptor)
}

package durable

import (
	"fmt"

	"github.com/Luo-root/pulse"
)

// restoreAttemptValues 只认当前完整节点根尝试的已提交普通输出。Core 将它们恢复进
// checkpoint 外帧，不提前发布执行槽，也不重做已经确认的输入消息。
func (s *runState) restoreAttemptValues(scopeID, parent, nodeID string) ([]pulse.CheckpointOutput, error) {
	managed, exists := s.record.Managed[managedKey(scopeID, parent, nodeID)]
	if !exists {
		return nil, nil
	}
	if managed.Scope != scopeID || managed.Parent != parent || managed.NodeID != nodeID {
		return nil, fmt.Errorf("durable: restored message values have a different processing owner")
	}
	scope := s.record.Scopes[scopeID]
	gs := s.def.graphs[location{scope.Graph, scope.Path}]
	if gs == nil {
		return nil, fmt.Errorf("durable: restored message values have an unknown graph")
	}
	node, ok := gs.nodes[nodeID]
	if !ok {
		return nil, fmt.Errorf("durable: restored message values have an unknown node")
	}
	keys := make(map[string]pulse.CheckpointKey, len(node.Provides))
	for _, key := range node.Provides {
		keys[key.Name] = key
	}
	values := make([]pulse.CheckpointOutput, 0, len(managed.Values))
	for _, value := range managed.Values {
		key, ok := keys[value.Name]
		if !ok || key.Element != nil || value.Stream != nil {
			return nil, fmt.Errorf("durable: restored message value is duplicate, undeclared or a stream")
		}
		delete(keys, value.Name)
		decoded, err := s.def.decodeValue(key, value)
		if err != nil {
			return nil, err
		}
		values = append(values, decoded)
	}
	return values, nil
}

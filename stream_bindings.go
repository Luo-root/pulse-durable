package durable

// nodeReadsStream 核验持久来源确实沿静态 In/Out 接到了处理节点；别的合法流也不能冒充输入。
// Generation 另由 currentStream 核验，此处只判断稳定接线身份。
func (s *runState) nodeReadsStream(scopeID, node string, source StreamIDRecord) bool {
	scope, ok := s.record.Scopes[scopeID]
	if !ok {
		return false
	}
	gs := s.def.graphs[location{scope.Graph, scope.Path}]
	if gs == nil {
		return false
	}
	for _, key := range gs.nodes[node].Requires {
		if key.Element != nil && s.streamComesFrom(scopeID, key.Name, source, make(map[string]bool)) {
			return true
		}
	}
	return false
}

func (s *runState) streamComesFrom(scopeID, key string, source StreamIDRecord, seen map[string]bool) bool {
	visit := managedKey(scopeID, "", key)
	if seen[visit] {
		return false
	}
	seen[visit] = true
	scope, ok := s.record.Scopes[scopeID]
	if !ok {
		return false
	}
	gs := s.def.graphs[location{scope.Graph, scope.Path}]
	if gs == nil {
		return false
	}
	if producer, internal := gs.producers[key]; internal {
		if scopeID == source.Scope && producer == source.NodeID && key == source.Key {
			return true
		}
		if child, sub := gs.children[producer]; sub {
			for childID, record := range s.record.Scopes {
				if (location{record.Graph, record.Path}) != child {
					continue
				}
				root, _, err := s.rootAttempt(childID)
				if err != nil || root.Parent != scopeID || root.NodeID != producer {
					continue
				}
				for _, binding := range gs.binds[producer] {
					if !binding.In && binding.Parent == key && s.streamComesFrom(childID, binding.Child, source, seen) {
						return true
					}
				}
			}
		}
		return false
	}
	if scopeID == s.record.RootScope {
		return false
	}
	root, _, err := s.rootAttempt(scopeID)
	if err != nil {
		return false
	}
	parent := s.record.Scopes[root.Parent]
	parentSchema := s.def.graphs[location{parent.Graph, parent.Path}]
	if parentSchema == nil {
		return false
	}
	for _, binding := range parentSchema.binds[root.NodeID] {
		if binding.In && !binding.Fixed && binding.Child == key && s.streamComesFrom(root.Parent, binding.Parent, source, seen) {
			return true
		}
	}
	return false
}

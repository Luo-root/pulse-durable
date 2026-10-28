package durable

import "fmt"

// GrowthSeedBatchesForTest prepares component history in bounded 16-operation
// batches. Setup may inspect all history; normal benchmark commits never do.
func GrowthSeedBatchesForTest(base, target RunRecord) ([]ChangeBatch, error) {
	full := GrowthBatchForTest(base, target).tx
	for id, m := range target.Managed {
		h := managedHeaderOf(m)
		_ = full.putManaged(id, nil, h)
		for _, v := range m.State {
			v := v
			_ = full.putManagedEntry(managedEntryKey{id, v.Name, true}, nil, &v)
		}
		for _, v := range m.Values {
			v := v
			_ = full.putManagedEntry(managedEntryKey{id, v.Name, false}, nil, &v)
		}
	}
	var batches []ChangeBatch
	tx := newRecordTransaction(base.Revision)
	n := 0
	flush := func() error {
		if n == 0 {
			return nil
		}
		batch := ChangeBatch{changeHeader(base), tx.copy()}
		if _, err := MarshalChangeBatch(batch); err != nil {
			return err
		}
		if _, err := batch.ApplyTo(&base); err != nil {
			return err
		}
		batches = append(batches, batch)
		tx = newRecordTransaction(base.Revision)
		n = 0
		return nil
	}
	push := func(stage func()) error {
		stage()
		n++
		if n == 16 {
			return flush()
		}
		return nil
	}
	// Dependency headers precede their entries even across batch boundaries.
	for k, v := range full.scopes {
		if err := push(func() {
			if tx.scopes == nil {
				tx.scopes = make(map[string]ScopeRecord)
			}
			tx.scopes[k] = v
		}); err != nil {
			return nil, err
		}
	}
	for k, v := range full.attempts {
		if err := push(func() {
			if tx.attempts == nil {
				tx.attempts = make(map[string]putAttempt)
			}
			tx.attempts[k] = v
		}); err != nil {
			return nil, err
		}
	}
	for k, v := range full.nodes {
		if err := push(func() {
			if tx.nodes == nil {
				tx.nodes = make(map[nodeRecordKey]putNode)
			}
			tx.nodes[k] = v
		}); err != nil {
			return nil, err
		}
	}
	for k, v := range full.streams {
		if err := push(func() {
			if tx.streams == nil {
				tx.streams = make(map[string]putStreamHeader)
			}
			tx.streams[k] = v
		}); err != nil {
			return nil, err
		}
	}
	for k, v := range full.pending {
		if err := push(func() {
			if tx.pending == nil {
				tx.pending = make(map[pendingKey]pendingOperation)
			}
			tx.pending[k] = v
		}); err != nil {
			return nil, err
		}
	}
	for k, v := range full.messages {
		if err := push(func() {
			if tx.messages == nil {
				tx.messages = make(map[string]putMessage)
			}
			tx.messages[k] = v
		}); err != nil {
			return nil, err
		}
	}
	for k, v := range full.managed {
		if err := push(func() {
			if tx.managed == nil {
				tx.managed = make(map[string]putManagedHeader)
			}
			tx.managed[k] = v
		}); err != nil {
			return nil, err
		}
	}
	for k, v := range full.managedEntries {
		if err := push(func() {
			if tx.managedEntries == nil {
				tx.managedEntries = make(map[managedEntryKey]managedEntryOperation)
			}
			tx.managedEntries[k] = v
		}); err != nil {
			return nil, err
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return batches, nil
}

// GrowthUpdateForTest changes exactly one existing message retry budget. It
// constructs one typed operation without copying or traversing background data.
func GrowthUpdateForTest(base RunRecord, id string) (ChangeBatch, error) {
	old, ok := base.Messages[id]
	if !ok {
		return ChangeBatch{}, fmt.Errorf("missing benchmark message")
	}
	next := old
	next.Limit++
	tx := newRecordTransaction(base.Revision)
	if err := tx.putMessage(id, &old, next); err != nil {
		return ChangeBatch{}, err
	}
	return ChangeBatch{changeHeader(base), tx}, nil
}

// OverrideAttemptForTest reproduces historical partial checkpoints through a
// real Apply. Exported only in the test binary for external-package fixtures.
func OverrideAttemptForTest(batch ChangeBatch, id string, value AttemptRecord) ChangeBatch {
	batch.tx = batch.tx.copy()
	op := batch.tx.attempts[id]
	op.value = value
	batch.tx.attempts[id] = op
	return batch
}

// GrowthBatchForTest stages retained history as typed absent operations for
// component benchmarks; it is not a public import/full-save API.
func GrowthBatchForTest(base, target RunRecord) ChangeBatch {
	tx := newRecordTransaction(base.Revision)
	for id, a := range target.Attempts {
		old, ok := base.Attempts[id]
		if ok {
			_ = tx.putAttempt(id, &old, a)
		} else {
			_ = tx.putAttempt(id, nil, a)
		}
	}
	for id, s := range target.Scopes {
		if _, ok := base.Scopes[id]; !ok {
			header := s
			header.Nodes = nil
			_ = tx.createScope(id, header)
		}
		for node, n := range s.Nodes {
			old, ok := base.Scopes[id].Nodes[node]
			if ok {
				_ = tx.putNode(id, node, &old, n)
			} else {
				_ = tx.putNode(id, node, nil, n)
			}
		}
	}
	for id, s := range target.Streams {
		h := headerOf(s)
		if old, ok := base.Streams[id]; ok {
			oh := headerOf(old)
			_ = tx.putStream(id, &oh, h)
		} else {
			_ = tx.putStream(id, nil, h)
		}
		for _, p := range s.Pending {
			p := p
			var expected *StreamMessageRecord
			for _, old := range base.Streams[id].Pending {
				if old.Sequence == p.Sequence {
					copy := old
					expected = &copy
					break
				}
			}
			_ = tx.putPending(id, p.Sequence, expected, &p)
		}
	}
	for id, m := range target.Messages {
		if old, ok := base.Messages[id]; ok {
			_ = tx.putMessage(id, &old, m)
		} else {
			_ = tx.putMessage(id, nil, m)
		}
	}
	return ChangeBatch{changeHeader(base), tx.copy()}
}

// replaceMemoryFixture deliberately injects an authoritative test fixture. It is
// not a Session write path and must never be used for fault/commit assertions.
func replaceMemoryFixture(session Session, r RunRecord) {
	s := session.(*memorySession)
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	installMemoryFixtureLocked(s.store, r)
	s.revision, s.chain, s.loaded = r.Revision, s.store.chains[s.runID], true
}

// installMemoryFixtureLocked starts explicitly at the supplied fixture revision;
// it does not invent revisions preceding that checkpoint. Caller holds store.mu.
func installMemoryFixtureLocked(store *MemoryStore, r RunRecord) {
	chain := SeedChain(memorySeedDigest(r))
	if store.records == nil {
		store.records = make(map[string]RunRecord)
	}
	if store.chains == nil {
		store.chains = make(map[string][32]byte)
	}
	if store.history == nil {
		store.history = make(map[string]*memoryHistory)
	}
	store.records[r.RunID] = cloneRecord(r)
	store.chains[r.RunID] = chain
	store.history[r.RunID] = &memoryHistory{generation: 1, first: r.Revision, base: r.Revision,
		anchors:   map[uint64][32]byte{r.Revision: chain},
		snapshots: map[uint64]RevisionImage{r.Revision: {Record: cloneRecord(r), Chain: chain}}}
}

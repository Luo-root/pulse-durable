package durable

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
)

func TestConsumerCanConfirmBeforeItsStreamProducerCompletes(t *testing.T) {
	confirmed := make(chan struct{})
	var once sync.Once
	var witnessed atomic.Bool
	store := &faultStore{Store: NewMemoryStore(), match: func(r RunRecord) bool {
		if recordHasNode(r, "early-consumer", "sink") && !recordHasNode(r, "early-consumer", "source") {
			witnessed.Store(true)
			once.Do(func() { close(confirmed) })
		}
		return false
	}}
	g := testGraph(t, "early-consumer")
	key := pulse.NewKey[pulse.Stream[int]]("stream")
	testAdd(t, g, pulse.NewNode("source", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error {
		writer, err := pulse.OpenStream(rc, key, pulse.WithStreamSource("early-source", true))
		if err != nil {
			return err
		}
		if err := writer.SendAt(1, 1); err != nil {
			return err
		}
		select {
		case <-confirmed:
			return nil
		case <-rc.Context().Done():
			return rc.Context().Err()
		}
	}))
	testAdd(t, g, pulse.NewNode("sink", "", pulse.Requires(key), nil, func(rc *pulse.RunCtx) error {
		stream, err := pulse.Get(rc, key)
		if err != nil {
			return err
		}
		delivery, err := stream.Receive(rc)
		if err != nil {
			return err
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
		return stream.Stop(rc)
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := testRuntime(t, store, NewCodecs()).Run(ctx, g, pulse.Inputs{}, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	if !witnessed.Load() {
		t.Fatal("consumer had to wait for complete source NodeRecord")
	}
	record := readRecord(t, store, "run")
	for _, stream := range record.Streams {
		if stream.Stream.Terminal != "stopped" || !stream.Stream.Stopped {
			t.Fatal("normal reading stop was not preserved")
		}
	}
}

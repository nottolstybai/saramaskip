package saramaskip

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

type fakeSession struct {
	sarama.ConsumerGroupSession
	ctx context.Context

	mu     sync.Mutex
	marked []*sarama.ConsumerMessage
}

func newFakeSession(ctx context.Context) *fakeSession {
	return &fakeSession{ctx: ctx}
}

func (s *fakeSession) Context() context.Context {
	return s.ctx
}

func (s *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked = append(s.marked, msg)
}

func (s *fakeSession) markedOffsets() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	offsets := make([]int64, len(s.marked))
	for i, msg := range s.marked {
		offsets[i] = msg.Offset
	}
	return offsets
}

type fakeClaim struct {
	sarama.ConsumerGroupClaim
	messages <-chan *sarama.ConsumerMessage
}

func (c fakeClaim) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}

func TestNewValidation(t *testing.T) {
	t.Parallel()
	handler := MessageHandler(func(context.Context, *sarama.ConsumerMessage) error { return nil })
	tests := []struct {
		name    string
		group   string
		handler MessageHandler
		opts    []Option
		want    string
	}{
		{name: "empty group", handler: handler, want: "consumer group"},
		{name: "nil handler", group: "workers", want: "message handler"},
		{name: "nil option", group: "workers", handler: handler, opts: []Option{nil}, want: "option"},
		{name: "zero retry interval", group: "workers", handler: handler, opts: []Option{WithRetryInterval(0)}, want: "retry interval"},
		{name: "negative retry interval", group: "workers", handler: handler, opts: []Option{WithRetryInterval(-time.Second)}, want: "retry interval"},
		{name: "invalid expression", group: "workers", handler: handler, opts: []Option{WithBypassExpression("offset ==")}, want: "position"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(tt.group, tt.handler, tt.opts...)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestNewOptions(t *testing.T) {
	t.Parallel()
	h, err := New("workers", func(context.Context, *sarama.ConsumerMessage) error { return nil },
		WithRetryInterval(10*time.Millisecond),
		WithBypassExpression(`topic == "orders"`),
	)
	require.NoError(t, err)
	require.Equal(t, 10*time.Millisecond, h.retryInterval)
	require.NotNil(t, h.state.Load().expression)
}

func TestSetBypassExpression(t *testing.T) {
	t.Parallel()
	h, err := New("workers", func(context.Context, *sarama.ConsumerMessage) error { return nil })
	require.NoError(t, err)

	require.NoError(t, h.SetBypassExpression(`offset == 42`))
	matched := h.state.Load()
	require.NotNil(t, matched.expression)

	err = h.SetBypassExpression(`offset ==`)
	require.Error(t, err)
	require.Same(t, matched, h.state.Load())

	require.NoError(t, h.SetBypassExpression(" \t\n"))
	cleared := h.state.Load()
	require.Nil(t, cleared.expression)
	require.NotSame(t, matched, cleared)
	select {
	case <-matched.changed:
	default:
		t.Fatal("previous state was not notified")
	}
}

func TestConsumeClaimSuccess(t *testing.T) {
	t.Parallel()
	msg := testMessage(42)
	messages := closedMessages(msg)
	session := newFakeSession(t.Context())
	calls := 0
	h, err := New("workers", func(_ context.Context, got *sarama.ConsumerMessage) error {
		calls++
		require.Same(t, msg, got)
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, h.ConsumeClaim(session, fakeClaim{messages: messages}))
	require.Equal(t, 1, calls)
	require.Equal(t, []int64{42}, session.markedOffsets())
}

func TestConsumeClaimRetriesSameMessage(t *testing.T) {
	t.Parallel()
	msg := testMessage(42)
	session := newFakeSession(t.Context())
	var offsets []int64
	h, err := New("workers", func(_ context.Context, got *sarama.ConsumerMessage) error {
		offsets = append(offsets, got.Offset)
		if len(offsets) < 3 {
			return fmt.Errorf("attempt %d", len(offsets))
		}
		return nil
	}, WithRetryInterval(time.Millisecond))
	require.NoError(t, err)

	require.NoError(t, h.ConsumeClaim(session, fakeClaim{messages: closedMessages(msg)}))
	require.Equal(t, []int64{42, 42, 42}, offsets)
	require.Equal(t, []int64{42}, session.markedOffsets())
}

func TestConsumeClaimDoesNotAdvanceAfterError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	session := newFakeSession(ctx)
	messages := make(chan *sarama.ConsumerMessage, 2)
	messages <- testMessage(1)
	messages <- testMessage(2)
	calls := make(chan int64, 10)
	h, err := New("workers", func(_ context.Context, msg *sarama.ConsumerMessage) error {
		calls <- msg.Offset
		return errors.New("failed")
	}, WithRetryInterval(time.Millisecond))
	require.NoError(t, err)

	done := consumeAsync(h, session, messages)
	for range 3 {
		require.Equal(t, int64(1), receive(t, calls))
	}
	select {
	case offset := <-calls:
		require.Equal(t, int64(1), offset)
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	require.NoError(t, receive(t, done))
	require.Empty(t, session.markedOffsets())
}

func TestConsumeClaimRuntimeBypass(t *testing.T) {
	t.Parallel()
	session := newFakeSession(t.Context())
	messages := closedMessages(testMessage(1), testMessage(2))
	calls := make(chan int64, 2)
	h, err := New("workers", func(_ context.Context, msg *sarama.ConsumerMessage) error {
		calls <- msg.Offset
		if msg.Offset == 1 {
			return errors.New("duplicate key")
		}
		return nil
	}, WithRetryInterval(time.Hour))
	require.NoError(t, err)

	done := consumeAsync(h, session, messages)
	require.Equal(t, int64(1), receive(t, calls))
	require.NoError(t, h.SetBypassExpression(
		`group == "workers" and topic == "orders" and partition == 3 and offset == 1 and error contains "duplicate"`,
	))

	require.NoError(t, receive(t, done))
	require.Equal(t, int64(2), receive(t, calls))
	require.Empty(t, calls)
	require.Equal(t, []int64{1, 2}, session.markedOffsets())
}

func TestConsumeClaimBypassUpdatedDuringHandler(t *testing.T) {
	t.Parallel()
	session := newFakeSession(t.Context())
	started := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	h, err := New("workers", func(context.Context, *sarama.ConsumerMessage) error {
		calls++
		close(started)
		<-release
		return errors.New("failed")
	}, WithRetryInterval(time.Hour))
	require.NoError(t, err)

	done := consumeAsync(h, session, closedMessages(testMessage(42)))
	waitSignal(t, started)
	require.NoError(t, h.SetBypassExpression(`offset == 42`))
	close(release)

	require.NoError(t, receive(t, done))
	require.Equal(t, 1, calls)
	require.Equal(t, []int64{42}, session.markedOffsets())
}

func TestConsumeClaimBypassesEmptyError(t *testing.T) {
	t.Parallel()
	session := newFakeSession(t.Context())
	calls := 0
	h, err := New("workers", func(context.Context, *sarama.ConsumerMessage) error {
		calls++
		return errors.New("")
	}, WithBypassExpression(`error == ""`), WithRetryInterval(time.Hour))
	require.NoError(t, err)

	require.NoError(t, h.ConsumeClaim(session, fakeClaim{messages: closedMessages(testMessage(42))}))
	require.Equal(t, 1, calls)
	require.Equal(t, []int64{42}, session.markedOffsets())
}

func TestConsumeClaimNonMatchingExpression(t *testing.T) {
	t.Parallel()
	session := newFakeSession(t.Context())
	calls := 0
	h, err := New("workers", func(context.Context, *sarama.ConsumerMessage) error {
		calls++
		if calls == 1 {
			return errors.New("failed")
		}
		return nil
	}, WithBypassExpression(`offset == 999`), WithRetryInterval(time.Millisecond))
	require.NoError(t, err)

	require.NoError(t, h.ConsumeClaim(session, fakeClaim{messages: closedMessages(testMessage(42))}))
	require.Equal(t, 2, calls)
	require.Equal(t, []int64{42}, session.markedOffsets())
}

func TestConsumeClaimCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	session := newFakeSession(ctx)
	started := make(chan struct{})
	h, err := New("workers", func(context.Context, *sarama.ConsumerMessage) error {
		close(started)
		return errors.New("failed")
	}, WithRetryInterval(time.Hour))
	require.NoError(t, err)

	done := consumeAsync(h, session, closedMessages(testMessage(42)))
	waitSignal(t, started)
	cancel()
	require.NoError(t, receive(t, done))
	require.Empty(t, session.markedOffsets())
}

func TestSetBypassExpressionConcurrent(t *testing.T) {
	t.Parallel()
	h, err := New("workers", func(context.Context, *sarama.ConsumerMessage) error { return nil })
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 10 {
				raw := fmt.Sprintf(`offset == %d`, i*10+j)
				if j%3 == 0 {
					raw = ""
				}
				if err := h.SetBypassExpression(raw); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func testMessage(offset int64) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{Topic: "orders", Partition: 3, Offset: offset}
}

func closedMessages(messages ...*sarama.ConsumerMessage) <-chan *sarama.ConsumerMessage {
	ch := make(chan *sarama.ConsumerMessage, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	close(ch)
	return ch
}

func consumeAsync(h *Handler, session sarama.ConsumerGroupSession, messages <-chan *sarama.ConsumerMessage) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- h.ConsumeClaim(session, fakeClaim{messages: messages})
	}()
	return done
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value, ok := <-ch:
		require.True(t, ok, "channel closed before a value was received")
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a value")
		var zero T
		return zero
	}
}

func waitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a signal")
	}
}

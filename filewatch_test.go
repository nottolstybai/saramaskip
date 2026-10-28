package saramaskip

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

func TestWatchBypassExpressionFile(t *testing.T) {
	t.Parallel()
	path := expressionFile(t, `offset == 1`)
	h := newWatchHandler(t)
	cancel, done := startFileWatcher(t, h, path, nil)
	require.Eventually(t, func() bool { return fileExpressionMatches(h, 1) }, time.Second, 5*time.Millisecond)

	writeExpression(t, path, `offset == 2`)
	require.Eventually(t, func() bool { return fileExpressionMatches(h, 2) }, time.Second, 5*time.Millisecond)
	stopFileWatcher(t, cancel, done)
}

func TestWatchBypassExpressionFileEmpty(t *testing.T) {
	t.Parallel()
	path := expressionFile(t, `offset == 1`)
	h := newWatchHandler(t)
	cancel, done := startFileWatcher(t, h, path, nil)
	require.Eventually(t, func() bool { return fileExpressionMatches(h, 1) }, time.Second, 5*time.Millisecond)

	writeExpression(t, path, "")
	require.Eventually(t, func() bool { return h.state.Load().expression == nil }, time.Second, 5*time.Millisecond)
	stopFileWatcher(t, cancel, done)
}

func TestWatchBypassExpressionFileInvalidUpdate(t *testing.T) {
	t.Parallel()
	path := expressionFile(t, `offset == 1`)
	h := newWatchHandler(t)
	errs := make(chan error, 10)
	cancel, done := startFileWatcher(t, h, path, func(err error) { errs <- err })
	require.Eventually(t, func() bool { return fileExpressionMatches(h, 1) }, time.Second, 5*time.Millisecond)
	validState := h.state.Load()

	writeExpression(t, path, `offset ==`)
	require.Error(t, receive(t, errs))
	require.Same(t, validState, h.state.Load())
	select {
	case err := <-errs:
		t.Fatalf("same invalid content reported twice: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	writeExpression(t, path, `offset == 2`)
	require.Eventually(t, func() bool { return fileExpressionMatches(h, 2) }, time.Second, 5*time.Millisecond)
	stopFileWatcher(t, cancel, done)
}

func TestWatchBypassExpressionFileReadErrorRecovery(t *testing.T) {
	t.Parallel()
	path := expressionFile(t, `offset == 1`)
	h := newWatchHandler(t)
	errs := make(chan error, 10)
	cancel, done := startFileWatcher(t, h, path, func(err error) { errs <- err })
	require.Eventually(t, func() bool { return fileExpressionMatches(h, 1) }, time.Second, 5*time.Millisecond)
	validState := h.state.Load()

	require.NoError(t, os.Remove(path))
	require.ErrorContains(t, receive(t, errs), "read bypass expression file")
	require.Same(t, validState, h.state.Load())

	writeExpression(t, path, `offset == 2`)
	require.Eventually(t, func() bool { return fileExpressionMatches(h, 2) }, time.Second, 5*time.Millisecond)
	stopFileWatcher(t, cancel, done)
}

func TestWatchBypassExpressionFileValidation(t *testing.T) {
	t.Parallel()
	h := newWatchHandler(t)
	validPath := expressionFile(t, `offset == 1`)
	invalidPath := expressionFile(t, `offset ==`)
	missingPath := filepath.Join(t.TempDir(), "missing.expression")
	tests := []struct {
		name     string
		path     string
		interval time.Duration
		want     string
	}{
		{name: "empty path", interval: time.Millisecond, want: "path"},
		{name: "zero interval", path: validPath, want: "interval"},
		{name: "negative interval", path: validPath, interval: -time.Millisecond, want: "interval"},
		{name: "missing file", path: missingPath, interval: time.Millisecond, want: "read bypass expression file"},
		{name: "invalid initial expression", path: invalidPath, interval: time.Millisecond, want: "parse bypass expression"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := h.WatchBypassExpressionFile(t.Context(), tt.path, tt.interval, nil)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func newWatchHandler(t *testing.T) *Handler {
	t.Helper()
	h, err := New("workers", func(context.Context, *sarama.ConsumerMessage) error { return nil })
	require.NoError(t, err)
	return h
}

func expressionFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "bypass.expression")
	writeExpression(t, path, content)
	return path
}

func writeExpression(t *testing.T, path, content string) {
	t.Helper()
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
}

func startFileWatcher(t *testing.T, h *Handler, path string, onError func(error)) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- h.WatchBypassExpressionFile(ctx, path, time.Millisecond, onError)
	}()
	return cancel, done
}

func stopFileWatcher(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	require.NoError(t, receive(t, done))
}

func fileExpressionMatches(h *Handler, offset int64) bool {
	expr := h.state.Load().expression
	return expr != nil && expr.Match(evaluationContext{Offset: offset})
}

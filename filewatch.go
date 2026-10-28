package saramaskip

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

func (h *Handler) WatchBypassExpressionFile(
	ctx context.Context,
	path string,
	interval time.Duration,
	onError func(error),
) error {
	if path == "" {
		return errors.New("saramaskip: bypass expression file path is required")
	}
	if interval <= 0 {
		return errors.New("saramaskip: bypass expression file interval must be positive")
	}

	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("saramaskip: read bypass expression file: %w", err)
	}
	lastContent := string(content)
	if err := h.SetBypassExpression(lastContent); err != nil {
		return err
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lastReadError := ""
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			content, err := os.ReadFile(path)
			if err != nil {
				if err.Error() != lastReadError {
					reportWatchError(onError, fmt.Errorf("saramaskip: read bypass expression file: %w", err))
					lastReadError = err.Error()
				}
				continue
			}
			lastReadError = ""

			currentContent := string(content)
			if currentContent == lastContent {
				continue
			}
			lastContent = currentContent
			if err := h.SetBypassExpression(currentContent); err != nil {
				reportWatchError(onError, err)
			}
		}
	}
}

func reportWatchError(onError func(error), err error) {
	if onError != nil {
		onError(err)
	}
}

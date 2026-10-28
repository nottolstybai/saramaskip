package saramaskip

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

const defaultRetryInterval = time.Second

type MessageHandler func(context.Context, *sarama.ConsumerMessage) error

type Handler struct {
	group         string
	handler       MessageHandler
	retryInterval time.Duration

	state atomic.Pointer[bypassState]
}

type Option func(*Handler) error

func New(group string, handler MessageHandler, opts ...Option) (*Handler, error) {
	if group == "" {
		return nil, errors.New("saramaskip: consumer group is required")
	}
	if handler == nil {
		return nil, errors.New("saramaskip: message handler is required")
	}

	h := &Handler{
		group:         group,
		handler:       handler,
		retryInterval: defaultRetryInterval,
	}
	h.state.Store(&bypassState{changed: make(chan struct{})})

	for _, opt := range opts {
		if opt == nil {
			return nil, errors.New("saramaskip: option is required")
		}
		if err := opt(h); err != nil {
			return nil, err
		}
	}
	return h, nil
}

func WithRetryInterval(interval time.Duration) Option {
	return func(h *Handler) error {
		if interval <= 0 {
			return errors.New("saramaskip: retry interval must be positive")
		}
		h.retryInterval = interval
		return nil
	}
}

func WithBypassExpression(raw string) Option {
	return func(h *Handler) error {
		return h.SetBypassExpression(raw)
	}
}

func (h *Handler) SetBypassExpression(raw string) error {
	expr, err := parseExpression(raw)
	if err != nil {
		return err
	}
	h.setExpression(expr)
	return nil
}

func (h *Handler) setExpression(expr expression) {
	state := &bypassState{
		expression: expr,
		changed:    make(chan struct{}),
	}
	oldState := h.state.Swap(state)
	close(oldState.changed)
}

func (h *Handler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *Handler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case <-session.Context().Done():
			return nil
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			h.consumeMessage(session, msg)
			if session.Context().Err() != nil {
				return nil
			}
		}
	}
}

func (h *Handler) consumeMessage(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	evalCtx := evaluationContext{
		Group:     h.group,
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}

	for {
		state := h.state.Load()
		if matches(state.expression, evalCtx) {
			session.MarkMessage(msg, "")
			return
		}
		if session.Context().Err() != nil {
			return
		}

		evalCtx.Attempt++
		err := h.handler(session.Context(), msg)
		if err == nil {
			session.MarkMessage(msg, "")
			return
		}
		evalCtx.Error = err.Error()

		state = h.state.Load()
		if matches(state.expression, evalCtx) {
			session.MarkMessage(msg, "")
			return
		}

		timer := time.NewTimer(h.retryInterval)
		select {
		case <-timer.C:
		case <-state.changed:
			timer.Stop()
		case <-session.Context().Done():
			timer.Stop()
			return
		}
	}
}

func matches(expr expression, evalCtx evaluationContext) bool {
	return evalCtx.Attempt > 0 && expr != nil && expr.Match(evalCtx)
}

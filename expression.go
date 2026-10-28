package saramaskip

import "strings"

type expression interface {
	Match(evaluationContext) bool
}

type evaluationContext struct {
	Group     string
	Topic     string
	Partition int32
	Offset    int64
	Attempt   int
	Error     string
}

type bypassState struct {
	expression expression
	changed    chan struct{}
}

type andExpression struct {
	left  expression
	right expression
}

func (e andExpression) Match(ctx evaluationContext) bool {
	return e.left.Match(ctx) && e.right.Match(ctx)
}

type orExpression struct {
	left  expression
	right expression
}

func (e orExpression) Match(ctx evaluationContext) bool {
	return e.left.Match(ctx) || e.right.Match(ctx)
}

type notExpression struct {
	inner expression
}

func (e notExpression) Match(ctx evaluationContext) bool {
	return !e.inner.Match(ctx)
}

type stringField uint8

const (
	stringFieldTopic stringField = iota
	stringFieldGroup
	stringFieldError
)

type stringComparison struct {
	field    stringField
	operator tokenKind
	value    string
}

func (c stringComparison) Match(ctx evaluationContext) bool {
	var actual string
	switch c.field {
	case stringFieldTopic:
		actual = ctx.Topic
	case stringFieldGroup:
		actual = ctx.Group
	case stringFieldError:
		actual = ctx.Error
	}

	switch c.operator {
	case tokenEqual:
		return actual == c.value
	case tokenNotEqual:
		return actual != c.value
	case tokenContains:
		return strings.Contains(actual, c.value)
	default:
		return false
	}
}

type numberField uint8

const (
	numberFieldPartition numberField = iota
	numberFieldOffset
	numberFieldAttempt
)

type numberComparison struct {
	field    numberField
	operator tokenKind
	value    int64
}

func (c numberComparison) Match(ctx evaluationContext) bool {
	var actual int64
	switch c.field {
	case numberFieldPartition:
		actual = int64(ctx.Partition)
	case numberFieldOffset:
		actual = ctx.Offset
	case numberFieldAttempt:
		actual = int64(ctx.Attempt)
	}

	switch c.operator {
	case tokenEqual:
		return actual == c.value
	case tokenNotEqual:
		return actual != c.value
	case tokenLess:
		return actual < c.value
	case tokenLessEqual:
		return actual <= c.value
	case tokenGreater:
		return actual > c.value
	case tokenGreaterEqual:
		return actual >= c.value
	default:
		return false
	}
}

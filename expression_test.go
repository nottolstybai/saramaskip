package saramaskip

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseExpressionMatch(t *testing.T) {
	t.Parallel()
	ctx := evaluationContext{
		Group:     "workers",
		Topic:     "orders",
		Partition: 3,
		Offset:    12042,
		Attempt:   4,
		Error:     "duplicate key \"id\"\nretry later",
	}
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "topic equal", raw: `topic == "orders"`, want: true},
		{name: "topic not equal", raw: `topic != "orders"`, want: false},
		{name: "group", raw: `group == "workers"`, want: true},
		{name: "error contains", raw: `error contains "duplicate key"`, want: true},
		{name: "contains is case sensitive", raw: `error contains "Duplicate"`, want: false},
		{name: "partition", raw: `partition == 3`, want: true},
		{name: "offset", raw: `offset == 12042`, want: true},
		{name: "attempt", raw: `attempt >= 4`, want: true},
		{name: "less", raw: `offset < 12043`, want: true},
		{name: "less equal", raw: `offset <= 12042`, want: true},
		{name: "greater", raw: `offset > 12041`, want: true},
		{name: "greater equal", raw: `offset >= 12042`, want: true},
		{name: "numeric not equal", raw: `partition != 4`, want: true},
		{name: "negative number", raw: `offset > -1`, want: true},
		{name: "and", raw: `topic == "orders" and partition == 3`, want: true},
		{name: "or", raw: `topic == "other" or partition == 3`, want: true},
		{name: "not", raw: `not topic == "other"`, want: true},
		{name: "and before or", raw: `topic == "other" or topic == "orders" and partition == 3`, want: true},
		{name: "not before and", raw: `not topic == "other" and partition == 3`, want: true},
		{name: "parentheses", raw: `(topic == "other" or topic == "orders") and partition == 4`, want: false},
		{name: "escaped quote", raw: `error contains "key \"id\""`, want: true},
		{name: "escaped newline", raw: `error contains "\n"`, want: true},
		{name: "whitespace", raw: " \n topic == \"orders\" \t", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			expr, err := parseExpression(tt.raw)
			require.NoError(t, err)
			require.NotNil(t, expr)
			require.Equal(t, tt.want, expr.Match(ctx))
		})
	}
}

func TestParseExpressionEmpty(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"", " \t\n", "\u2003"} {
		expr, err := parseExpression(raw)
		require.NoError(t, err)
		require.Nil(t, expr)
	}
}

func TestParseExpressionErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "missing operator", raw: "topic", want: "position 6: expected an operator"},
		{name: "missing value", raw: "topic ==", want: "position 9: expected a value"},
		{name: "unknown field", raw: "unknown == 1", want: `position 1: unknown field "unknown"`},
		{name: "numeric contains", raw: `partition contains "3"`, want: `position 11: invalid operator "contains"`},
		{name: "string comparison", raw: `topic > "orders"`, want: `position 7: invalid operator ">"`},
		{name: "number expected", raw: `partition == "3"`, want: "position 14: expected an integer"},
		{name: "string expected", raw: `topic == 1`, want: "position 10: expected a string"},
		{name: "integer overflow", raw: `offset == 9223372036854775808`, want: "position 11: invalid integer"},
		{name: "trailing token", raw: `offset == 1 garbage`, want: `position 13: unexpected token "garbage"`},
		{name: "single equal", raw: `offset = 1`, want: `position 8: expected "=="`},
		{name: "unterminated string", raw: `error == "value`, want: "position 10: unterminated string"},
		{name: "invalid escape", raw: `error == "\q"`, want: "position 10: invalid string"},
		{name: "missing right parenthesis", raw: `(offset == 1`, want: `position 13: expected ")"`},
		{name: "empty parentheses", raw: `()`, want: `position 2: expected a field`},
		{name: "missing operand", raw: `offset == 1 and`, want: "position 16: expected a field"},
		{name: "unexpected character", raw: `offset == @`, want: "position 11: unexpected character"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			expr, err := parseExpression(tt.raw)
			require.ErrorContains(t, err, tt.want)
			require.Nil(t, expr)
		})
	}
}

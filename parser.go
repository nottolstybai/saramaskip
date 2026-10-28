package saramaskip

import (
	"fmt"
	"strconv"
)

type parser struct {
	tokens []token
	offset int
}

func parseExpression(raw string) (expression, error) {
	tokens, err := lex(raw)
	if err != nil {
		return nil, err
	}
	p := parser{tokens: tokens}
	if p.peek().kind == tokenEOF {
		return nil, nil
	}

	expr, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if tok := p.peek(); tok.kind != tokenEOF {
		return nil, parseErrorf(tok.position, "unexpected token %q", tok.text)
	}
	return expr, nil
}

func (p *parser) parseOr() (expression, error) {
	expr, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.match(tokenOr) {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		expr = orExpression{left: expr, right: right}
	}
	return expr, nil
}

func (p *parser) parseAnd() (expression, error) {
	expr, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.match(tokenAnd) {
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		expr = andExpression{left: expr, right: right}
	}
	return expr, nil
}

func (p *parser) parseUnary() (expression, error) {
	if p.match(tokenNot) {
		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return notExpression{inner: expr}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (expression, error) {
	if p.match(tokenLeftParen) {
		expr, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if !p.match(tokenRightParen) {
			return nil, parseErrorf(p.peek().position, "expected %q", ")")
		}
		return expr, nil
	}
	return p.parsePredicate()
}

func (p *parser) parsePredicate() (expression, error) {
	field := p.peek()
	if field.kind != tokenIdentifier {
		return nil, parseErrorf(field.position, "expected a field or %q", "(")
	}
	p.offset++

	switch field.text {
	case "topic", "group", "error", "partition", "offset", "attempt":
	default:
		return nil, parseErrorf(field.position, "unknown field %q", field.text)
	}

	operator := p.peek()
	if operator.kind == tokenEOF {
		return nil, parseErrorf(operator.position, "expected an operator after field %q", field.text)
	}
	p.offset++
	value := p.peek()
	if value.kind == tokenEOF {
		return nil, parseErrorf(value.position, "expected a value after operator %q", operator.text)
	}
	p.offset++

	switch field.text {
	case "topic", "group", "error":
		return parseStringPredicate(field, operator, value)
	default:
		return parseNumberPredicate(field, operator, value)
	}
}

func parseStringPredicate(field, operator, value token) (expression, error) {
	switch operator.kind {
	case tokenEqual, tokenNotEqual, tokenContains:
	default:
		return nil, parseErrorf(operator.position, "invalid operator %q for string field %q", operator.text, field.text)
	}
	if value.kind != tokenString {
		return nil, parseErrorf(value.position, "expected a string value for field %q", field.text)
	}

	fields := map[string]stringField{
		"topic": stringFieldTopic,
		"group": stringFieldGroup,
		"error": stringFieldError,
	}
	return stringComparison{field: fields[field.text], operator: operator.kind, value: value.text}, nil
}

func parseNumberPredicate(field, operator, value token) (expression, error) {
	switch operator.kind {
	case tokenEqual, tokenNotEqual, tokenLess, tokenLessEqual, tokenGreater, tokenGreaterEqual:
	default:
		return nil, parseErrorf(operator.position, "invalid operator %q for numeric field %q", operator.text, field.text)
	}
	if value.kind != tokenNumber {
		return nil, parseErrorf(value.position, "expected an integer value for field %q", field.text)
	}
	number, err := strconv.ParseInt(value.text, 10, 64)
	if err != nil {
		return nil, parseErrorf(value.position, "invalid integer %q", value.text)
	}

	fields := map[string]numberField{
		"partition": numberFieldPartition,
		"offset":    numberFieldOffset,
		"attempt":   numberFieldAttempt,
	}
	return numberComparison{field: fields[field.text], operator: operator.kind, value: number}, nil
}

func (p *parser) peek() token {
	return p.tokens[p.offset]
}

func (p *parser) match(kind tokenKind) bool {
	if p.peek().kind != kind {
		return false
	}
	p.offset++
	return true
}

func (t token) String() string {
	if t.kind == tokenEOF {
		return "end of expression"
	}
	return fmt.Sprintf("%q", t.text)
}

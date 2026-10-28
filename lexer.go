package saramaskip

import (
	"fmt"
	"strconv"
	"unicode"
	"unicode/utf8"
)

type tokenKind uint8

const (
	tokenInvalid tokenKind = iota
	tokenEOF
	tokenIdentifier
	tokenString
	tokenNumber
	tokenEqual
	tokenNotEqual
	tokenLess
	tokenLessEqual
	tokenGreater
	tokenGreaterEqual
	tokenLeftParen
	tokenRightParen
	tokenAnd
	tokenOr
	tokenNot
	tokenContains
)

type token struct {
	kind     tokenKind
	text     string
	position int
}

type lexer struct {
	input  string
	offset int
}

func lex(input string) ([]token, error) {
	l := lexer{input: input}
	var tokens []token
	for {
		l.skipWhitespace()
		if l.offset == len(l.input) {
			tokens = append(tokens, token{kind: tokenEOF, position: len(input) + 1})
			return tokens, nil
		}

		tok, err := l.nextToken()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, tok)
	}
}

func (l *lexer) nextToken() (token, error) {
	position := l.offset + 1
	ch := l.input[l.offset]

	switch {
	case isIdentifierStart(ch):
		start := l.offset
		l.offset++
		for l.offset < len(l.input) && isIdentifierPart(l.input[l.offset]) {
			l.offset++
		}
		text := l.input[start:l.offset]
		kind := tokenIdentifier
		switch text {
		case "and":
			kind = tokenAnd
		case "or":
			kind = tokenOr
		case "not":
			kind = tokenNot
		case "contains":
			kind = tokenContains
		}
		return token{kind: kind, text: text, position: position}, nil
	case isDigit(ch) || ch == '-' && l.offset+1 < len(l.input) && isDigit(l.input[l.offset+1]):
		start := l.offset
		if ch == '-' {
			l.offset++
		}
		for l.offset < len(l.input) && isDigit(l.input[l.offset]) {
			l.offset++
		}
		return token{kind: tokenNumber, text: l.input[start:l.offset], position: position}, nil
	case ch == '"':
		return l.scanString()
	}

	switch ch {
	case '=':
		return l.scanOperator(tokenEqual, "==")
	case '!':
		return l.scanOperator(tokenNotEqual, "!=")
	case '<':
		l.offset++
		if l.consume('=') {
			return token{kind: tokenLessEqual, text: "<=", position: position}, nil
		}
		return token{kind: tokenLess, text: "<", position: position}, nil
	case '>':
		l.offset++
		if l.consume('=') {
			return token{kind: tokenGreaterEqual, text: ">=", position: position}, nil
		}
		return token{kind: tokenGreater, text: ">", position: position}, nil
	case '(':
		l.offset++
		return token{kind: tokenLeftParen, text: "(", position: position}, nil
	case ')':
		l.offset++
		return token{kind: tokenRightParen, text: ")", position: position}, nil
	default:
		return token{}, parseErrorf(position, "unexpected character %q", ch)
	}
}

func (l *lexer) scanOperator(kind tokenKind, operator string) (token, error) {
	position := l.offset + 1
	if l.offset+1 >= len(l.input) || l.input[l.offset:l.offset+2] != operator {
		return token{}, parseErrorf(position, "expected %q", operator)
	}
	l.offset += 2
	return token{kind: kind, text: operator, position: position}, nil
}

func (l *lexer) scanString() (token, error) {
	start := l.offset
	position := start + 1
	l.offset++
	for l.offset < len(l.input) {
		switch l.input[l.offset] {
		case '\n', '\r':
			return token{}, parseErrorf(position, "unterminated string")
		case '\\':
			l.offset++
			if l.offset == len(l.input) {
				return token{}, parseErrorf(position, "unterminated string")
			}
			l.offset++
		case '"':
			l.offset++
			raw := l.input[start:l.offset]
			value, err := strconv.Unquote(raw)
			if err != nil {
				return token{}, parseErrorf(position, "invalid string: %v", err)
			}
			return token{kind: tokenString, text: value, position: position}, nil
		default:
			_, size := utf8.DecodeRuneInString(l.input[l.offset:])
			l.offset += size
		}
	}
	return token{}, parseErrorf(position, "unterminated string")
}

func (l *lexer) skipWhitespace() {
	for l.offset < len(l.input) {
		r, size := utf8.DecodeRuneInString(l.input[l.offset:])
		if !unicode.IsSpace(r) {
			return
		}
		l.offset += size
	}
}

func (l *lexer) consume(ch byte) bool {
	if l.offset == len(l.input) || l.input[l.offset] != ch {
		return false
	}
	l.offset++
	return true
}

func isIdentifierStart(ch byte) bool {
	return ch == '_' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || isDigit(ch)
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func parseErrorf(position int, format string, args ...any) error {
	return fmt.Errorf("saramaskip: parse bypass expression at position %d: %s", position, fmt.Sprintf(format, args...))
}

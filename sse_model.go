package main

import (
	"bytes"
	"errors"
)

var errSSEModel = errors.New("invalid or oversized upstream SSE event")

// Rewrites only complete events. Memory is bounded per event, not per answer.
// Line endings are normalized to LF; field values and ordering are preserved.
type sseModelWriter struct {
	model        string
	line         []byte
	lines        [][]byte
	size         int
	skipLF       bool
	done, failed bool
}

func (s *sseModelWriter) feed(p []byte, emit func([]byte) error) error {
	for _, b := range p {
		if s.skipLF {
			s.skipLF = false
			if b == '\n' {
				continue
			}
		}
		s.size++
		if s.size > maxSSEEvent {
			return errSSEModel
		}
		if b != '\r' && b != '\n' {
			s.line = append(s.line, b)
			continue
		}
		s.skipLF = b == '\r'
		if len(s.line) != 0 {
			s.lines = append(s.lines, s.line)
			s.line = nil
			continue
		}
		data, err := s.event()
		if err != nil {
			return err
		}
		if err := emit(data); err != nil {
			return err
		}
		s.lines = nil
		s.size = 0
	}
	return nil
}

func (s *sseModelWriter) event() ([]byte, error) {
	var data []byte
	first := -1
	for i, line := range s.lines {
		if !isDataLine(line) {
			continue
		}
		if first == -1 {
			first = i
		} else {
			data = append(data, '\n')
		}
		value := bytes.TrimPrefix(line, []byte("data"))
		value = bytes.TrimPrefix(value, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		data = append(data, value...)
	}
	var replacement []byte
	if first >= 0 {
		if bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]")) {
			s.done = true
		} else {
			var fields rawObject
			if decodeJSON(data, &fields) != nil || fields == nil {
				return nil, errSSEModel
			}
			if _, hasError := fields["error"]; hasError {
				s.failed = true
			} else {
				_, hasChoices := fields["choices"]
				_, hasModel := fields["model"]
				object, _ := rawString(fields["object"])
				if hasChoices || hasModel || object == "chat.completion.chunk" {
					fields["model"] = encodeJSON(s.model)
					replacement = append([]byte("data: "), encodeJSON(fields)...)
				}
			}
		}
	}
	var out []byte
	for i, line := range s.lines {
		if replacement != nil && isDataLine(line) {
			if i != first {
				continue
			}
			line = replacement
		}
		out = append(out, line...)
		out = append(out, '\n')
	}
	return append(out, '\n'), nil
}

func isDataLine(line []byte) bool {
	return bytes.Equal(line, []byte("data")) || bytes.HasPrefix(line, []byte("data:"))
}

func (s *sseModelWriter) finish() error {
	if len(s.line) != 0 || len(s.lines) != 0 || (!s.done && !s.failed) {
		return errSSEModel
	}
	return nil
}

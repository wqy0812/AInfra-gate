package aigate

// A push parser shared by recording and equal-output verification. It retains
// neither content strings nor whole SSE events. All scratch/state has a fixed
// bound, including for a single arbitrarily long JSON string or SSE data line.
import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const responseObserverBuffer = 64 << 10
const responseObserverDepth = 128
const responseErrorLimit = 4 << 10

type responseObservation struct {
	UsageKnown       bool              `json:"usage_known"`
	Usage            map[string]uint64 `json:"usage,omitempty"`
	FinishReasons    []string          `json:"finish_reasons,omitempty"`
	ParseStatus      string            `json:"parse_status"`
	ParseError       string            `json:"parse_error,omitempty"`
	ProtocolComplete bool              `json:"protocol_complete"`
	TransportError   string            `json:"transport_error,omitempty"`
	ErrorSummary     string            `json:"error_summary,omitempty"`
	ResponseError    bool              `json:"response_error,omitempty"`
}

type responseObserver struct {
	onOutput                                        func(time.Time)
	observedAt                                      time.Time
	multipleChoices                                 bool
	input                                           [responseObserverBuffer]byte
	result                                          responseObservation
	parser                                          responseJSONParser
	sse, done, cr                                   bool
	field                                           [16]byte
	fieldLen                                        int
	value, dataLine, skipSpace, lineSeen, eventData bool
	prefix                                          [8]byte
	prefixLen                                       int
	jsonEvent                                       bool
	ended                                           bool
}

func newResponseObserver(h http.Header) *responseObserver {
	o := &responseObserver{sse: strings.Contains(strings.ToLower(h.Get("Content-Type")), "text/event-stream")}
	o.result.ParseStatus = "ok"
	if encoding := strings.TrimSpace(h.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
		o.fail("unsupported_content_encoding")
	}
	return o
}

func (o *responseObserver) fail(code string) {
	if o.result.ParseError == "" {
		o.result.ParseError, o.result.ParseStatus = code, "failed"
	}
}

// Write always accepts all bytes. Observation errors must never affect forwarding.
func (o *responseObserver) Write(p []byte) (int, error) {
	n := len(p)
	if o.onOutput != nil {
		o.observedAt = time.Now()
	}
	if o.ended || o.result.ParseError != "" {
		return n, nil
	}
	for len(p) > 0 {
		k := copy(o.input[:], p)
		for _, b := range o.input[:k] {
			if o.sse {
				o.sseByte(b)
			} else {
				o.parser.feed(b)
			}
			if o.parser.err != "" {
				o.fail(o.parser.err)
			}
			if o.result.ParseError != "" {
				break
			}
		}
		p = p[k:]
		if o.result.ParseError != "" {
			break
		}
	}
	return n, nil
}

func (o *responseObserver) eventByte(b byte) {
	if o.done {
		o.fail("data_after_done")
		return
	}
	if o.jsonEvent {
		o.parser.feed(b)
		return
	}
	if o.prefixLen == 0 && jsonSpace(b) {
		return
	}
	if o.prefixLen == 0 && b == '{' {
		o.jsonEvent = true
		o.parser.feed(b)
		return
	}
	if o.prefixLen >= len(o.prefix) {
		o.fail("invalid_sse_data")
		return
	}
	o.prefix[o.prefixLen] = b
	o.prefixLen++
}

func (o *responseObserver) sseByte(b byte) {
	if o.cr {
		o.cr = false
		if b == '\n' {
			return
		}
	}
	if b == '\r' || b == '\n' {
		o.cr = b == '\r'
		// A field without a colon has an empty value (SSE grammar).
		if !o.value && o.fieldLen == 4 && string(o.field[:4]) == "data" {
			if o.eventData {
				o.eventByte('\n')
			}
			o.eventData = true
		}
		if !o.lineSeen {
			o.endEvent()
		}
		o.fieldLen, o.value, o.dataLine, o.skipSpace, o.lineSeen = 0, false, false, false, false
		return
	}
	o.lineSeen = true
	if !o.value {
		if b != ':' {
			if o.fieldLen < len(o.field) {
				o.field[o.fieldLen] = b
			}
			o.fieldLen = min(o.fieldLen+1, len(o.field)+1)
			return
		}
		o.value, o.skipSpace = true, true
		o.dataLine = o.fieldLen == 4 && string(o.field[:4]) == "data"
		if o.dataLine {
			if o.eventData {
				o.eventByte('\n')
			}
			o.eventData = true
		}
		return
	}
	if o.skipSpace {
		o.skipSpace = false
		if b == ' ' {
			return
		}
	}
	if o.dataLine {
		o.eventByte(b)
	}
}

func (o *responseObserver) mergeParser() {
	o.result.ResponseError = o.result.ResponseError || o.parser.responseError
	if o.parser.usage != nil {
		o.result.Usage = o.parser.usage
		_, o.result.UsageKnown = o.parser.usage["completion_tokens"]
	}
	if len(o.parser.reasons) > 0 {
		o.result.FinishReasons = append([]string(nil), o.parser.reasons...)
	}
	if o.parser.errorSummary != "" {
		o.result.ErrorSummary = o.parser.errorSummary
	}
}

func (o *responseObserver) endEvent() {
	if !o.eventData {
		return
	}
	if o.jsonEvent {
		o.parser.finish()
		// Preserve a completed usage object even if the enclosing event broke.
		o.mergeParser()
		o.multipleChoices = o.multipleChoices || o.parser.multipleChoices
		if o.parser.err == "" && o.parser.generated && o.onOutput != nil {
			o.onOutput(o.observedAt)
		}
		if o.parser.err != "" {
			o.fail(o.parser.err)
		}
	} else if strings.TrimSpace(string(o.prefix[:o.prefixLen])) == "[DONE]" {
		o.done = true
	} else {
		o.fail("invalid_sse_data")
	}
	o.parser = responseJSONParser{}
	o.eventData, o.jsonEvent, o.prefixLen = false, false, 0
}

func (o *responseObserver) finish(transportError string) responseObservation {
	if o.ended {
		return o.result
	}
	o.ended = true
	if o.sse {
		// An unterminated event is incomplete, even when its JSON is valid.
		if o.eventData {
			o.mergeParser()
			o.fail("incomplete_sse_event")
		}
		if !o.done {
			o.fail("sse_done_missing")
		}
	} else {
		o.parser.finish()
		o.mergeParser()
		if o.parser.err != "" {
			o.fail(o.parser.err)
		}
	}
	o.result.TransportError = transportError
	o.result.ProtocolComplete = o.result.ParseError == "" && transportError == "" && !o.result.ResponseError
	return o.result
}

// JSON grammar states: objects use key/end, colon, value, comma/end, key;
// arrays use value/end, comma/end, value. A small lexer validates skipped
// strings too, but only retains keys and relevant scalar values.
const (
	jsonOther = iota
	jsonRoot
	jsonUsage
	jsonChoices
	jsonChoice
	jsonError
	jsonDelta
	jsonTools
	jsonTool
	jsonFunction
)

type responseJSONFrame struct {
	kind  byte
	state uint8
	role  int
	key   string
	usage map[string]uint64
}

type responseJSONParser struct {
	choiceCount                int
	generated, multipleChoices bool
	stringBytes                int
	stack                      [responseObserverDepth]responseJSONFrame
	depth                      int
	started, complete          bool
	lex                        byte
	keyToken, keep, escaped    bool
	unicode                    int
	token                      [responseErrorLimit + 2]byte
	length                     int
	overflow                   bool
	err                        string
	usage                      map[string]uint64
	reasons                    []string
	errorSummary               string
	responseError              bool
	utf                        [utf8.UTFMax]byte
	utfLen                     int
}

func jsonSpace(b byte) bool { return b == ' ' || b == '\t' || b == '\r' || b == '\n' }
func (p *responseJSONParser) fail(code string) {
	if p.err == "" {
		p.err = code
	}
}
func (p *responseJSONParser) top() *responseJSONFrame { return &p.stack[p.depth-1] }
func (p *responseJSONParser) add(b byte) {
	if !p.keep {
		return
	}
	if p.length < len(p.token) {
		p.token[p.length] = b
		p.length++
	} else {
		p.overflow = true
	}
}

func (p *responseJSONParser) valueRole() int {
	if p.depth == 0 {
		return jsonRoot
	}
	f := p.top()
	if f.role == jsonRoot && f.key == "usage" {
		return jsonUsage
	}
	if f.role == jsonRoot && f.key == "choices" {
		return jsonChoices
	}
	if f.role == jsonRoot && f.key == "error" {
		return jsonError
	}
	if f.role == jsonChoices && f.kind == '[' {
		return jsonChoice
	}
	if f.role == jsonChoice && f.key == "delta" {
		return jsonDelta
	}
	if f.role == jsonDelta && f.key == "tool_calls" {
		return jsonTools
	}
	if f.role == jsonTools && f.kind == '[' {
		return jsonTool
	}
	if (f.role == jsonTool && f.key == "function") || (f.role == jsonDelta && f.key == "function_call") {
		return jsonFunction
	}
	return jsonOther
}

func (p *responseJSONParser) outputScalar() bool {
	if p.depth == 0 {
		return false
	}
	f := p.top()
	return (f.role == jsonDelta && (f.key == "content" || f.key == "reasoning_content" || f.key == "reasoning")) ||
		(f.role == jsonFunction && (f.key == "name" || f.key == "arguments")) || (f.role == jsonChoice && f.key == "text")
}

func (p *responseJSONParser) wantsScalar() bool {
	if p.depth == 0 {
		return false
	}
	f := p.top()
	return (f.role == jsonUsage && (f.key == "prompt_tokens" || f.key == "completion_tokens" || f.key == "total_tokens")) ||
		(f.role == jsonChoice && (f.key == "finish_reason" || f.key == "index")) || (f.role == jsonError && f.key == "message") ||
		(f.role == jsonRoot && f.key == "error")
}

func (p *responseJSONParser) valueDone() {
	if p.depth == 0 {
		p.complete = true
		return
	}
	f := p.top()
	if f.kind == '{' {
		f.state = 3
	} else {
		f.state = 1
	}
}

func (p *responseJSONParser) scalar() {
	if !p.keyToken && p.outputScalar() && p.stringBytes > 2 {
		p.generated = true
	}
	if p.keyToken {
		if p.overflow || p.length > 256 {
			p.top().key = ""
		} else {
			var key string
			if json.Unmarshal(p.token[:p.length], &key) != nil {
				p.fail("invalid_json_string")
				return
			}
			p.top().key = key
		}
		p.top().state = 1
		return
	}
	if p.keep && p.wantsScalar() {
		f := p.top()
		raw := p.token[:p.length]
		if f.role == jsonChoice && f.key == "index" {
			index, err := strconv.ParseUint(string(raw), 10, 64)
			p.multipleChoices = p.multipleChoices || err != nil || index > 0
			p.valueDone()
			return
		}
		if f.role == jsonUsage {
			n, err := strconv.ParseUint(string(raw), 10, 64)
			if p.overflow || err != nil {
				p.fail("invalid_usage")
				return
			}
			f.usage[f.key] = n
		} else if string(raw) != "null" {
			if f.role == jsonRoot && f.key == "error" {
				p.responseError = true
			}
			var s string
			if !p.overflow && json.Unmarshal(raw, &s) == nil {
				if f.role == jsonChoice {
					if len(s) > 256 || len(p.reasons) >= 128 {
						p.fail("finish_reason_limit")
						return
					}
					p.reasons = append(p.reasons, s)
				} else {
					p.errorSummary = s
				}
			} else if f.role == jsonError || (f.role == jsonRoot && f.key == "error") {
				// Diagnostic text alone may be truncated; never grow the lexer.
				p.errorSummary = strings.ToValidUTF8(string(raw[1:min(len(raw), responseErrorLimit)]), "?")
			} else {
				p.fail("invalid_finish_reason")
			}
		}
	}
	p.valueDone()
}

func (p *responseJSONParser) feed(b byte) {
	if p.err != "" {
		return
	}
	if p.lex == 's' {
		if p.stringBytes < 3 {
			p.stringBytes++
		}
		p.add(b)
		if b >= utf8.RuneSelf || p.utfLen != 0 {
			p.utf[p.utfLen] = b
			p.utfLen++
			if utf8.FullRune(p.utf[:p.utfLen]) {
				_, size := utf8.DecodeRune(p.utf[:p.utfLen])
				if size == 1 {
					p.fail("invalid_json_utf8")
				}
				p.utfLen = 0
			}
			if p.escaped || p.unicode > 0 {
				p.fail("invalid_json_escape")
			}
			return
		}
		if p.unicode > 0 {
			if !((b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')) {
				p.fail("invalid_json_escape")
			}
			p.unicode--
			return
		}
		if p.escaped {
			p.escaped = false
			if b == 'u' {
				p.unicode = 4
			} else if !strings.ContainsRune(`"\/bfnrt`, rune(b)) {
				p.fail("invalid_json_escape")
			}
			return
		}
		if b == '\\' {
			p.escaped = true
		} else if b == '"' {
			p.lex = 0
			p.scalar()
		} else if b < 0x20 {
			p.fail("invalid_json_string")
		}
		return
	}
	if p.lex == 'a' {
		if !jsonSpace(b) && b != ',' && b != ']' && b != '}' {
			p.add(b)
			if p.length > 128 || p.overflow {
				p.fail("invalid_json_atom")
			}
			return
		}
		if !json.Valid(p.token[:p.length]) {
			p.fail("invalid_json_atom")
			return
		}
		p.lex = 0
		p.scalar()
		if p.err != "" {
			return
		}
	}
	if jsonSpace(b) {
		return
	}
	if p.complete {
		p.fail("trailing_json_data")
		return
	}
	key := false
	if p.depth > 0 {
		f := p.top()
		if f.kind == '{' {
			switch f.state {
			case 0, 4:
				if b == '}' && f.state == 0 {
					p.close()
					return
				}
				if b != '"' {
					p.fail("invalid_json_key")
					return
				}
				key = true
			case 1:
				if b != ':' {
					p.fail("invalid_json_colon")
				} else {
					f.state = 2
				}
				return
			case 3:
				if b == '}' {
					p.close()
				} else if b == ',' {
					f.state = 4
				} else {
					p.fail("invalid_json_separator")
				}
				return
			}
		} else {
			if f.state == 0 && b == ']' {
				p.close()
				return
			}
			if f.state == 1 {
				if b == ']' {
					p.close()
				} else if b == ',' {
					f.state = 2
				} else {
					p.fail("invalid_json_separator")
				}
				return
			}
		}
	} else if b != '{' {
		p.fail("json_object_required")
		return
	}
	if b == '{' || b == '[' {
		if p.wantsScalar() && !(p.top().role == jsonRoot && p.top().key == "error") {
			p.fail("invalid_response_scalar")
			return
		}
		if p.depth >= responseObserverDepth {
			p.fail("json_depth_exceeded")
			return
		}
		role := p.valueRole()
		if role == jsonChoice {
			p.choiceCount++
			p.multipleChoices = p.multipleChoices || p.choiceCount > 1
		}
		if (role == jsonUsage && b != '{') || (role == jsonChoices && b != '[') || (role == jsonChoice && b != '{') {
			p.fail("invalid_response_shape")
			return
		}
		f := responseJSONFrame{kind: b, role: role}
		if role == jsonUsage {
			f.usage = map[string]uint64{}
		}
		p.stack[p.depth] = f
		p.depth++
		p.started = true
		return
	}
	p.keyToken, p.keep, p.length, p.overflow = key, key || p.wantsScalar(), 0, false
	p.stringBytes = 0
	if b == '"' {
		p.stringBytes = 1
		p.lex = 's'
		p.add(b)
		return
	}
	if b == '-' || (b >= '0' && b <= '9') || b == 't' || b == 'f' || b == 'n' {
		// Atoms are always retained briefly to validate the JSON grammar.
		p.lex, p.keep = 'a', true
		p.add(b)
		return
	}
	p.fail("invalid_json_value")
}

func (p *responseJSONParser) close() {
	f := p.top()
	if f.role == jsonError {
		p.responseError = true
	}
	if f.role == jsonUsage {
		p.usage = f.usage
	}
	*f = responseJSONFrame{}
	p.depth--
	p.valueDone()
}

func (p *responseJSONParser) finish() {
	if p.lex == 'a' {
		p.feed(' ')
	}
	if !p.complete || p.depth != 0 || p.lex != 0 {
		p.fail("incomplete_json")
	}
}

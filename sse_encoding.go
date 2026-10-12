package aigate

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func decodeSSEBody(response *http.Response) *GatewayError {
	if err := decodeUpstreamBody(response); err != nil {
		return fault(502, "invalid_upstream_stream", "下游 SSE 内容编码不受支持")
	}
	return nil
}

// The transport only decodes gzip when it added Accept-Encoding itself.
// Decode any remaining gzip before model JSON parsing or SSE forwarding.
// Initialization stays in Read so waiting for the gzip header is still observed
// as an upstream body read and cancelled by the request's transport context.
func decodeUpstreamBody(response *http.Response) error {
	compressed := false
	for _, value := range response.Header.Values("Content-Encoding") {
		for _, coding := range strings.Split(value, ",") {
			switch strings.ToLower(strings.TrimSpace(coding)) {
			case "", "identity":
			case "gzip":
				if compressed {
					return fmt.Errorf("repeated upstream gzip encoding")
				}
				compressed = true
			default:
				return fmt.Errorf("unsupported upstream content encoding")
			}
		}
	}
	if compressed {
		response.Body = &gzipResponseBody{source: response.Body}
		response.Header.Del("Content-Encoding")
		response.Header.Del("Content-Length")
		response.ContentLength = -1
		response.Uncompressed = true
	}
	return nil
}

type gzipResponseBody struct {
	source io.ReadCloser
	reader *gzip.Reader
	err    error
}

func (b *gzipResponseBody) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.reader == nil {
		b.reader, b.err = gzip.NewReader(b.source)
		if b.err == io.EOF {
			b.err = io.ErrUnexpectedEOF // an absent gzip header is not a clean body end
		}
		if b.err != nil {
			return 0, b.err
		}
	}
	return b.reader.Read(p)
}

func (b *gzipResponseBody) Close() error {
	if b.reader != nil {
		_ = b.reader.Close()
	}
	return b.source.Close()
}

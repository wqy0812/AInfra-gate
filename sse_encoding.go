package main

import (
	"compress/gzip"
	"io"
	"net/http"
	"strings"
)

// The transport only decodes gzip when it added Accept-Encoding itself.
// Decode any remaining gzip before both SSE observation and client forwarding.
// Initialization stays in Read so waiting for the gzip header is still observed
// as an upstream body read and cancelled by the request's transport context.
func decodeSSEBody(response *http.Response) *GatewayError {
	compressed := false
	for _, value := range response.Header.Values("Content-Encoding") {
		for _, coding := range strings.Split(value, ",") {
			switch strings.ToLower(strings.TrimSpace(coding)) {
			case "", "identity":
			case "gzip":
				if compressed {
					return fault(502, "invalid_upstream_stream", "下游 SSE 内容编码不受支持")
				}
				compressed = true
			default:
				return fault(502, "invalid_upstream_stream", "下游 SSE 内容编码不受支持")
			}
		}
	}
	if compressed {
		response.Body = &gzipSSEBody{source: response.Body}
		response.Header.Del("Content-Encoding")
		response.Header.Del("Content-Length")
		response.ContentLength = -1
		response.Uncompressed = true
	}
	return nil
}

type gzipSSEBody struct {
	source io.ReadCloser
	reader *gzip.Reader
	err    error
}

func (b *gzipSSEBody) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.reader == nil {
		b.reader, b.err = gzip.NewReader(b.source)
		if b.err == io.EOF {
			b.err = io.ErrUnexpectedEOF // an absent gzip header is not a clean stream end
		}
		if b.err != nil {
			return 0, b.err
		}
	}
	return b.reader.Read(p)
}

func (b *gzipSSEBody) Close() error {
	if b.reader != nil {
		_ = b.reader.Close()
	}
	return b.source.Close()
}

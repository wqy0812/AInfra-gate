package aigate

import (
	"net/http"
	"time"
)

const businessReadHeaderTimeout = 30 * time.Second

func businessHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: businessReadHeaderTimeout}
}

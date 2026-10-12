package aigate

import (
	"net/http"
	"time"
)

// abandonRequestBody is called before responding without consuming the upload.
// Connection: close alone skips net/http's pre-response drain, but its later
// Body.Close can still block. Expire reads before explicitly closing the body so
// both response delivery and connection cleanup are independent of the sender.
func abandonRequestBody(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil || r.Body == http.NoBody {
		return
	}
	if r.ProtoMajor == 1 {
		w.Header().Set("Connection", "close")
		_ = http.NewResponseController(w).SetReadDeadline(time.Now())
	}
	// HTTP/2 closes only the request stream without draining. Do not set a
	// connection header or a read deadline that could interrupt its response.
	_ = r.Body.Close()
}

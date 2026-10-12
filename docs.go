package aigate

import (
	_ "embed"
	"net/http"
)

//go:embed openapi.json
var openAPI []byte

func serveOpenAPI(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(openAPI)
}

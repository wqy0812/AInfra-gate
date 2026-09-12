package main

import (
	"crypto/subtle"
	"embed"
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"strings"
)

//go:embed admin_web/*
var adminAssets embed.FS

//go:embed admin_openapi.json
var adminOpenAPI []byte

func (g *Gateway) adminHandler() http.Handler {
	assets, _ := fs.Sub(adminAssets, "admin_web")
	files := http.FileServer(http.FS(assets))
	key := "Bearer " + os.Getenv(g.config.Management.APIKeyEnv)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
		if !strings.HasPrefix(r.URL.Path, "/admin/v1/") {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				w.WriteHeader(405)
				return
			}
			if r.URL.Path == "/openapi.json" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(adminOpenAPI)
				return
			}
			switch r.URL.Path {
			case "/", "/index.html", "/app.js", "/backends.js", "/style.css":
				files.ServeHTTP(w, r)
			default:
				http.NotFound(w, r)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		id := randomID()
		w.Header().Set("X-Request-ID", id)
		reject := func(status int, code, message, operationID string) {
			w.WriteHeader(status)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"code": code, "message": message, "request_id": id, "operation_id": operationID}})
		}
		if g.management == nil {
			reject(404, "management_disabled", "管理功能未启用", "")
			return
		}
		if subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte(key)) != 1 {
			reject(401, "unauthorized", "管理凭据无效", "")
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			if err != nil || u.Host != r.Host || (u.Scheme != "http" && u.Scheme != "https") {
				reject(403, "cross_origin_denied", "管理接口不允许跨域访问", "")
				return
			}
		}
		if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
			reject(403, "cross_origin_denied", "管理接口不允许跨站访问", "")
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/admin/v1/")
		if r.Method == http.MethodGet {
			if path == "backends" {
				_ = json.NewEncoder(w).Encode(g.management.backendList())
				return
			}
			s := g.management.status()
			if path == "status" {
				_ = json.NewEncoder(w).Encode(s)
				return
			}
			if strings.HasPrefix(path, "operations/") {
				op := operationByID(&s.ManagedState, strings.TrimPrefix(path, "operations/"))
				if op != nil {
					_ = json.NewEncoder(w).Encode(op)
					return
				}
			}
			reject(404, "not_found", "管理接口或操作不存在", "")
			return
		}

		backendParts := strings.Split(path, "/")
		createBackend := path == "backends" && r.Method == http.MethodPost
		editBackend := len(backendParts) == 2 && backendParts[0] == "backends" && r.Method == http.MethodPatch
		if createBackend || editBackend {
			if !strings.EqualFold(strings.TrimSpace(strings.SplitN(r.Header.Get("Content-Type"), ";", 2)[0]), "application/json") {
				reject(415, "invalid_content_type", "需要 application/json", "")
				return
			}
			var req BackendWriteRequest
			data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 64*1024))
			if err != nil || strictDecode(data, &req) != nil {
				reject(400, "invalid_request", "后端请求格式无效或超过 64 KiB", "")
				return
			}
			if editBackend {
				if req.Target != "" && req.Target != backendParts[1] {
					reject(400, "target_mismatch", "目标与路径不一致", "")
					return
				}
				req.Target = backendParts[1]
			}
			op, problem := g.management.writeBackend(createBackend, req)
			if problem != nil {
				reject(problem.Status, problem.Code, problem.Message, problem.OperationID)
				return
			}
			if createBackend {
				w.WriteHeader(http.StatusCreated)
			}
			_ = json.NewEncoder(w).Encode(op)
			return
		}
		if r.Method != http.MethodPost {
			reject(405, "method_not_allowed", "请求方法不支持", "")
			return
		}
		if !strings.EqualFold(strings.TrimSpace(strings.SplitN(r.Header.Get("Content-Type"), ";", 2)[0]), "application/json") {
			reject(415, "invalid_content_type", "需要 application/json", "")
			return
		}
		var req OperationRequest
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
		d.DisallowUnknownFields()
		if d.Decode(&req) != nil || d.Decode(new(any)) != io.EOF {
			reject(400, "invalid_request", "管理请求格式无效", "")
			return
		}
		kind := ""
		switch path {
		case "warmups":
			kind = "warmup"
		case "switches":
			kind = "switch"
		}
		parts := strings.Split(path, "/")
		if len(parts) == 3 && parts[0] == "backends" {
			if req.Target != "" && req.Target != parts[1] {
				reject(400, "target_mismatch", "目标与路径不一致", "")
				return
			}
			req.Target = parts[1]
			switch parts[2] {
			case "maintenance", "restore", "confirm-drain":
				kind = parts[2]
			}
		}
		if kind == "" {
			reject(404, "not_found", "管理接口不存在", "")
			return
		}
		op, err := g.management.submit(kind, req)
		if err != nil {
			reject(err.Status, err.Code, err.Message, err.OperationID)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(op)
	})
}

package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

const maxControlRequest = 128 << 10

type controlRequest struct {
	Command string          `json:"command"`
	Payload json.RawMessage `json:"payload,omitempty"`
}
type controlResponse struct {
	Result json.RawMessage  `json:"result,omitempty"`
	Error  *ManagementError `json:"error,omitempty"`
}

type controlServer struct {
	listener    net.Listener
	management  *Management
	mu          sync.Mutex
	connections map[net.Conn]struct{}
	closing     bool
	workers     sync.WaitGroup
}

// The private directory prevents another local user from connecting even during
// the interval between socket creation and chmod. Never replace ordinary files.
func listenControl(m *Management, path string) (*controlServer, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("CLI socket 目录必须为非共享私有目录（0700）: %s", dir)
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("CLI socket 路径已被非 socket 文件占用")
		}
		conn, dialErr := net.DialTimeout("unix", path, time.Second)
		if dialErr == nil {
			conn.Close()
			return nil, fmt.Errorf("CLI socket 已被运行中的进程占用")
		}
		// Only connection refused proves a leftover socket, not timeout/permission errors.
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return nil, fmt.Errorf("无法确认 CLI socket 已失效: %w", dialErr)
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		listener.Close()
		return nil, err
	}
	return &controlServer{listener: listener, management: m, connections: make(map[net.Conn]struct{})}, nil
}

func (s *controlServer) Serve() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return err
		}
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			conn.Close()
			return net.ErrClosed
		}
		s.connections[conn] = struct{}{}
		s.workers.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.workers.Done()
			defer func() { conn.Close(); s.mu.Lock(); delete(s.connections, conn); s.mu.Unlock() }()
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			data, err := bufio.NewReaderSize(conn, maxControlRequest).ReadSlice('\n')
			if err != nil {
				return
			}
			var request controlRequest
			var response controlResponse
			if strictDecode(data, &request) != nil {
				response.Error = &ManagementError{Status: 400, Code: "invalid_request", Message: "CLI 请求格式无效"}
			} else {
				result, problem := s.management.control(request)
				response.Error = problem
				if problem == nil {
					response.Result = encodeJSON(result)
				}
			}
			_ = json.NewEncoder(conn).Encode(response)
		}()
	}
}

func (s *controlServer) Close() error {
	s.mu.Lock()
	s.closing = true
	err := s.listener.Close()
	for conn := range s.connections {
		_ = conn.Close()
	}
	s.mu.Unlock()
	s.workers.Wait()
	return err
}

func (m *Management) control(req controlRequest) (any, *ManagementError) {
	invalid := func() (any, *ManagementError) {
		return nil, &ManagementError{Status: 400, Code: "invalid_request", Message: "CLI 命令参数无效"}
	}
	switch req.Command {
	case "status", "backends-list":
		if len(req.Payload) != 0 && string(req.Payload) != "null" {
			return invalid()
		}
		if req.Command == "status" {
			return m.status(), nil
		}
		return m.backendList(), nil
	case "operation":
		var id string
		if strictDecode(req.Payload, &id) != nil || !validOperationID(id) {
			return invalid()
		}
		state := m.status()
		if op := operationByID(&state.ManagedState, id); op != nil {
			return op, nil
		}
		return nil, &ManagementError{Status: 404, Code: "not_found", Message: "操作不存在"}
	case "gateway-addresses":
		var urls []string
		if strictDecode(req.Payload, &urls) != nil || len(urls) > 256 {
			return invalid()
		}
		return m.registerGatewayAddresses(urls)
	case "test-acquire", "test-release", "test-status":
		var body TestOccupancyRequest
		if strictDecode(req.Payload, &body) != nil {
			return invalid()
		}
		return m.testControl(req.Command, body)
	case "backend-create", "backend-edit":
		var body BackendWriteRequest
		if len(req.Payload) > 64<<10 || strictDecode(req.Payload, &body) != nil {
			return invalid()
		}
		return m.writeBackend(req.Command == "backend-create", body)
	case "warmup", "switch", "maintenance", "restore", "confirm-drain":
		var body OperationRequest
		if len(req.Payload) > 8192 || strictDecode(req.Payload, &body) != nil {
			return invalid()
		}
		return m.submit(req.Command, body)
	default:
		return nil, &ManagementError{Status: 400, Code: "unknown_command", Message: "不支持的 CLI 命令"}
	}
}

func callControl(path, command string, body, result any) error {
	req := controlRequest{Command: command}
	if body != nil {
		req.Payload = encodeJSON(body)
	}
	data := append(encodeJSON(req), '\n')
	if len(data) > maxControlRequest {
		return fmt.Errorf("CLI 请求超过大小限制")
	}
	conn, err := net.DialTimeout("unix", path, 5*time.Second)
	if err != nil {
		return fmt.Errorf("无法连接运行中的网关 CLI socket %s: %w", path, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if _, err = conn.Write(data); err != nil {
		return fmt.Errorf("CLI 请求发送失败: %w", err)
	}
	var response controlResponse
	if err = json.NewDecoder(io.LimitReader(conn, 4<<20)).Decode(&response); err != nil {
		return fmt.Errorf("CLI 响应读取失败: %w", err)
	}
	if response.Error != nil {
		e := response.Error
		return fmt.Errorf("%s (%s; operation_id=%s)", e.Message, e.Code, e.OperationID)
	}
	if decodeJSON(response.Result, result) != nil {
		return fmt.Errorf("CLI 响应无效")
	}
	return nil
}

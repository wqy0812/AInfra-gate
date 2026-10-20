package aigate

import (
	"bufio"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func queryReplayProgress(path string, id replayProgressIdentity) (replayProgressResponse, error) {
	var response replayProgressResponse
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		return response, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Second))
	if _, err = conn.Write(append(encodeJSON(replayProgressQuery{id, "status"}), '\n')); err != nil {
		return response, err
	}
	err = json.NewDecoder(conn).Decode(&response)
	return response, err
}

func TestReplayProgressProtocol(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "progress-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "progress.sock")
	id := replayProgressIdentity{"run", "boot", strings.Repeat("a", 64)}
	closeServer, err := startReplayProgress(path, id, func() replayProgressView {
		return replayProgressView{RunID: "run", State: "running", Progress: replayProgress{Planned: 4, Sent: 3, Completed: 1, Failed: 1, Inflight: 1, Unsent: 1}}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeServer()
	for name, mode := range map[string]os.FileMode{dir: 0700, path: 0600} {
		st, err := os.Stat(name)
		if err != nil || st.Mode().Perm() != mode {
			t.Fatalf("permissions %s: %v", name, err)
		}
	}
	response, err := queryReplayProgress(path, id)
	if err != nil || response.replayProgressIdentity != id || response.SampledAt.IsZero() || response.RunView.Progress.Unsent != 1 {
		t.Fatalf("response %+v, %v", response, err)
	}
	for _, bad := range []string{
		`{"op":"stop","run_id":"run","boot_id":"boot","task_sha256":"` + id.TaskSHA256 + `"}` + "\n",
		`{"op":"status","run_id":"other","boot_id":"boot","task_sha256":"` + id.TaskSHA256 + `"}` + "\n",
		"{broken}\n", strings.Repeat("x", 2049) + "\n",
	} {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		conn.Write([]byte(bad))
		line, _ := bufio.NewReader(conn).ReadString('\n')
		conn.Close()
		if !strings.Contains(line, "invalid_progress_query") {
			t.Fatalf("invalid query accepted: %q", line)
		}
	}
	// A silent client cannot monopolize a handler indefinitely.
	conn, _ := net.Dial("unix", path)
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	begin := time.Now()
	_, err = bufio.NewReader(conn).ReadString('\n')
	if time.Since(begin) > 1500*time.Millisecond {
		t.Fatalf("handler failed to time out: %v", err)
	}
	if _, err := queryReplayProgress(path, id); err != nil {
		t.Fatal(err)
	}
}

func TestReplayCheckpointCadenceAndStop(t *testing.T) {
	input, req := syntheticReplay(t, 1, 0, `{"stream":true,"messages":[]}`)
	release := make(chan struct{})
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) { <-release; w.Write([]byte("data: [DONE]\n\n")) })
	f := newReplayFixture(t, input, map[string]string{"r1": target.URL})
	t.Cleanup(func() { close(release) })
	ticks := make(chan time.Time, 2)
	f.svc.checkpointTicker = func(d time.Duration) (<-chan time.Time, func()) {
		if d != 30*time.Second {
			t.Errorf("period=%s", d)
		}
		return ticks, func() {}
	}
	if _, status, reason := f.svc.start(req); status != 200 {
		t.Fatal(status, reason)
	}
	f.svc.mu.Lock()
	run := f.svc.runs[req.RunID]
	f.svc.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for run.view().Progress.Sent != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	path := filepath.Join(run.dir, "state.json")
	before, _ := os.ReadFile(path)
	var checkpoint replayRunView
	json.Unmarshal(before, &checkpoint)
	if checkpoint.Progress.Sent != 0 {
		t.Fatalf("unexpected checkpoint %s", before)
	}
	ticks <- time.Now()
	for time.Now().Before(deadline) {
		raw, _ := os.ReadFile(path)
		json.Unmarshal(raw, &checkpoint)
		if checkpoint.Progress.Sent == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if checkpoint.Progress.Sent != 1 || checkpoint.SampledAt == nil {
		t.Fatal(checkpoint)
	}
	if _, status, reason := f.svc.stop(req.RunID); status != 200 {
		t.Fatal(status, reason)
	}
	stopped, _ := os.ReadFile(path)
	json.Unmarshal(stopped, &checkpoint)
	if checkpoint.State != "stopping" || checkpoint.Progress.Inflight != 1 {
		t.Fatal(checkpoint)
	}
	f.svc.stop(req.RunID)
	again, _ := os.ReadFile(path)
	if string(stopped) != string(again) {
		t.Fatal("repeated stop rewrote checkpoint")
	}
}

func TestReplayTaskSocketFailureNeverDispatches(t *testing.T) {
	input, req := syntheticReplay(t, 1, 0, `{"stream":true,"messages":[]}`)
	var calls atomic.Int64
	target := replayTarget(t, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	root := t.TempDir()
	t.Setenv("AIGATE_REPLAY_INPUT_ROOT", input)
	t.Setenv("AIGATE_REPLAY_MARK_KEY", string(testMarkKey))
	// A regular file at the endpoint must never be unlinked to make room.
	path := filepath.Join(root, "occupied")
	os.WriteFile(path, []byte("keep"), 0600)
	t.Setenv(replayProgressSocketEnv, path)
	task := replayTaskInput{BootID: "boot", PlanID: "plan", Targets: map[string]string{"r1": target.URL}, Digests: map[string]string{"g1/synthetic/" + req.Recording.Segments[0]: runtimeDigest("input")}, Request: req}
	recordingAtomicJSON(root, "task.json", task)
	if err := RunReplayTask([]string{root}); err == nil {
		t.Fatal("socket failure accepted")
	}
	raw, _ := os.ReadFile(path)
	if string(raw) != "keep" || calls.Load() != 0 {
		t.Fatal("socket collision destroyed data or dispatched")
	}
}

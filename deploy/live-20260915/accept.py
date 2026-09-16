"""Bounded real-protocol acceptance plus live-gauge observation, no route changes."""
import json
import re
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from profile_checks import values, metric, policy_of

FAMILIES = ['streams_waiting_first_output', 'stream_first_output_wait_max_seconds', 'stream_idle_requests', 'stream_idle_max_seconds', 'inflight_requests_by_stage', 'inflight_oldest_age_seconds', 'inflight_oldest_stage', 'stream_observation_unknown', 'live_backend_groups']


def save(root, name, value):
    p = Path(root) / name
    p.write_text(json.dumps(value, ensure_ascii=False, indent=2))
    p.chmod(0o600)


def verify_live(text):
    for name in FAMILIES:
        assert '# TYPE aigate_' + name + ' gauge' in text, name
    v = values(text)
    groups = metric(v, 'aigate_live_backend_groups')
    stages = {k: n for k, n in v.items() if k.startswith('aigate_inflight_requests_by_stage{')}
    expected = {'routing_discovery', 'preparing_upstream', 'waiting_upstream_headers',
                'waiting_first_output', 'streaming', 'reading_response', 'writing_client', 'finishing_stream'}
    if policy_of(v) == 'legacy':
        expected.add('receiving_request')
    by_backend = {}
    for key, count in stages.items():
        backend = re.search(r'(?:\{|,)backend="((?:\\.|[^"\\])*)"', key).group(1)
        stage = re.search(r'(?:\{|,)stage="([^"\\]*)"', key).group(1)
        observed = by_backend.setdefault(backend, set())
        assert stage not in observed and count >= 0, 'Duplicate or negative stage'
        observed.add(stage)
    assert len(by_backend) == groups and groups >= 1
    assert all(stages == expected for stages in by_backend.values()), 'Incomplete stage inventory'
    assert sum(stages.values()) == metric(v, 'aigate_requests_inflight')
    return {k: n for k, n in v.items() if any(k.startswith('aigate_' + f) for f in FAMILIES) and n > 0}


def accept(root, phase, binary, base, environ, status, cli, metrics):
    root = Path(root)
    initial = status()
    with (root / (phase + '-http.json')).open('w') as out:
        proc = subprocess.Popen([str(binary), '--base', base], stdout=out, env=environ)
        snapshots = []
        deadline = time.monotonic() + 340
        try:
            while proc.poll() is None:
                assert time.monotonic() < deadline, 'HTTP acceptance exceeded bounded deadline'
                m = metrics()
                live = verify_live(m)
                if any(k.startswith('aigate_inflight_oldest_age_seconds{') and v > 0 for k, v in live.items()):
                    snapshots.append({'at': time.time(), 'values': live})
                time.sleep(.1)
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
        assert proc.returncode == 0, 'HTTP checks failed; see report'
    report = json.loads((root / (phase + '-http.json')).read_text())
    assert report['passed'] == report['total']
    print(json.dumps({'phase': phase, 'http_passed': report['passed'], 'http_total': report['total'], 'live_snapshots': len(snapshots)}), flush=True)
    operation = cli('warmup', initial['active_backend'], '--operation-id', 'live-20260915-' + phase)
    save(root, phase + '-warmup.json', operation)
    assert operation['outcome'] == 'completed', 'Protocol warmup failed'
    m = metrics()
    verify_live(m)
    (root / (phase + '-metrics.txt')).write_text(m)
    current = status()
    assert current['active_backend'] == initial['active_backend'] and current['route_version'] == initial['route_version']
    save(root, phase + '-status.json', current)
    save(root, phase + '-live.json', {'observed_inflight': bool(snapshots), 'snapshots': snapshots})
    assert snapshots, 'No live request observed during real HTTP checks'
    save(root, phase + '-acceptance.json', {'passed': True, 'http': report['total'], 'protocol_steps': len(operation['report']['steps']), 'observed_live_requests': True, 'active_backend': current['active_backend'], 'route_version': current['route_version']})
    print(json.dumps({'phase': phase, 'passed': True, 'protocol_steps': len(operation['report']['steps'])}), flush=True)


def check_preserved_metrics(before, after):
    a, b = values(before), values(after)
    assert policy_of(a) == policy_of(b), 'Use checkpoint verification for a scope migration'
    for marker in ('aigate_profile_counter_start_time_seconds', 'aigate_error_metrics_start_time_seconds'):
        assert metric(a, marker) == metric(b, marker), marker
    prefixes = ('aigate_requests_started_total', 'aigate_requests_ended_total', 'aigate_prompt_tokens_total', 'aigate_completion_tokens_total', 'aigate_generation_requests_ended_total', 'aigate_upstream_errors_total')
    keys = [k for k in a if k.startswith(prefixes)]
    assert all(b.get(k, -1) >= a[k] for k in keys)
    assert metric(b, 'aigate_profile_dropped_events_total') == metric(b, 'aigate_profile_write_errors_total') == 0
    return len(keys)

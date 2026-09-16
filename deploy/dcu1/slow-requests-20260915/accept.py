"""DCU production acceptance, including one controlled slow upload and saved-input replay."""
import hashlib
import http.client
import json
import subprocess
import time
from pathlib import Path
from release import BASE, ROOT, CONFIG, SLOW, check_baseline, cli, load, metrics, save, state, route, values, metric, verify_profile


def quick():
    with (ROOT / 'http-verification.json').open('w') as out:
        subprocess.run([str(ROOT / 'verify'), '--base', 'http://127.0.0.1:10015'],
                       stdout=out, stderr=subprocess.PIPE, check=True, timeout=330)
    report = load('http-verification.json')
    assert report['passed'] == report['total']
    baseline = load('baseline.json')
    operation = cli('warmup', baseline['route']['active_backend'], '--operation-id', 'slow-dcu-20260915-accept')
    save('warmup.json', operation)
    assert operation['outcome'] == 'completed' and operation['report']['passed']
    check_baseline(baseline)
    v = values(metrics())
    assert 'aigate_nonstream_requests_total{request_scope="nonstreaming"}' in v
    assert metric(v, 'aigate_profile_counter_start_time_seconds') == load('switched.json')['counter_start']
    verify_profile(load('profile-stopped.json'), metrics(), baseline['config']['profile']['directory'])
    result = {'passed': True, 'http_checks': report['total'], 'protocol_steps': len(operation['report']['steps'])}
    save('quick-acceptance.json', result)
    print(json.dumps(result), flush=True)


def slow_capture():
    assert route(state()) == load('baseline.json')['route']
    body = (' {\n "model":"slow-capture-replay-alias", "stream":true, "temperature":0, "max_tokens":16,\n'
            ' "messages":[{"role":"user","content":"Reply only OK. Synthetic DCU aigate slow capture acceptance 20260915."}]\n}\n ').encode()
    (ROOT / 'acceptance-input.json').write_bytes(body)
    conn = http.client.HTTPConnection('127.0.0.1', 10015, timeout=150)
    started = time.monotonic()
    conn.putrequest('POST', '/v1/chat/completions')
    conn.putheader('Content-Type', 'application/json')
    conn.putheader('Content-Length', str(len(body)))
    conn.endheaders()
    conn.send(body[:1])
    save('slow-progress.json', {'phase': 'waiting_for_301_seconds', 'started_at': time.time(),
                               'method': 'controlled slow upload; not slow model inference'})
    print('Synthetic DCU request started; holding remaining input for 301 seconds', flush=True)
    while time.monotonic() - started < 301:
        time.sleep(min(10, 301 - (time.monotonic() - started)))
    conn.send(body[1:])
    response = conn.getresponse()
    output = response.read()
    request_id = response.getheader('X-Request-ID')
    conn.close()
    assert response.status == 200 and b'[DONE]' in output
    assert request_id and len(request_id) == 32 and all(c in '0123456789abcdef' for c in request_id)
    path = Path(SLOW['directory']) / ('request-' + request_id)
    deadline = time.monotonic() + 15
    while not (path / 'meta.json').exists():
        if time.monotonic() > deadline:
            raise RuntimeError('Slow capture was not published')
        time.sleep(.1)
    meta = json.loads((path / 'meta.json').read_text())
    assert (path / 'request.json').read_bytes() == body
    assert meta['input_bytes'] == len(body) and meta['input_sha256'] == hashlib.sha256(body).hexdigest()
    assert meta['duration_seconds'] > 300 and meta['request_id'] == request_id
    assert meta['generation_result'] == 'completed' and meta['error_class'] is None and not meta['client_cancelled']
    (ROOT / 'slow-response.sse').write_bytes(output)
    save('saved-meta.json', meta)
    (ROOT / 'saved-request.json').write_bytes(body)
    replay_saved()


def replay_saved():
    meta = load('saved-meta.json')
    request_id = meta['request_id']
    path = Path(SLOW['directory']) / ('request-' + request_id)
    body = (path / 'request.json').read_bytes()
    assert hashlib.sha256(body).hexdigest() == meta['input_sha256']
    permissions = {str(p): oct(p.stat().st_mode & 0o777) for p in [Path(SLOW['directory']), path, path / 'request.json', path / 'meta.json']}
    assert list(permissions.values()) == ['0o700', '0o700', '0o600', '0o600']
    subprocess.run(['curl', '--fail', '--no-buffer', '--max-time', '150', '-sS',
                    '-H', 'Content-Type: application/json', '--data-binary', '@' + str(path / 'request.json'),
                    '-D', str(ROOT / 'replay-headers.txt'), '-o', str(ROOT / 'replay-response.sse'),
                    'http://127.0.0.1:10015/v1/chat/completions'], check=True, timeout=160)
    headers = (ROOT / 'replay-headers.txt').read_text()
    replay_id = next(line.split(':', 1)[1].strip() for line in headers.splitlines() if line.lower().startswith('x-request-id:'))
    assert replay_id != request_id and '[DONE]' in (ROOT / 'replay-response.sse').read_text()
    assert not (Path(SLOW['directory']) / ('request-' + replay_id)).exists()
    check_baseline(load('baseline.json'))
    result = {'passed': True, 'at': time.time(), 'request_id': request_id, 'duration_seconds': meta['duration_seconds'],
              'method': 'controlled slow upload included in total gateway duration; not slow model inference',
              'raw_input_exact': True, 'input_sha256': meta['input_sha256'], 'input_bytes': len(body),
              'generation_result': meta['generation_result'], 'replay_request_id': replay_id, 'replay_completed': True,
              'short_replay_not_saved': True, 'permissions': permissions, 'record_path': str(path), 'route': route(state())}
    save('slow-acceptance.json', result)
    (ROOT / 'after-metrics.txt').write_text(metrics())
    save('after-state.json', state())
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    quick()
    slow_capture()

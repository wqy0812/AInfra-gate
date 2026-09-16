"""Run on dcu1 through SSH MCP only. Upgrade aigate, retaining route and inference services."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from profile_checks import values, metric, policy_of, stopped_snapshot, verify_profile

BASE = Path('/data4/aigate')
ROOT = BASE / 'evidence/slow-requests-20260915'
RELEASE = BASE / 'releases/slow-requests-20260915'
CONFIG = BASE / 'config.json'
CURRENT = BASE / 'current'
SLOW = dict(enabled=True, threshold_seconds=300, directory=str(BASE / 'slow-requests'),
            retention_hours=72, max_disk_bytes=10737418240, max_queue_bytes=67108864)


def run(*args, timeout=45, env=None):
    return subprocess.check_output(args, stderr=subprocess.PIPE, timeout=timeout, env=env)


def save(name, data):
    p = ROOT / name
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')
    p.chmod(0o600)


def load(name):
    return json.loads((ROOT / name).read_text())


def digest(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def pid():
    return int(run('systemctl', 'show', 'aigate.service', '--property=MainPID', '--value').strip())


def service_env():
    return dict(item.decode().split('=', 1) for item in Path('/proc/%d/environ' % pid()).read_bytes().split(b'\0') if b'=' in item)


def cli(*args):
    return json.loads(run(str(CURRENT / 'aigate'), *args, '--config', str(CONFIG), '--json',
                          timeout=930 if args[0] == 'warmup' else 45, env=service_env()))


def state():
    return cli('status')


def route(s):
    return dict({k: s.get(k) for k in ['active_backend', 'route_version', 'rollback_candidate', 'rollback_armed']},
                flags={n: {k: b.get(k) for k in ['maintenance', 'drain_unknown']} for n, b in s['backends'].items()})


def metrics():
    c = json.loads(CONFIG.read_text())['profile']
    req = urllib.request.Request('http://' + c['listen'] + '/metrics',
                                 headers={'Authorization': 'Bearer ' + service_env()[c['api_key_env']]})
    return urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=10).read().decode()


def protected():
    ids = run('docker', 'ps', '-q').decode().split()
    objects = json.loads(run('docker', 'inspect', *ids)) if ids else []
    return {c['Name']: {'id': c['Id'], 'started_at': c['State']['StartedAt'], 'restart_count': c['RestartCount'],
                        'pid': c['State']['Pid'], 'running': c['State']['Running']} for c in objects}


def protected_files(c):
    paths = [BASE / 'admin.env', Path(c['profile']['directory']) / 'hmac.key',
             Path('/etc/systemd/system/aigate.service')]
    paths += sorted(Path('/etc/systemd/system/aigate.service.d').glob('*.conf'))
    return {str(p): digest(p) for p in paths}


def check_baseline(b):
    assert route(state()) == b['route'], 'Routing or maintenance changed since baseline'
    assert json.loads(Path(b['config']['management']['state_file']).read_text())['registry'] == b['registry']
    assert protected() == b['protected'], 'Inference/container state changed since baseline'
    assert protected_files(b['config']) == b['protected_files'], 'Credentials, HMAC or unit changed'


def atomic_bytes(path, data, mode=0o600):
    tmp = path.with_name(path.name + '.slow-request-tmp')
    with tmp.open('xb') as f:
        os.chmod(tmp, mode)
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def point_current(target):
    tmp = BASE / 'current-slow-request-tmp'
    tmp.symlink_to(target)
    tmp.replace(CURRENT)


def wait_ready(timeout=60):
    deadline = time.monotonic() + timeout
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    while time.monotonic() < deadline:
        try:
            if opener.open('http://127.0.0.1:10015/health', timeout=2).status == 200:
                return
        except Exception:
            pass
        time.sleep(.5)
    raise RuntimeError('Gateway failed to become healthy')


def prepare():
    assert not (ROOT / 'baseline.json').exists()
    assert CURRENT.is_symlink() and not CONFIG.is_symlink()
    c = json.loads(CONFIG.read_text())
    assert 'slow_requests' not in c
    s = state()
    assert not s.get('busy') and all(not b.get('drain_unknown') for b in s['backends'].values())
    expected = load('source-manifest.json')['binary_sha256']
    assert digest(RELEASE / 'aigate') == expected
    assert digest(ROOT / 'verify') == load('source-manifest.json')['verify_sha256']
    registry = json.loads(Path(c['management']['state_file']).read_text())['registry']
    save('baseline.json', {'pid': pid(), 'previous_release': str(CURRENT.resolve()),
                          'previous_binary_sha256': digest('/proc/%d/exe' % pid()), 'config': c,
                          'config_sha256': digest(CONFIG), 'config_mode': CONFIG.stat().st_mode & 0o777,
                          'route': route(s), 'state': s, 'registry': registry,
                          'protected': protected(), 'protected_files': protected_files(c)})
    (ROOT / 'config-before.json').write_bytes(CONFIG.read_bytes())
    before_metrics = metrics()
    policy_of(values(before_metrics))
    (ROOT / 'before-metrics.txt').write_text(before_metrics)
    c['slow_requests'] = SLOW
    save('config-new.json', c)
    run(str(RELEASE / 'aigate'), 'check', '--config', str(ROOT / 'config-new.json'), env=service_env())
    print(json.dumps({'prepared': True, 'route': route(s), 'counts': s['counts'], 'binary_sha256': expected}), flush=True)


def wait_idle(b, timeout=1800):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        s = state()
        assert route(s) == b['route'], 'Routing changed while waiting'
        assert not s.get('busy') and all(not x.get('drain_unknown') for x in s['backends'].values())
        live = values(metrics())
        inflight = metric(live, 'aigate_requests_inflight')
        counts = {n: {k: v.get(k, 0) for k in ['business', 'streams', 'probes']} for n, v in s['counts'].items()}
        summary = {'counts': counts, 'inflight': inflight}
        save('waiting.json', dict(summary, at=time.time()))
        if summary != last:
            print(json.dumps({'waiting_for_idle': summary}), flush=True)
            last = summary
        if inflight == 0 and all(v == 0 for c in counts.values() for v in c.values()):
            save('pre-switch-state.json', s)
            return
        time.sleep(2)
    raise RuntimeError('No idle window within 30 minutes; no production change made')


def rollback():
    b = load('baseline.json')
    assert CURRENT.resolve() in [RELEASE, Path(b['previous_release'])]
    assert json.loads(CONFIG.read_text()) in [load('config-new.json'), b['config']], 'Concurrent config change'
    run('systemctl', 'stop', 'aigate.service', timeout=3710)
    stopped = json.loads(Path(b['config']['management']['state_file']).read_text())
    assert stopped['clean_shutdown'] and route(stopped) == b['route']
    atomic_bytes(CONFIG, (ROOT / 'config-before.json').read_bytes(), b['config_mode'])
    point_current(b['previous_release'])
    run('systemctl', 'start', 'aigate.service')
    wait_ready()
    assert route(state()) == b['route']
    save('rollback.json', {'restored': True, 'at': time.time(), 'pid': pid(), 'route': route(state())})
    print('Previous DCU gateway binary/config restored; current state retained', flush=True)


def switch(graceful=False):
    b = load('baseline.json')
    assert CURRENT.resolve() == Path(b['previous_release']) and pid() == b['pid']
    assert digest(CONFIG) == b['config_sha256']
    check_baseline(b)
    if graceful:
        s = state()
        assert not s.get('busy') and all(not x.get('drain_unknown') for x in s['backends'].values())
        save('pre-switch-state.json', s)
        print(json.dumps({'graceful_drain_of_active_requests': s['counts']}), flush=True)
    else:
        wait_idle(b)
    check_baseline(b)
    assert digest(CONFIG) == b['config_sha256']
    before_metrics = metrics()
    policy_of(values(before_metrics))
    stopped = False
    try:
        print('Requesting graceful DCU gateway stop; waiting for accepted streams to finish', flush=True)
        stopped = True  # A timed-out stop may already have stopped the service.
        run('systemctl', 'stop', 'aigate.service', timeout=3710)
        final_state = json.loads(Path(b['config']['management']['state_file']).read_text())
        assert final_state['clean_shutdown'] and route(final_state) == b['route'] and final_state['registry'] == b['registry']
        save('clean-stopped-state.json', final_state)
        profile = Path(b['config']['profile']['directory'])
        snapshot = stopped_snapshot(profile, before_metrics)
        save('profile-stopped.json', snapshot)
        history = {str(p): digest(p) for p in sorted(profile.glob('events-*.jsonl'))}
        save('preserved-events.json', history)
        atomic_bytes(CONFIG, (ROOT / 'config-new.json').read_bytes(), b['config_mode'])
        point_current(RELEASE)
        run('systemctl', 'start', 'aigate.service')
        wait_ready()
        assert digest('/proc/%d/exe' % pid()) == load('source-manifest.json')['binary_sha256']
        assert json.loads(CONFIG.read_text()) == load('config-new.json')
        check_baseline(b)
        assert all(Path(p).exists() and digest(p) == sha for p, sha in history.items())
        v = values(metrics())
        assert 'aigate_nonstream_requests_total{request_scope="nonstreaming"}' in v
        checked = verify_profile(snapshot, metrics(), profile)
        assert Path(SLOW['directory']).stat().st_mode & 0o777 == 0o700
        result = {'passed': True, 'at': time.time(), 'pid': pid(), 'binary_sha256': digest('/proc/%d/exe' % pid()),
                  'route': route(state()), 'protected_containers_preserved': len(b['protected']),
                  **checked, 'historical_checkpoints_preserved': True,
                  'history_files_preserved': len(history),
                  'credentials_hmac_unit_preserved': True, 'registry_preserved': True}
        save('switched.json', result)
        print(json.dumps(result), flush=True)
    except BaseException:
        if stopped:
            rollback()
        raise


if __name__ == '__main__':
    {'prepare': prepare, 'switch': switch, 'switch-graceful': lambda: switch(graceful=True), 'rollback': rollback}[sys.argv[1]]()

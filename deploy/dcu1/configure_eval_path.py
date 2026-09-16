"""Run on dcu1 via SSH MCP. Align the existing Gateway/Router for evaluation."""
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request

ROOT = Path('/data4/aigate/backups/eval-path-20260915')
CONFIG = Path('/data4/aigate/config.json')
ROUTER = Path('/data2/sglang-router-native/docker-run.json')
DROPIN = Path('/etc/systemd/system/aigate.service.d/90-eval-timeout.conf')
NAME = 'sglang-router-internal'


def run(*args, **kwargs):
    return subprocess.check_output(args, stderr=subprocess.STDOUT, **kwargs).decode()


def status():
    return json.loads(run('/data4/aigate/current/aigate', 'status', '--config', str(CONFIG), '--json'))


def save(name, value):
    (ROOT / name).write_text(json.dumps(value, ensure_ascii=False, indent=2))
    (ROOT / name).chmod(0o600)


def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


assert not ROOT.exists(), 'Inspect existing release before rerunning'
ROOT.mkdir(mode=0o700)
baseline = status()
assert baseline['active_backend'] == 'dcu'
assert all(not any(v[k] for k in ('business', 'streams', 'probes')) for v in baseline['counts'].values())
config = json.loads(CONFIG.read_text())
command = json.loads(ROUTER.read_text())
assert command[:3] == ['docker', 'run', '-d'] and command[command.index('--name') + 1] == NAME
assert config['timeout_seconds'] == 1860
save('gateway-before.json', config)
save('router-command-before.json', command)
save('state-before.json', baseline)
old = json.loads(run('docker', 'inspect', NAME))[0]
save('router-before.json', old)
save('dropin-before.json', {'exists': DROPIN.exists(), 'text': DROPIN.read_text() if DROPIN.exists() else None})
ids = run('docker', 'ps', '-q').split()
protected = {x['Name']: [x['Id'], x['State']['StartedAt']] for x in json.loads(run('docker', 'inspect', *ids)) if x['Name'] != '/' + NAME}
save('protected-before.json', protected)

candidate = dict(config)
candidate['timeout_seconds'] = 3600
candidate['management'] = {**config['management'], 'drain_timeout_seconds': 3660}
save('gateway-candidate.json', candidate)
pid = run('systemctl', 'show', 'aigate', '--property=MainPID', '--value').strip()
env = {k.decode(): v.decode() for pair in Path('/proc/' + pid + '/environ').read_bytes().split(b'\0') if b'=' in pair for k, v in [pair.split(b'=', 1)]}
run('/data4/aigate/current/aigate', 'check', '--config', str(ROOT / 'gateway-candidate.json'), env=env)
new_command = command + ['--request-timeout-secs', '3600', '--disable-retries', '--retry-max-retries', '1', '--pool-max-idle-per-host', '0']
save('router-command-candidate.json', new_command)
assert all(not any(v[k] for k in ('business', 'streams', 'probes')) for v in status()['counts'].values())
backup_name = NAME + '-before-eval-20260915'
renamed = False
try:
    run('systemctl', 'stop', 'aigate')
    run('docker', 'stop', '--time', '20', NAME)
    run('docker', 'rename', NAME, backup_name)
    renamed = True
    run(*new_command)
    deadline = time.time() + 40
    while True:
        try:
            ids = [x['id'] for x in get('http://127.0.0.1:10016/v1/models')['data']]
            assert ids == ['DeepSeek-V4-Flash-FP8-Channel']
            break
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(1)
    CONFIG.write_text(json.dumps(candidate, ensure_ascii=False, indent=2) + '\n')
    ROUTER.write_text(json.dumps(new_command, indent=2) + '\n')
    DROPIN.parent.mkdir(exist_ok=True)
    DROPIN.write_text('[Service]\nExecStart=\nExecStart=/data4/aigate/current/aigate serve --config /data4/aigate/config.json --listen 0.0.0.0:10015 --shutdown-timeout 61m\nTimeoutStopSec=3700\n')
    run('systemctl', 'daemon-reload')
    run('systemctl', 'start', 'aigate')
    deadline = time.time() + 20
    while True:
        try:
            final = status()
            assert final['active_backend'] == 'dcu' and final['route_version'] == baseline['route_version']
            ids = [x['id'] for x in get('http://127.0.0.1:10015/v1/models')['data']]
            assert ids == ['DeepSeek-V4-Flash-FP8-Channel']
            break
        except Exception:
            if time.time() > deadline:
                raise
            time.sleep(1)
    ids = run('docker', 'ps', '-q').split()
    after = {x['Name']: [x['Id'], x['State']['StartedAt']] for x in json.loads(run('docker', 'inspect', *ids)) if x['Name'] != '/' + NAME}
    assert after == protected
    save('complete.json', {'gateway_timeout_seconds': 3600, 'router_timeout_seconds': 3600,
                          'router_retries_disabled': True, 'router_idle_pool': 0,
                          'active_backend': final['active_backend'], 'route_version': final['route_version'],
                          'router_rollback': backup_name, 'protected_containers_unchanged': True})
    print((ROOT / 'complete.json').read_text())
except BaseException:
    if renamed:
        subprocess.run(['docker', 'rm', '-f', NAME], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        run('docker', 'rename', backup_name, NAME)
    CONFIG.write_text(json.dumps(config, ensure_ascii=False, indent=2) + '\n')
    ROUTER.write_text(json.dumps(command, indent=2) + '\n')
    drop = json.loads((ROOT / 'dropin-before.json').read_text())
    if drop['exists']:
        DROPIN.write_text(drop['text'])
    else:
        DROPIN.unlink(missing_ok=True)
    run('docker', 'start', NAME)
    run('systemctl', 'daemon-reload')
    run('systemctl', 'start', 'aigate')
    raise

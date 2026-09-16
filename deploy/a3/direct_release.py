"""Execute on worker-131 via SSH MCP; uploads go directly to A3-gate.

The caller must announce and create ROOT and ROOT/rollback before prepare.
No transfer Pod, SSH subprocess, ConfigMap mutation, or route change is used.
"""
import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from profile_checks import CHECKPOINTS, stopped_snapshot, verify_profile, values, policy_of

NS, NAME = 'ai-gate', 'aigate'
STATE = Path('/var/lib/ai-gate/state')
ROOT = None
RELEASE = None
NODE_DIR = None


def run(*args, timeout=45):
    return subprocess.check_output(args, stderr=subprocess.PIPE, timeout=timeout)


def kub(*args, timeout=45):
    return run('kubectl', *args, timeout=timeout)


def obj(*args):
    return json.loads(kub(*args))


def save(name, value):
    path = ROOT / name
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    path.chmod(0o600)


def load(name):
    return json.loads((ROOT / name).read_text())


def sha(path):
    with Path(path).open('rb') as f:
        h = hashlib.sha256()
        for chunk in iter(lambda: f.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def deployment():
    return obj('get', 'deployment', NAME, '-n', NS, '-o', 'json')


def cli(*args):
    return json.loads(kub('exec', '-n', NS, 'deployment/' + NAME, '--', '/opt/aigate/aigate',
                         *args, '--config', '/etc/aigate/config.json', '--json',
                         timeout=930 if args[0] == 'warmup' else 45))


def route(s):
    return dict({k: s.get(k) for k in ('active_backend', 'route_version', 'rollback_candidate', 'rollback_armed')},
                flags={n: {k: b.get(k) for k in ('maintenance', 'drain_unknown')} for n, b in s['backends'].items()})


def idle(s):
    assert not s.get('busy'), 'Management operation is active'
    assert all(v.get(k, 0) == 0 for v in s['counts'].values() for k in ('business', 'streams', 'probes')), 'Requests are in flight'
    assert all(not b.get('drain_unknown') for b in s['backends'].values()), 'Drain state is unknown'


def protected():
    return {p['metadata']['name']: {'uid': p['metadata']['uid'], 'start': p['status'].get('startTime'),
            'containers': [{k: c.get(k) for k in ('name', 'containerID', 'restartCount')}
                           for c in p['status'].get('containerStatuses', [])]}
            for p in obj('get', 'pods', '-n', 'zwt', '-o', 'json')['items']}


def registry():
    return json.loads((STATE / 'management-state.json').read_text()).get('registry')


def metrics():
    return kub('exec', '-n', NS, 'deployment/' + NAME, '--', 'sh', '-c',
               'curl -fsS --max-time 5 -H "Authorization: Bearer $AIGATE_PROFILE_KEY" http://127.0.0.1:18082/metrics').decode()


def patch(d, changes):
    p = [{'op': 'test', 'path': '/metadata/resourceVersion', 'value': d['metadata']['resourceVersion']}] + changes
    save('patch.json', p)
    kub('patch', 'deployment', NAME, '-n', NS, '--type=json', '--patch-file', str(ROOT / 'patch.json'))


def data_manifest():
    result = {}
    for sub in ('request-profile', 'slow-requests'):
        for p in sorted((STATE / sub).rglob('*')):
            if p.is_file() and (p.name.startswith('events-') or sub == 'slow-requests' or p.name == 'hmac.key'):
                assert not p.is_symlink()
                result[str(p.relative_to(STATE))] = {'size': p.stat().st_size, 'sha256': sha(p)}
    return result


def check_data(saved):
    for rel, info in saved.items():
        p = STATE / rel
        assert p.is_file() and not p.is_symlink() and p.stat().st_size >= info['size'], ('missing/truncated data', rel)
        with p.open('rb') as f:
            h = hashlib.sha256()
            remaining = info['size']
            while remaining:
                block = f.read(min(1024 * 1024, remaining))
                assert block
                h.update(block)
                remaining -= len(block)
        assert h.hexdigest() == info['sha256'], ('data changed', rel)
    return len(saved)


def prepare():
    assert not (ROOT / 'baseline.json').exists(), 'Baseline already exists'
    assert run('hostname').decode().strip() == 'worker-131'
    assert ROOT.is_dir() and (ROOT / 'rollback').is_dir()
    manifest = load('source-manifest.json')
    assert sha(ROOT / 'aigate') == manifest['binary_sha256']
    assert sha(ROOT / 'verify') == manifest['verify_sha256']
    assert sha(ROOT / 'source.tar.gz') == manifest['source_archive_sha256']
    d, s = deployment(), cli('status')
    idle(s)
    assert d['spec']['replicas'] == 1 and d['spec']['strategy']['type'] == 'Recreate'
    spec = d['spec']['template']['spec']
    assert spec['nodeSelector'] == {'kubernetes.io/hostname': 'worker-131'}
    assert next(v['hostPath']['path'] for v in spec['volumes'] if v['name'] == 'state') == str(STATE)
    old_dir = Path(next(v['hostPath']['path'] for v in spec['volumes'] if v['name'] == 'binary'))
    assert old_dir.parent == NODE_DIR.parent and old_dir != NODE_DIR
    old_sha = sha(old_dir / 'aigate')
    assert kub('exec', '-n', NS, 'deployment/' + NAME, '--', 'sha256sum', '/proc/1/exe').decode().split()[0] == old_sha
    cm_name = next(v['configMap']['name'] for v in spec['volumes'] if v['name'] == 'config')
    cm = obj('get', 'configmap', cm_name, '-n', NS, '-o', 'json')
    save('baseline.json', {'deployment': d, 'route': route(s), 'state': s, 'registry': registry(),
                          'protected': protected(), 'configmap': cm, 'old_sha256': old_sha,
                          'hmac_sha256': sha(STATE / 'request-profile/hmac.key')})
    save('data-before.json', data_manifest())
    before_metrics = metrics()
    policy_of(values(before_metrics))  # Refuse an unknown schema before staging a release.
    (ROOT / 'before-metrics.txt').write_text(before_metrics)
    backup_binary = ROOT / 'rollback/aigate'
    assert not backup_binary.exists()
    shutil.copyfile(old_dir / 'aigate', backup_binary)
    backup_binary.chmod(0o755)
    assert sha(backup_binary) == old_sha
    template = copy.deepcopy(d['spec']['template'])
    for v in template['spec']['volumes']:
        if v['name'] == 'binary':
            v['hostPath']['path'] = str(ROOT / 'rollback')
    save('rollback-template.json', template)
    # NODE_DIR must have been announced by the caller. Never overwrite a release.
    NODE_DIR.mkdir(mode=0o755)
    NODE_DIR.chmod(0o755)
    shutil.copyfile(ROOT / 'aigate', NODE_DIR / 'aigate')
    (NODE_DIR / 'aigate').chmod(0o755)
    assert sha(NODE_DIR / 'aigate') == manifest['binary_sha256']
    save('staged.json', {'release': RELEASE, 'binary_sha256': manifest['binary_sha256'], 'path': str(NODE_DIR)})
    template = copy.deepcopy(d['spec']['template'])
    for v in template['spec']['volumes']:
        if v['name'] == 'binary':
            v['hostPath']['path'] = str(NODE_DIR)
    template['metadata'].setdefault('annotations', {}).update({'aigate.release': RELEASE, 'aigate.binary.sha256': manifest['binary_sha256']})
    save('new-template.json', template)
    print(json.dumps({'prepared': True, 'sha256': manifest['binary_sha256'], 'previous_sha256': old_sha,
                      'same_binary': old_sha == manifest['binary_sha256'], 'route': route(s)}), flush=True)


def stop(d):
    pods = obj('get', 'pods', '-n', NS, '-l', 'app=aigate', '-o', 'json')['items']
    patch(d, [{'op': 'replace', 'path': '/spec/replicas', 'value': 0}])
    print('Graceful stop requested', flush=True)
    for p in pods:
        kub('wait', '--for=delete', 'pod/' + p['metadata']['name'], '-n', NS, '--timeout=3700s', timeout=3720)


def verify_counters():
    return verify_profile(load('profile-stopped.json'), metrics(), STATE / 'request-profile')


def verify_runtime(expected_sha):
    b = load('baseline.json')
    s = cli('status')
    assert route(s) == b['route'] and registry() == b['registry']
    assert protected() == b['protected']
    actual = kub('exec', '-n', NS, 'deployment/' + NAME, '--', 'sha256sum', '/proc/1/exe').decode().split()[0]
    assert actual == expected_sha
    assert sha(STATE / 'request-profile/hmac.key') == b['hmac_sha256']
    name = b['configmap']['metadata']['name']
    assert obj('get', 'configmap', name, '-n', NS, '-o', 'json')['data'] == b['configmap']['data']
    assert json.loads(kub('exec', '-n', NS, 'deployment/' + NAME, '--', 'cat', '/etc/aigate/config.json')) == json.loads(b['configmap']['data']['config.json'])
    return dict({'binary_sha256': actual, 'route': route(s), 'protected_pods': len(b['protected']),
                 'preserved_data_files': check_data(load('data-stopped.json'))}, **verify_counters())


def rollback():
    b, d = load('baseline.json'), deployment()
    allowed = [b['deployment']['spec']['template'], load('new-template.json'), load('rollback-template.json')]
    assert d['spec']['template'] in allowed, 'Concurrent template change; refusing rollback'
    assert sha(ROOT / 'rollback/aigate') == b['old_sha256']
    if d['spec']['replicas']:
        # A failed startup may have no management socket; Kubernetes still handles graceful termination.
        stop(d)
    d = deployment()
    assert d['spec']['template'] in allowed and d['spec']['replicas'] == 0
    patch(d, [{'op': 'replace', 'path': '/spec/template', 'value': load('rollback-template.json')},
              {'op': 'replace', 'path': '/spec/replicas', 'value': 1},
              {'op': 'add', 'path': '/metadata/annotations/aigate.binary.sha256', 'value': b['old_sha256']}])
    kub('rollout', 'status', 'deployment/' + NAME, '-n', NS, '--timeout=180s', timeout=195)
    assert route(cli('status')) == b['route'] and registry() == b['registry'] and protected() == b['protected']
    result = {'restored': True, 'at': time.time(), 'path': str(ROOT / 'rollback'), 'route': b['route']}
    save('rollback-result.json', result)
    print(json.dumps(result), flush=True)


def switch():
    b, d = load('baseline.json'), deployment()
    assert d['spec'] == b['deployment']['spec'], 'Deployment changed since preparation'
    s = cli('status')
    idle(s)
    assert route(s) == b['route'] and registry() == b['registry'] and protected() == b['protected']
    save('pre-switch-state.json', s)
    before_metrics = metrics()
    policy_of(values(before_metrics))
    attempted = False
    try:
        attempted = True
        stop(d)
        stopped = json.loads((STATE / 'management-state.json').read_text())
        assert stopped['clean_shutdown'] and route(stopped) == b['route'] and stopped['registry'] == b['registry']
        save('clean-stopped-state.json', stopped)
        check_data(load('data-before.json'))
        save('data-stopped.json', data_manifest())
        save('profile-stopped.json', stopped_snapshot(STATE / 'request-profile', before_metrics))
        for name in CHECKPOINTS.values():
            path = STATE / 'request-profile' / name
            if path.is_file():
                shutil.copyfile(path, ROOT / ('stopped-' + name))
        d = deployment()
        assert d['spec']['template'] == b['deployment']['spec']['template'] and d['spec']['replicas'] == 0
        patch(d, [{'op': 'replace', 'path': '/spec/template', 'value': load('new-template.json')},
                  {'op': 'replace', 'path': '/spec/replicas', 'value': 1},
                  {'op': 'add', 'path': '/metadata/annotations/aigate.binary.sha256', 'value': load('staged.json')['binary_sha256']}])
        print('Direct release starting', flush=True)
        kub('rollout', 'status', 'deployment/' + NAME, '-n', NS, '--timeout=180s', timeout=195)
        result = verify_runtime(load('staged.json')['binary_sha256'])
        result.update(passed=True, at=time.time())
        save('switched.json', result)
        print(json.dumps(result), flush=True)
    except BaseException:
        if attempted:
            rollback()
        raise


def accept():
    assert deployment()['spec']['template'] == load('new-template.json')
    with (ROOT / 'http-verification.json').open('w') as out:
        subprocess.run([str(ROOT / 'verify'), '--base', 'http://127.0.0.1:9090'], stdout=out,
                       stderr=subprocess.PIPE, timeout=330, check=True)
    report = load('http-verification.json')
    assert report['passed'] == report['total']
    operation = cli('warmup', load('baseline.json')['route']['active_backend'], '--operation-id', RELEASE + '-accept')
    save('warmup.json', operation)
    assert operation['outcome'] == 'completed' and operation['report']['passed']
    result = verify_runtime(load('staged.json')['binary_sha256'])
    s = cli('status')
    idle(s)
    save('after-state.json', s)
    (ROOT / 'after-metrics.txt').write_text(metrics())
    pods = obj('get', 'pods', '-n', NS, '-l', 'app=aigate', '-o', 'json')['items']
    assert len(pods) == 1
    statuses = pods[0]['status']['containerStatuses']
    assert all(c['ready'] and c['restartCount'] == 0 for c in statuses)
    result.update(passed=True, at=time.time(), http_checks=report['total'], protocol_steps=len(operation['report']['steps']),
                  pod=pods[0]['metadata']['name'], uid=pods[0]['metadata']['uid'], restart_count=0)
    save('acceptance.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['prepare', 'switch', 'accept', 'rollback'])
    parser.add_argument('--root', required=True)
    args = parser.parse_args()
    ROOT = Path(args.root)
    assert ROOT.is_absolute() and ROOT.parent == Path('/var/lib/ai-gate/deployments') and not ROOT.is_symlink()
    RELEASE = ROOT.name
    assert re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,79}', RELEASE)
    NODE_DIR = Path('/var/lib/ai-gate/releases') / RELEASE
    os.umask(0o077)
    globals()[args.action]()

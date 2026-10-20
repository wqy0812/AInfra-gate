#!/usr/bin/env python3
"""Manage the deployed aigate containers without a service manager.

PPU's containerd 1.6 restart plugin requires restart.status=stopped as well
as restart.policy=no for a persistent maintenance stop. Do not replace this
with an unqualified nerdctl stop on that deployment.
"""
import argparse
import http.client
import json
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('environment', choices=['ppu', 'a3'])
parser.add_argument('action', choices=['status', 'start', 'stop'])
parser.add_argument('role', choices=['switch', 'proxy'])
args = parser.parse_args()
binary = '/usr/local/nerdctl/nerdctl.current' if args.environment == 'ppu' else '/usr/bin/nerdctl'
nerdctl = [binary, '--namespace', 'ai-gateway']
name = ('ppu-aigate-' + args.role if args.environment == 'ppu' else
        'a3-aigate' + ('' if args.role == 'switch' else '-' + args.role))
policy = 'always' if args.environment == 'ppu' else 'unless-stopped'


def command(*argv):
    return subprocess.check_output(argv, stderr=subprocess.PIPE).decode().strip()


def inspect():
    return json.loads(command(*nerdctl, 'inspect', name))[0]


def require_idle(obj):
    pid = obj['State']['Pid']
    root = Path('/proc') / str(pid)
    argv = root.joinpath('cmdline').read_bytes().decode().rstrip('\0').split('\0')
    env = dict(v.split('=', 1) for v in root.joinpath('environ').read_bytes().decode().split('\0') if '=' in v)
    flag = '--control-listen' if args.role == 'switch' else '--listen'
    host, port = argv[argv.index(flag) + 1].rsplit(':', 1)
    if host == '0.0.0.0':
        host = '127.0.0.1'
    path = {'switch': '/control/v3/status', 'proxy': '/runtime/v1/status'}[args.role]
    key = 'AIGATE_CONTROL_KEY' if args.role == 'switch' else None
    conn = http.client.HTTPConnection(host, int(port), timeout=10, source_address=('127.0.0.1', 0))
    try:
        conn.request('GET', path, headers={'Authorization': 'Bearer ' + env[key]} if key else {})
        response = conn.getresponse()
        if response.status != 200:
            raise RuntimeError('Cannot confirm service status; no stop performed')
        status = json.loads(response.read())
    finally:
        conn.close()
    if args.role == 'switch':
        if status['admission'] or status.get('candidate') or status.get('test_occupancy'):
            raise RuntimeError('Pause admission through the platform and finish control/test operations first')
        # Explicit null means recording is disabled; a missing field is unknown.
        recording = status['recording']
        if recording is not None and (recording.get('sessions') or recording.get('active_records')):
            raise RuntimeError('Active recording; no stop performed')
    counts = status['counts']
    if isinstance(counts, dict):
        counts = [counts]
    if any(sum(c.get(k, 0) for k in ['business', 'streams', 'tests', 'probes']) for c in counts):
        raise RuntimeError('Requests are still in flight; no stop performed')



obj = inspect()
if args.action == 'stop':
    if obj['State']['Status'] == 'running':
        require_idle(obj)
        if args.environment == 'ppu':
            command(*nerdctl, 'update', '--restart', 'no', name)
            command('ctr', '-n', 'ai-gateway', 'containers', 'label', obj['Id'],
                    'containerd.io/restart.status=stopped')
        command(*nerdctl, 'stop', '--time', '3720', name)
    elif args.environment == 'ppu':
        command('ctr', '-n', 'ai-gateway', 'containers', 'label', obj['Id'],
                'containerd.io/restart.policy=no', 'containerd.io/restart.status=stopped')
elif args.action == 'start':
    if obj['State']['Status'] != 'running':
        command(*nerdctl, 'start', name)
    command(*nerdctl, 'update', '--restart', policy, name)
obj = inspect()
print(json.dumps({'namespace': 'ai-gateway', 'container': name,
                  'status': obj['State']['Status'], 'pid': obj['State']['Pid'],
                  'restart_policy': obj['Config']['Labels'].get('containerd.io/restart.policy'),
                  'restart_status': obj['Config']['Labels'].get('containerd.io/restart.status')}))

"""Run through SSH MCP on dcu1; retain current route/state/data."""
import json
import subprocess
import sys
from pathlib import Path


def cmd(*args):
    return subprocess.check_output(args, stderr=subprocess.PIPE, timeout=45).decode()


def idle(s):
    assert not s.get('busy')
    assert all(v[k] == 0 for v in s['counts'].values() for k in ('business', 'streams', 'probes')), 'Requests still active; no change made'


if sys.argv[1] == 'dcu':
    r = Path('/data4/aigate')
    b = json.loads((r / 'evidence/live-20260915/baseline.json').read_text())
    assert (r / 'current').resolve() == r / 'releases/live-20260915'
    idle(json.loads(cmd(str(r / 'aigate-admin'), 'status', '--json')))
    cmd('systemctl', 'stop', 'aigate')
    assert json.loads(Path(b['config']['management']['state_file']).read_text())['clean_shutdown']
    link = r / 'current-live-rollback'
    link.symlink_to(b['previous_release'])
    link.replace(r / 'current')
    cmd('systemctl', 'start', 'aigate')

else:
    raise ValueError('Usage: rollback.py dcu')
print('Previous code restored/requested. Check startup, route and real requests; current state and profile data retained.')

"""Read-only final checks of the DCU service; write only this release's evidence."""
import collections
import json
import time
from pathlib import Path
import release as r

b = r.load('baseline.json')
a = r.load('slow-acceptance.json')
assert a['passed']
r.check_baseline(b)
assert r.pid() == r.load('switched.json')['pid']
actual = r.digest('/proc/%d/exe' % r.pid())
assert actual == r.load('source-manifest.json')['binary_sha256']
assert r.digest(r.ROOT / 'source.tar.gz') == r.load('source-manifest.json')['source_archive_sha256']
assert json.loads(r.CONFIG.read_text()) == r.load('config-new.json')
r.verify_profile(r.load('profile-stopped.json'), r.metrics(), b['config']['profile']['directory'])
history = r.load('preserved-events.json')
assert all(Path(p).exists() and r.digest(p) == sha for p, sha in history.items())
v = r.values(r.metrics())
assert r.metric(v, 'aigate_profile_write_errors_total') == 0
assert r.metric(v, 'aigate_profile_dropped_events_total') == 0
assert r.metric(v, 'aigate_profile_counter_start_time_seconds') == r.load('switched.json')['counter_start']
events = []
for line in r.run('journalctl', '_PID=%d' % r.pid(), '--no-pager', '-o', 'cat').decode().splitlines():
    try:
        item = json.loads(line)
    except ValueError:
        continue
    if item.get('event') == 'slow_request_capture':
        events.append(item)
assert any(e.get('request_id') == a['request_id'] and e.get('result') == 'saved' for e in events)
assert not any(e.get('result') in ['failed', 'cleanup_failed'] for e in events)
service = dict(line.split('=', 1) for line in r.run('systemctl', 'show', 'aigate.service',
               '--property=ActiveState,SubState,MainPID,NRestarts,ExecMainStartTimestamp').decode().splitlines())
assert service['ActiveState'] == 'active' and service['SubState'] == 'running'
s = r.state()
final = {'passed': True, 'at': time.time(), 'service': service, 'binary_sha256': actual, 'route': r.route(s),
         'counts_at_verification': s['counts'], 'slow_requests': r.SLOW,
         'capture_result_counts': dict(collections.Counter(e['result'] for e in events)),
         'synthetic_request_id': a['request_id'], 'profile_write_errors': 0, 'profile_dropped_events': 0,
         'protected_dcu1_containers_preserved': len(b['protected']), 'legacy_counter_preserved': True,
         'history_files_preserved': len(history), 'credentials_hmac_unit_preserved': True,
         'registry_preserved': True, 'scope': 'Only dcu1 aigate service changed by this task'}
r.save('final.json', final)
print(json.dumps(final, ensure_ascii=False))

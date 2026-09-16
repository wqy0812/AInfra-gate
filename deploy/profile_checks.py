"""Local checkpoint/metrics checks shared by SSH-MCP deployment scripts.

Copy this file beside the release script when uploading a release bundle.
Missing or ambiguous metrics are errors; they never mean zero traffic.
"""
import hashlib
import json
import math
from pathlib import Path


CHECKPOINTS = {
    'legacy': 'counters.json',
    'streaming': 'counters-streaming.json',
    'metric-v2': 'counters-metric-v2.json',
}


def values(text):
    result = {}
    for line in text.splitlines():
        if not line or line.startswith('#'):
            continue
        key, raw = line.rsplit(' ', 1)
        number = float(raw)
        if key in result or not math.isfinite(number):
            raise ValueError('Duplicate or non-finite metric: ' + key)
        result[key] = number
    return result


def public_key(key, policy):
    if policy == 'legacy':
        return key
    name = key.split('{', 1)[0]
    scope = 'streaming'
    if name == 'aigate_nonstream_requests_total':
        scope = 'nonstreaming'
    elif policy == 'metric-v2' and not (
            name.startswith(('aigate_first_increment_seconds_', 'aigate_stream_'))
            or name == 'aigate_streams_waiting_first_output'):
        scope = 'all'
    elif policy not in ('streaming', 'metric-v2'):
        raise ValueError('Unknown profile policy: ' + policy)
    suffix = 'request_scope="' + scope + '"}'
    return key[:-1] + ',' + suffix if '{' in key else key + '{' + suffix


def policy_of(metrics):
    marker = 'aigate_profile_counter_start_time_seconds'
    matches = [policy for policy in CHECKPOINTS if public_key(marker, policy) in metrics]
    if len(matches) != 1:
        raise ValueError('Missing or ambiguous profile counter lifecycle')
    policy = matches[0]
    if metrics[public_key(marker, policy)] <= 0:
        raise ValueError('Invalid profile counter lifecycle')
    return policy


def metric(metrics, name):
    policy = policy_of(metrics)
    value = metrics[public_key(name, policy)]
    if value < 0:
        raise ValueError('Negative metric: ' + name)
    return value


def stopped_snapshot(directory, before_text):
    """Read the active checkpoint only after the gateway has stopped cleanly."""
    directory = Path(directory)
    before = values(before_text)
    policy = policy_of(before)
    path = directory / CHECKPOINTS[policy]
    saved = json.loads(path.read_text())
    if (saved.get('version') != 1 or not isinstance(saved.get('values'), dict)
            or not isinstance(saved.get('types'), dict)
            or saved.get('started') != metric(before, 'aigate_profile_counter_start_time_seconds')):
        raise ValueError('Checkpoint does not match the stopped gateway lifecycle')
    hashes = {name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
              for name in CHECKPOINTS.values() if (directory / name).is_file()}
    return {'policy': policy, 'checkpoint': saved, 'checkpoint_sha256': hashes}


def verify_profile(snapshot, text, directory):
    current = values(text)
    policy = policy_of(current)
    previous = snapshot['policy']
    saved = snapshot['checkpoint']
    started = metric(current, 'aigate_profile_counter_start_time_seconds')
    # Other generations are rollback material, not the active checkpoint.
    for name, digest in snapshot['checkpoint_sha256'].items():
        if name != CHECKPOINTS[policy]:
            if hashlib.sha256((Path(directory) / name).read_bytes()).hexdigest() != digest:
                raise ValueError('Historical checkpoint changed: ' + name)
    checked = 0
    if policy == previous:
        if started != saved['started']:
            raise ValueError('Counter lifecycle changed during same-policy restart')
        for key, value in saved['values'].items():
            name = key.split('{', 1)[0]
            family = name.rsplit('_', 1)[0]
            cumulative = saved['types'].get(name) == 'counter' or (
                name.endswith(('_bucket', '_count', '_sum'))
                and saved['types'].get(family) == 'histogram')
            if cumulative:
                if current[public_key(key, policy)] < value:
                    raise ValueError('Counter or histogram decreased: ' + name)
                checked += 1
        if not checked:
            raise ValueError('No cumulative series verified')
    elif list(CHECKPOINTS).index(policy) > list(CHECKPOINTS).index(previous):
        if started <= saved['started']:
            raise ValueError('Scope migration reused an old counter lifecycle')
        # A pre-existing target checkpoint would resurrect an earlier generation.
        if CHECKPOINTS[policy] in snapshot['checkpoint_sha256']:
            raise ValueError('Scope migration must start without a stale target checkpoint')
    else:
        raise ValueError('Unexpected profile policy transition: ' + previous + ' -> ' + policy)
    return {'policy': policy, 'previous_policy': previous, 'counter_start': started,
            'restored_counter_series': checked, 'scope_migrated': policy != previous}

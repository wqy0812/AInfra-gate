#!/usr/bin/env python3
"""Explicit live-model admission gate. Standard library plus curl; never executes tools."""
import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import sys
import time
import urllib.request

FIXTURES = {
    'body.json': '8525ce2591de6c2a389396d6acaebaac7b3928f6a04adbb3d33e1839c712bb90',
    'body-agent-ds4.json': 'eb15927d1100c4bf341f177c22f5d966b660fdb7e22ee89eeb662d5ccd6cf1b7',
}


def strict_json_loads(raw):
    def reject_constant(value):
        raise ValueError('non_json_constant: ' + value)
    return json.loads(raw, parse_constant=reject_constant)


def validate_sse(raw, request):
    """Validate complete OpenAI SSE and assembled function calls, not answer wording."""
    events, lines = [], []
    # Only CR/LF end SSE lines. EOF must not manufacture an event delimiter;
    # Unicode line separators inside JSON strings are ordinary content.
    parts = re.split(r'\r\n|\r|\n', raw.decode('utf-8-sig'))
    for line in parts[:-1]:
        if not line:
            if lines:
                events.append('\n'.join(lines))
                lines = []
        else:
            field, _, value = line.partition(':')
            if field == 'data':
                lines.append(value[1:] if value.startswith(' ') else value)
    if lines or parts[-1].partition(':')[0] == 'data':
        raise ValueError('unterminated_data_event')
    done, finished, content, reasoning, calls, usage = False, None, '', '', {}, None
    for data in events:
        if done:
            raise ValueError('data_after_done')
        if data.strip() == '[DONE]':
            done = True
            continue
        event = strict_json_loads(data)
        if not isinstance(event, dict) or event.get('error') is not None:
            raise ValueError('stream_error_or_invalid_event')
        if event.get('usage'):
            usage = event['usage']
        for choice in event.get('choices', []):
            if type(choice.get('index')) is not int or choice['index'] != 0:
                raise ValueError('unexpected_choice')
            delta = choice.get('delta', {})
            if finished and any(delta.get(k) for k in ('content', 'reasoning_content', 'tool_calls')):
                raise ValueError('output_after_finish')
            content += delta.get('content') or ''
            reasoning += delta.get('reasoning_content') or ''
            for item in delta.get('tool_calls') or []:
                index = item.get('index')
                if type(index) is not int or index < 0:
                    raise ValueError('invalid_tool_index')
                call = calls.setdefault(index, dict(id='', name='', arguments='', type=''))
                for key in ('id', 'type'):
                    value = item.get(key)
                    if value is not None and not isinstance(value, str):
                        raise ValueError('invalid_tool_identity_type: ' + key)
                    if value:
                        if call[key] and call[key] != value:
                            raise ValueError('conflicting_tool_identity')
                        call[key] = value
                function = item.get('function', {})
                call['name'] += function.get('name') or ''
                call['arguments'] += function.get('arguments') or ''
            if choice.get('finish_reason'):
                if finished:
                    raise ValueError('duplicate_finish')
                finished = choice['finish_reason']
    if not done or finished not in ('stop', 'tool_calls'):
        raise ValueError('missing_done_or_abnormal_finish: ' + str(finished))
    if not content.strip() and not calls:
        raise ValueError('empty_response_or_reasoning_only')
    names = {tool['function']['name'] for tool in request['tools']}
    if (finished == 'tool_calls') != bool(calls):
        raise ValueError('tool_finish_mismatch')
    ids = set()
    for call in calls.values():
        if not call['id'] or call['id'] in ids or call['type'] != 'function' or call['name'] not in names:
            raise ValueError('invalid_tool_identity_or_name')
        ids.add(call['id'])
        if not isinstance(strict_json_loads(call['arguments']), dict):
            raise ValueError('tool_arguments_must_be_json_object')
    if not isinstance(usage, dict) or any(type(usage.get(k)) is not int or usage[k] <= 0 for k in ('prompt_tokens', 'completion_tokens')):
        raise ValueError('missing_or_invalid_usage')
    return dict(done=done, finish_reason=finished, usage=usage,
                content_chars=len(content), reasoning_chars=len(reasoning),
                tools=[call['name'] for call in calls.values()])


def status(base):
    with urllib.request.urlopen(base + '/runtime/v1/status', timeout=15) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', required=True, help='Target aigate-proxy base URL, without /v1')
    parser.add_argument('--expected-model', required=True)
    parser.add_argument('--expected-router', required=True)
    parser.add_argument('--output', required=True, type=pathlib.Path, help='New evidence directory (must not exist)')
    parser.add_argument('--timeout', type=int, default=600, help='Whole-request timeout in seconds; no retries')
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error('timeout must be positive')
    base = args.base_url.rstrip('/')
    fixtures = pathlib.Path(__file__).resolve().parents[1] / 'testdata/model-admission'
    for name, digest in FIXTURES.items():
        if hashlib.sha256((fixtures / name).read_bytes()).hexdigest() != digest:
            parser.error('fixture hash mismatch: ' + name)
    args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
    report = dict(passed=False, base_url=base, expected_model=args.expected_model,
                  expected_router=args.expected_router, timeout_seconds=args.timeout,
                  fixture_sha256=FIXTURES, started=time.time(), results=[])
    try:
        before = status(base)
        report['runtime_before'] = before
        if before.get('service') != 'aigate-proxy' or before.get('router_id') != args.expected_router or not before.get('admission') or not before.get('config_digest'):
            raise ValueError('wrong_target_or_closed_admission')
        for name in FIXTURES:
            result = dict(file=name, passed=False)
            report['results'].append(result)
            try:
                headers_path, stream_path = args.output / (name + '.headers'), args.output / (name + '.sse')
                completed = subprocess.run([
                    'curl', '--silent', '--show-error', '--noproxy', '*', '--connect-timeout', '15',
                    '--max-time', str(args.timeout), '--request', 'POST',
                    '--header', 'Content-Type: application/json', '--data-binary', '@' + str(fixtures / name),
                    '--dump-header', str(headers_path), '--output', str(stream_path),
                    '--write-out', '%{http_code} %{time_total} %{time_starttransfer}',
                    base + '/v1/chat/completions',
                ], capture_output=True, text=True, timeout=args.timeout + 20)
                result.update(curl_exit=completed.returncode, transfer=completed.stdout, error=completed.stderr)
                if completed.returncode != 0:
                    raise ValueError('transport_failed')
                code, elapsed, first_byte = completed.stdout.split()
                result.update(http_status=int(code), seconds=float(elapsed), first_response_byte_seconds=float(first_byte))
                headers = {}
                for line in headers_path.read_text().splitlines():
                    if line.startswith('HTTP/'):
                        headers = {}
                    elif ':' in line:
                        key, value = line.split(':', 1)
                        headers[key.lower()] = value.strip()
                if code != '200' or headers.get('content-type', '').split(';')[0].strip().lower() != 'text/event-stream':
                    raise ValueError('not_http_200_sse')
                for key, expected in [('x-aigate-model', args.expected_model), ('x-aigate-backend', args.expected_router), ('x-aigate-config-digest', before['config_digest'])]:
                    if headers.get(key) != expected:
                        raise ValueError('target_identity_mismatch: ' + key)
                result.update(validate_sse(stream_path.read_bytes(), json.loads((fixtures / name).read_bytes())))
                result['passed'] = True
            except Exception as exc:
                result['failure'] = str(exc)
            print(json.dumps(result, ensure_ascii=False), flush=True)
        after = status(base)
        report['runtime_after'] = after
        if any(after.get(k) != before.get(k) for k in ('service', 'router_id', 'boot_id', 'config_digest', 'admission')):
            raise ValueError('runtime_changed_during_gate')
        report['passed'] = len(report['results']) == 2 and all(r['passed'] for r in report['results'])
    except Exception as exc:
        report['failure'] = str(exc)
    (args.output / 'report.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())

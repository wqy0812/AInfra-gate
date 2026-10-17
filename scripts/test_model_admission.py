import json
import hashlib
import pathlib
import subprocess
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from model_admission import FIXTURES, validate_sse


REQUEST = {'tools': [{'type': 'function', 'function': {'name': 'lookup'}}]}
USAGE = {'prompt_tokens': 100, 'completion_tokens': 10}


def stream(delta=None, finish='stop', done=True, usage=USAGE):
    events = [{'choices': [{'index': 0, 'delta': delta or {'content': 'ok'}, 'finish_reason': None}]}]
    events.append({'choices': [{'index': 0, 'delta': {}, 'finish_reason': finish}]})
    events.append({'choices': [], 'usage': usage})
    return encode(events) + (b'data: [DONE]\n\n' if done else b'')


def encode(events):
    return ''.join('data: ' + json.dumps(e) + '\n\n' for e in events).encode()


class StreamGateTest(unittest.TestCase):
    def test_text_success(self):
        self.assertEqual(validate_sse(stream(), REQUEST)['finish_reason'], 'stop')

    def test_sse_line_endings_and_bom(self):
        for ending in (b'\n', b'\r\n', b'\r'):
            for prefix in (b'', b'\xef\xbb\xbf'):
                with self.subTest(ending=ending, prefix=prefix):
                    self.assertTrue(validate_sse(prefix + stream().replace(b'\n', ending), REQUEST)['done'])
        self.assertTrue(validate_sse(stream().replace(b'data: [DONE]', b'data:   [DONE] \t'), REQUEST)['done'])

    def test_unterminated_data_event(self):
        for ending in (b'\n', b'\r\n', b'\r'):
            complete = stream().replace(b'\n', ending)
            for raw in (complete[:-len(ending)], complete.rstrip(b'\r\n'),
                        complete + b'data: {}', complete + b'data: {}' + ending):
                with self.subTest(raw=raw), self.assertRaises(ValueError):
                    validate_sse(raw, REQUEST)

    def test_unicode_separators_are_content(self):
        raw = stream(delta={'content': 'first\u2028second\u0085third'}).replace(
            b'\\u2028', '\u2028'.encode()).replace(b'\\u0085', '\u0085'.encode())
        self.assertEqual(validate_sse(raw, REQUEST)['content_chars'], len('first\u2028second\u0085third'))

    def test_non_json_constants(self):
        for constant in ('NaN', 'Infinity', '-Infinity'):
            delta = {'tool_calls': [{'index': 0, 'id': 'call-1', 'type': 'function',
                                    'function': {'name': 'lookup', 'arguments': '{"value":' + constant + '}'}}]}
            for raw in (stream(delta=delta, finish='tool_calls'),
                        stream().replace(b'"content": "ok"', b'"content": "ok", "extra": ' + constant.encode())):
                with self.subTest(constant=constant, raw=raw), self.assertRaises(ValueError):
                    validate_sse(raw, REQUEST)
        # These strings are valid JSON; the gate must not inspect their meaning.
        delta['tool_calls'][0]['function']['arguments'] = '{"value":"NaN Infinity -Infinity"}'
        self.assertEqual(validate_sse(stream(delta=delta, finish='tool_calls'), REQUEST)['tools'], ['lookup'])

    def test_abnormal_completion(self):
        for raw in [stream(done=False), stream(finish='length'), stream(usage=None),
                    stream(delta={'reasoning_content': 'thinking'}),
                    stream() + encode([{'error': {'message': 'late error'}}]),
                    encode([{'error': {'message': 'failed'}}])]:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                validate_sse(raw, REQUEST)

    def test_fragmented_tool_call(self):
        events = [
            {'choices': [{'index': 0, 'delta': {'tool_calls': [{'index': 0, 'id': 'call-1', 'type': 'function', 'function': {'name': 'look', 'arguments': '{"q":'}}]}}]},
            {'choices': [{'index': 0, 'delta': {'tool_calls': [{'index': 0, 'function': {'name': 'up', 'arguments': '"test"}'}}]}, 'finish_reason': 'tool_calls'}]},
            {'choices': [], 'usage': USAGE},
        ]
        result = validate_sse(encode(events) + b'data: [DONE]\n\n', REQUEST)
        self.assertEqual(result['tools'], ['lookup'])

    def test_invalid_tool_calls(self):
        for name, arguments, ident in [('unknown', '{}', 'id'), ('lookup', '{', 'id'),
                                       ('lookup', '[]', 'id'), ('lookup', '{}', '')]:
            delta = {'tool_calls': [{'index': 0, 'id': ident, 'type': 'function', 'function': {'name': name, 'arguments': arguments}}]}
            with self.subTest(delta=delta), self.assertRaises(ValueError):
                validate_sse(stream(delta=delta, finish='tool_calls'), REQUEST)

    def test_tool_identity_and_index_types(self):
        invalid = {
            'id': [42, True, False, 0, 1.5, [], {}],
            'type': [42, True, False, 0, [], {}],
            'index': [False, True, 0.0, '0', None, -1],
        }
        for field, values in invalid.items():
            for value in values:
                call = {'index': 0, 'id': 'call-1', 'type': 'function',
                        'function': {'name': 'lookup', 'arguments': '{}'}, field: value}
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    validate_sse(stream(delta={'tool_calls': [call]}, finish='tool_calls'), REQUEST)

    def test_invalid_identity_in_later_chunk(self):
        for field in ('id', 'type'):
            for value in (42, True, False, 0, [], {}):
                events = [
                    {'choices': [{'index': 0, 'delta': {'tool_calls': [{'index': 0, 'id': 'call-1', 'type': 'function',
                        'function': {'name': 'lookup', 'arguments': '{}'}}]}}]},
                    {'choices': [{'index': 0, 'delta': {'tool_calls': [{'index': 0, field: value}]},
                                  'finish_reason': 'tool_calls'}]},
                    {'choices': [], 'usage': USAGE},
                ]
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    validate_sse(encode(events) + b'data: [DONE]\n\n', REQUEST)

    def test_choice_index_types(self):
        for value in (False, True, 0.0, '0', None):
            raw = stream().replace(b'"index": 0', b'"index": ' + json.dumps(value).encode())
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_sse(raw, REQUEST)


class CommandGateTest(unittest.TestCase):
    def test_original_bytes_reports_and_target_mismatch(self):
        for scenario in ('success', 'wrong_target', 'unterminated_done', 'nan_arguments',
                         'numeric_id', 'boolean_id', 'boolean_index'):
            received = []
            sent = []

            class Handler(BaseHTTPRequestHandler):
                def log_message(self, *args):
                    pass

                def do_GET(self):
                    body = json.dumps(dict(service='aigate-proxy', router_id='router-test',
                                           boot_id='boot-test', config_digest='digest-test', admission=True)).encode()
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(body)

                def do_POST(self):
                    request_bytes = self.rfile.read(int(self.headers['Content-Length']))
                    received.append(hashlib.sha256(request_bytes).hexdigest())
                    raw = stream()
                    if scenario == 'unterminated_done':
                        raw = raw.rstrip(b'\n')
                    elif scenario in ('nan_arguments', 'numeric_id', 'boolean_id', 'boolean_index'):
                        name = json.loads(request_bytes)['tools'][0]['function']['name']
                        call = {'index': 0, 'id': 'call-1', 'type': 'function',
                                'function': {'name': name, 'arguments': '{}'}}
                        if scenario == 'nan_arguments':
                            call['function']['arguments'] = '{"value":NaN}'
                        elif scenario == 'numeric_id':
                            call['id'] = 42
                        elif scenario == 'boolean_id':
                            call['id'] = True
                        else:
                            call['index'] = False
                        raw = stream(delta={'tool_calls': [call]}, finish='tool_calls')
                    sent.append(raw)
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/event-stream')
                    self.send_header('X-Aigate-Model', 'wrong-model' if scenario == 'wrong_target' else 'model-test')
                    self.send_header('X-Aigate-Backend', 'router-test')
                    self.send_header('X-Aigate-Config-Digest', 'digest-test')
                    self.end_headers()
                    self.wfile.write(raw)

            with tempfile.TemporaryDirectory() as temp:
                server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    output = pathlib.Path(temp) / 'evidence'
                    result = subprocess.run([
                        sys.executable, str(pathlib.Path(__file__).with_name('model_admission.py')),
                        '--base-url', 'http://127.0.0.1:' + str(server.server_port),
                        '--expected-model', 'model-test', '--expected-router', 'router-test',
                        '--output', str(output), '--timeout', '10',
                    ], capture_output=True, text=True, timeout=30)
                    self.assertEqual(result.returncode, 0 if scenario == 'success' else 1, scenario + result.stderr + result.stdout)
                    report = json.loads((output / 'report.json').read_text())
                    self.assertEqual(report['passed'], scenario == 'success')
                    self.assertEqual(received, list(FIXTURES.values()))
                    self.assertEqual(len(list(output.glob('*.sse'))), 2)
                    self.assertEqual([(output / (name + '.sse')).read_bytes() for name in FIXTURES], sent)
                    if scenario == 'wrong_target':
                        self.assertTrue(all('target_identity_mismatch' in item['failure'] for item in report['results']))
                finally:
                    server.shutdown()
                    server.server_close()
                    thread.join()


if __name__ == '__main__':
    unittest.main()

import copy
from contextlib import redirect_stderr
import importlib.metadata
import io
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import deepseek_recipe_compare as compare


class ComparisonTest(unittest.TestCase):
    def test_diff_preserves_types_removals_and_json_pointer_escaping(self):
        changes = compare.differences({'a/b': {'~': True}, 'list': [1, 2], 'removed': 3},
                                      {'a/b': {'~': 1}, 'list': [1]})
        self.assertEqual([change['path'] for change in changes], ['/a~1b/~0', '/list/1', '/removed'])
        self.assertEqual(compare.pointer({'a/b': {'~': 1}}, '/a~1b/~0'), 1)
        self.assertFalse(compare.check_case({'flag': 1}, [{'path': '/flag', 'equals': True}])[0]['passed'])
        self.assertFalse(compare.check_case({}, [{'path': '/absent', 'equals': {'missing': True}}])[0]['passed'])

    def test_json_rejects_constants_and_duplicate_keys(self):
        for value in ('NaN', 'Infinity', '1e10000', '{"key":1,"key":2}'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                compare.load_json(value)

    def test_pointer_rejects_negative_indices_and_invalid_escapes(self):
        for path in ('/items/-1', '/items/01', '/items/+0'):
            self.assertIs(compare.pointer({'items': ['first', 'last']}, path), compare.MISSING)
        for path in ('not-a-pointer', '/bad~2escape'):
            with self.assertRaises(ValueError):
                compare.pointer({}, path)

    def test_canonical_stream_preserves_refusal(self):
        before = [{'choices': [{'index': 0, 'delta': {'content': 'ok'}, 'finish_reason': 'stop'}]}]
        after = copy.deepcopy(before)
        after[0]['choices'][0]['delta']['refusal'] = 'refused'
        differences = compare.differences(compare.canonical_events(before), compare.canonical_events(after))
        self.assertEqual([change['path'] for change in differences], ['/choices/0/refusal'])

    def test_canonical_stream_rejects_protocol_corruption(self):
        base = [{'choices': [{'index': 0, 'delta': {'tool_calls': [
            {'index': 0, 'id': 'call-1', 'type': 'function', 'function': {'name': 'echo', 'arguments': '{}'}}
        ]}, 'finish_reason': 'tool_calls'}]}]
        for mutate in (
            lambda event: event['choices'][0]['delta']['tool_calls'][0].update(type='invalid'),
            lambda event: event['choices'][0]['delta']['tool_calls'][0].pop('type'),
            lambda event: event['choices'][0]['delta']['tool_calls'][0].update(id=123),
            lambda event: event['choices'][0]['delta']['tool_calls'][0].update(id=''),
            lambda event: event['choices'][0]['delta']['tool_calls'].append(
                {'index': 1, 'id': 'call-1', 'type': 'function', 'function': {'name': 'echo', 'arguments': '{}'}}),
            lambda event: event['choices'][0]['delta']['tool_calls'][0].update(index=True),
            lambda event: event['choices'][0]['delta'].update(content=123),
            lambda event: event['choices'][0].update(index=False),
            lambda event: event.update(error={'message': 'failed'}),
        ):
            events = copy.deepcopy(base)
            mutate(events[0])
            with self.assertRaises(ValueError):
                compare.canonical_events(events)
        first = copy.deepcopy(base[0])
        first['choices'][0]['finish_reason'] = None
        for field in ('id', 'type'):
            delta = {'tool_calls': [{'index': 0, field: 'conflicting-value'}]}
            with self.assertRaisesRegex(ValueError, 'conflicting protocol tool'):
                compare.canonical_events([first, {'choices': [{'index': 0, 'delta': delta, 'finish_reason': 'tool_calls'}]}])

    def test_canonical_stream_normalizes_stable_ids_and_fragment_boundaries(self):
        def event(delta, finish=None):
            return {'choices': [{'index': 0, 'delta': delta, 'finish_reason': finish}]}
        whole = [event({'tool_calls': [{'index': 0, 'id': 'one', 'type': 'function',
                                       'function': {'name': 'echo', 'arguments': '{"value":"hi"}'}}]}, 'tool_calls')]
        fragmented = [event({'tool_calls': [{'index': 0, 'id': 'another', 'type': 'function',
                                            'function': {'name': 'ec', 'arguments': '{"value":'}}]}),
                      event({'tool_calls': [{'index': 0, 'id': 'another', 'type': 'function',
                                            'function': {'name': 'ho', 'arguments': ' "hi"}'}}]}, 'tool_calls')]
        self.assertEqual(compare.canonical_events(whole), compare.canonical_events(fragmented))

    def test_model_version_and_policy_fields_are_required(self):
        original = compare.load_manifest(compare.MANIFEST)
        for mutate in (
            lambda manifest: manifest['policies'][0].pop('model_version'),
            lambda manifest: manifest['policies'][0].update(default_thinking_mode='true'),
            lambda manifest: manifest['requests'].append(manifest['requests'][0]),
            lambda manifest: manifest['requests'][0].pop('source'),
        ):
            manifest = copy.deepcopy(original)
            mutate(manifest)
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'manifest.json'
                path.write_text(json.dumps(manifest))
                with self.assertRaises(ValueError):
                    compare.load_manifest(path)

    def test_malformed_manifest_is_a_controlled_setup_error(self):
        original = compare.load_manifest(compare.MANIFEST)
        mutations = (
            lambda manifest: manifest['requests'][0].update(id=123),
            lambda manifest: manifest['requests'][0].update(source=1),
            lambda manifest: manifest['requests'][0].update(checks=True),
            lambda manifest: manifest['requests'][0]['checks'][0].update(path=1),
            lambda manifest: manifest['requests'][0]['backend'].update(overrides=[]),
            lambda manifest: manifest['requests'][0]['backend'].update(compatibility=None),
            lambda manifest: manifest['requests'][0]['backend']['compatibility'].update(drop_fields='patches'),
            lambda manifest: manifest['requests'][0].update(target_model=False),
            lambda manifest: manifest['streams'][0].update(policy=[]),
            lambda manifest: manifest['streams'][0].update(output=False),
            lambda manifest: manifest['wire_cases'][0].update(sse={}),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'manifest.json'
            for mutate in mutations:
                manifest = copy.deepcopy(original)
                mutate(manifest)
                path.write_text(json.dumps(manifest), encoding='utf-8')
                with self.assertRaises(ValueError):
                    compare.load_manifest(path)

    def test_explicit_invalid_baseline_never_skips_comparison(self):
        invalid = (None, [], 1, {'schema_version': True, 'snapshot': {}},
                   {'schema_version': 1, 'snapshot': {}, 'provenance': {}})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / 'baseline.json'
            for baseline in invalid:
                path.write_text(json.dumps(baseline), encoding='utf-8')
                with patch.object(compare, 'run_suite', side_effect=AssertionError('invalid baseline must fail before suite runs')), redirect_stderr(io.StringIO()) as error:
                    code = compare.main(['--baseline', str(path), '--output', str(root / 'output')])
                self.assertEqual(code, 2, error.getvalue())
                self.assertFalse((root / 'output').exists())

    def test_fixed_request_hash_mismatch_is_fatal(self):
        case = copy.deepcopy(compare.load_manifest(compare.MANIFEST)['requests'][0])
        case['request']['sha256'] = '0' * 64
        with self.assertRaisesRegex(ValueError, 'fixture hash mismatch'):
            compare.request_body(case)

    def test_version_mismatch_never_falls_back_or_installs(self):
        with patch.object(importlib.metadata, 'version', return_value='0.1.0'):
            with self.assertRaisesRegex(ValueError, 'exactly 0.2.0'):
                compare.load_recipe()


class OfflineSuiteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.recipe = compare.load_recipe()
        cls.manifest = compare.load_manifest(compare.MANIFEST)
        # The Python command must finish even when network connects are forbidden.
        # The Go bridge itself only calls pure adapters/observers and file I/O.
        with patch.object(socket.socket, 'connect', side_effect=AssertionError('network forbidden')), \
                patch.object(socket, 'create_connection', side_effect=AssertionError('network forbidden')):
            cls.snapshot = compare.run_suite(cls.recipe, cls.manifest)

    def test_all_expected_contracts(self):
        report = compare.make_report(self.snapshot, {})
        failures = [(section, ident, check) for section in ('request_cases', 'stream_cases', 'wire_cases')
                    for ident, result in self.snapshot[section].items() for check in result['checks'] if not check['passed']]
        self.assertEqual(failures, [])
        self.assertEqual(report['summary']['checks'], 251)
        self.assertTrue(report['summary']['passed'])

    def test_fixed_inputs_survive_adapter_without_artificial_prompt_changes(self):
        for ident in ('fixed_body', 'fixed_body_agent_ds4'):
            case = self.snapshot['request_cases'][ident]
            self.assertEqual([change['path'] for change in case['adaptation_changes']], ['/model'])
            for conversion in case['official'].values():
                self.assertEqual(conversion['adaptation_differences'], [])

    def test_repeated_runs_are_semantically_identical(self):
        # Recipe generates random tool IDs and creation times; those must not
        # make a different semantic snapshot, even across independent processors.
        again = compare.run_suite(self.recipe, self.manifest)
        self.assertEqual(compare.differences(self.snapshot, again), [])
        self.assertEqual(compare.json_bytes(self.snapshot), compare.json_bytes(again))

    def test_baseline_detects_changed_policy_and_removed_fixture(self):
        baseline = compare.make_report(self.snapshot, {'python': 'old'})
        current = copy.deepcopy(self.snapshot)
        current['policies']['v4_default']['default_thinking_mode'] = False
        current['wire_cases'].pop('missing_done')
        report = compare.make_report(current, {'python': 'new'}, baseline)
        self.assertFalse(report['summary']['passed'])
        self.assertEqual([diff['path'] for diff in report['baseline']['differences']],
                         ['/policies/v4_default/default_thinking_mode', '/wire_cases/missing_done'])
        identity_only = compare.make_report(self.snapshot, {'python': 'new'}, baseline)
        self.assertTrue(identity_only['summary']['passed'])
        self.assertEqual(identity_only['baseline']['provenance_differences'][0]['path'], '/python')

    def test_report_failure_and_unexpected_fixture_contract(self):
        changed = copy.deepcopy(self.snapshot)
        changed['wire_cases']['missing_done']['checks'][0]['passed'] = False
        report = compare.make_report(changed, {})
        self.assertEqual(report['summary']['failed_checks'], 1)
        self.assertFalse(report['summary']['passed'])
        self.assertIn('失败 1 项', compare.render_report(report))

    def test_reference_parser_failure_remains_reportable(self):
        with patch.object(compare, 'recipe_stream', side_effect=ValueError('malformed tool arguments')):
            snapshot = compare.run_suite(self.recipe, self.manifest)
        report = compare.make_report(snapshot, {})
        self.assertFalse(report['summary']['passed'])
        result = snapshot['stream_cases']['tool_dsml']['whole']
        self.assertEqual(result['status'], 'error')
        self.assertEqual(result['error']['message'], 'malformed tool arguments')
        self.assertIn('not_run', compare.render_report(report))

    def test_cli_writes_baseline_report_and_keeps_failures_reviewable(self):
        script = compare.ROOT / 'scripts/deepseek_recipe_compare.py'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = compare.make_report(copy.deepcopy(self.snapshot), {})
            baseline['snapshot']['policies']['v4_default']['default_thinking_mode'] = False
            path = root / 'baseline.json'
            path.write_text(json.dumps(baseline))
            output = root / 'result'
            process = subprocess.run([sys.executable, str(script), '--output', str(output), '--baseline', str(path)],
                                     capture_output=True, text=True, timeout=180)
            self.assertEqual(process.returncode, 1, process.stdout + process.stderr)
            actual = json.loads((output / 'report.json').read_text())
            self.assertEqual(actual['summary']['baseline_differences'], 1)
            self.assertIn('/policies/v4_default/default_thinking_mode', (output / 'report.md').read_text())
            before = (output / 'report.json').read_bytes()
            process = subprocess.run([sys.executable, str(script), '--output', str(output)], capture_output=True, text=True)
            self.assertEqual(process.returncode, 2)
            self.assertEqual((output / 'report.json').read_bytes(), before)


if __name__ == '__main__':
    unittest.main()

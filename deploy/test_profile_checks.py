import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from profile_checks import CHECKPOINTS, metric, policy_of, public_key, stopped_snapshot, values, verify_profile


def load_script(name, relative):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).parent / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


live = load_script('live_accept', 'live-20260915/accept.py')
a3 = load_script('direct_release', 'a3/direct_release.py')
dcu = load_script('slow_release', 'dcu1/slow-requests-20260915/release.py')


def text(policy, started=10, count=7):
    metrics = {
        'aigate_profile_counter_start_time_seconds': started,
        'aigate_error_metrics_start_time_seconds': started,
        'aigate_requests_started_total': count,
        'aigate_requests_inflight': 0,
        'aigate_request_duration_seconds_count{backend="a3",model="m"}': count,
        'aigate_request_duration_seconds_sum{backend="a3",model="m"}': count,
        'aigate_first_increment_seconds_count{backend="a3",model="m"}': count,
        'aigate_nonstream_requests_total': count,
        'aigate_profile_write_errors_total': 0,
        'aigate_profile_dropped_events_total': 0,
    }
    return '\n'.join(public_key(k, policy) + ' ' + str(v) for k, v in metrics.items())


def checkpoint(started=10, count=7):
    return {'version': 1, 'started': started, 'groups': {},
            'values': {'aigate_requests_started_total': count,
                       'aigate_request_duration_seconds_count{backend="a3",model="m"}': count,
                       'aigate_request_duration_seconds_sum{backend="a3",model="m"}': count,
                       'aigate_first_increment_seconds_count{backend="a3",model="m"}': count,
                       'aigate_nonstream_requests_total': count},
            'types': {'aigate_requests_started_total': 'counter',
                      'aigate_request_duration_seconds': 'histogram',
                      'aigate_first_increment_seconds': 'histogram',
                      'aigate_nonstream_requests_total': 'counter'}}


class ProfileReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def snapshot(self, policy):
        path = self.root / CHECKPOINTS[policy]
        path.write_text(json.dumps(checkpoint()))
        return stopped_snapshot(self.root, text(policy))

    def test_same_policy_restart_preserves_counters_and_histograms(self):
        for policy in CHECKPOINTS:
            with self.subTest(policy=policy):
                snapshot = self.snapshot(policy)
                (self.root / CHECKPOINTS[policy]).unlink()  # Startup consumes this checkpoint.
                result = verify_profile(snapshot, text(policy, count=8), self.root)
                self.assertEqual(result['restored_counter_series'], 5)
                self.assertFalse(result['scope_migrated'])

    def test_scope_migrations_start_new_lifecycle_and_preserve_old_files(self):
        for previous, target in [('legacy', 'streaming'), ('legacy', 'metric-v2'), ('streaming', 'metric-v2')]:
            with self.subTest(previous=previous, target=target):
                for path in self.root.iterdir():
                    path.unlink()
                snapshot = self.snapshot(previous)
                result = verify_profile(snapshot, text(target, started=20, count=1), self.root)
                self.assertTrue(result['scope_migrated'])
                self.assertEqual(result['restored_counter_series'], 0)
                with self.assertRaisesRegex(ValueError, 'old counter lifecycle'):
                    verify_profile(snapshot, text(target), self.root)
                (self.root / CHECKPOINTS[previous]).write_text('{}')
                with self.assertRaisesRegex(ValueError, 'Historical checkpoint changed'):
                    verify_profile(snapshot, text(target, started=20), self.root)

    def test_missing_ambiguous_and_nonfinite_metrics_fail_closed(self):
        for body in ['', text('legacy') + '\n' + text('metric-v2'), text('metric-v2').replace(' 7', ' NaN')]:
            with self.subTest(body=body), self.assertRaises(ValueError):
                policy_of(values(body))
        body = '\n'.join(line for line in text('metric-v2').splitlines() if 'requests_inflight' not in line)
        with self.assertRaises(KeyError):
            metric(values(body), 'aigate_requests_inflight')
        with self.assertRaises(ValueError):
            metric(values(text('metric-v2').replace('requests_inflight{request_scope="all"} 0',
                                                     'requests_inflight{request_scope="all"} -1')), 'aigate_requests_inflight')

    def test_missing_or_stale_checkpoint_is_rejected(self):
        with self.assertRaises(FileNotFoundError):
            stopped_snapshot(self.root, text('metric-v2'))
        self.snapshot('metric-v2')
        with self.assertRaisesRegex(ValueError, 'stopped gateway lifecycle'):
            stopped_snapshot(self.root, text('metric-v2', started=20))
        snapshot = self.snapshot('streaming')
        with self.assertRaisesRegex(ValueError, 'stale target checkpoint'):
            verify_profile(snapshot, text('metric-v2', started=20), self.root)

    def test_same_scope_reset_or_histogram_loss_fails(self):
        snapshot = self.snapshot('metric-v2')
        with self.assertRaisesRegex(ValueError, 'lifecycle changed'):
            verify_profile(snapshot, text('metric-v2', started=20), self.root)
        body = text('metric-v2').replace('duration_seconds_sum{backend="a3",model="m",request_scope="all"} 7',
                                         'duration_seconds_sum{backend="a3",model="m",request_scope="all"} 6')
        with self.assertRaisesRegex(ValueError, 'histogram decreased'):
            verify_profile(snapshot, body, self.root)

    def test_a3_uses_active_v2_checkpoint(self):
        profile = self.root / 'request-profile'
        profile.mkdir()
        (profile / CHECKPOINTS['metric-v2']).write_text(json.dumps(checkpoint()))
        snapshot = stopped_snapshot(profile, text('metric-v2'))
        with patch.object(a3, 'STATE', self.root), patch.object(a3, 'load', return_value=snapshot), \
                patch.object(a3, 'metrics', return_value=text('metric-v2', count=8)):
            self.assertEqual(a3.verify_counters()['restored_counter_series'], 5)

    def test_dcu_missing_inflight_does_not_report_idle(self):
        state = {'active_backend': 'dcu', 'backends': {}, 'counts': {}, 'busy': False}
        body = '\n'.join(line for line in text('metric-v2').splitlines() if 'requests_inflight' not in line)
        with patch.object(dcu, 'state', return_value=state), patch.object(dcu, 'metrics', return_value=body), \
                patch.object(dcu, 'save') as save:
            with self.assertRaises(KeyError):
                dcu.wait_idle({'route': dcu.route(state)}, timeout=1)
            save.assert_not_called()

    def test_live_stage_inventory_and_scoped_counters(self):
        for policy in CHECKPOINTS:
            with self.subTest(policy=policy):
                stages = ['routing_discovery', 'preparing_upstream', 'waiting_upstream_headers',
                          'waiting_first_output', 'streaming', 'reading_response', 'writing_client', 'finishing_stream']
                if policy == 'legacy':
                    stages.append('receiving_request')
                lines = [text(policy), public_key('aigate_live_backend_groups', policy) + ' 1']
                lines += ['# TYPE aigate_' + family + ' gauge' for family in live.FAMILIES]
                lines += [public_key('aigate_inflight_requests_by_stage{backend="a3",stage="' + stage + '"}', policy) + ' 0'
                          for stage in stages]
                live.verify_live('\n'.join(lines))
                with self.assertRaises(AssertionError):
                    live.verify_live('\n'.join(lines[:-1]))
                self.assertEqual(live.check_preserved_metrics(text(policy), text(policy, count=8)), 1)


if __name__ == '__main__':
    unittest.main()

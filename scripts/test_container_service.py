import contextlib
import io
import json
import pathlib
import runpy
import sys
import unittest
from unittest.mock import patch


class ContainerServiceTest(unittest.TestCase):
    def run_stop(self, status):
        commands = []
        self.commands = commands
        obj = {'Id': 'container-test', 'State': {'Status': 'running', 'Pid': 123},
               'Config': {'Labels': {}}}

        def command(*argv, **kwargs):
            commands.append(list(argv[0]))
            return json.dumps([obj]).encode()

        def read_bytes(path):
            if path.name == 'cmdline':
                return b'aigate-switch\x00--control-listen\x00127.0.0.1:18083\x00'
            return b'AIGATE_CONTROL_KEY=local-test-only\0'

        with patch.object(sys, 'argv', ['container-service.py', 'ppu', 'stop', 'switch']), \
             patch('subprocess.check_output', side_effect=command), \
             patch.object(pathlib.Path, 'read_bytes', read_bytes), \
             patch('http.client.HTTPConnection') as connection, \
             contextlib.redirect_stdout(io.StringIO()):
            response = connection.return_value.getresponse.return_value
            response.status = 200
            response.read.return_value = json.dumps(status).encode()
            runpy.run_path(str(pathlib.Path(__file__).with_name('container-service.py')), run_name='__main__')
        return commands

    def idle_status(self, recording):
        return dict(admission=False, candidate=None, test_occupancy=None, recording=recording, counts=[])

    def test_stop_with_recording_disabled_or_idle(self):
        for recording in (None, {'sessions': [], 'active_records': 0}):
            with self.subTest(recording=recording):
                commands = self.run_stop(self.idle_status(recording))
                self.assertEqual(commands[1][3:5], ['update', '--restart'])
                self.assertIn('containerd.io/restart.status=stopped', commands[2])
                self.assertEqual(commands[3][3:], ['stop', '--time', '3720', 'ppu-aigate-switch'])

    def test_busy_switch_is_not_stopped(self):
        states = [self.idle_status({'sessions': [{'id': 'record-test'}], 'active_records': 0}),
                  self.idle_status({'sessions': [], 'active_records': 1}),
                  dict(self.idle_status(None), admission=True),
                  dict(self.idle_status(None), counts=[{'business': 1}])]
        for status in states:
            with self.subTest(status=status):
                with self.assertRaises(RuntimeError):
                    self.run_stop(status)
                self.assertEqual(len(self.commands), 1)  # Only the initial inspect.

    def test_missing_recording_status_fails_closed(self):
        status = self.idle_status(None)
        del status['recording']
        with self.assertRaises(KeyError):
            self.run_stop(status)
        self.assertEqual(len(self.commands), 1)


if __name__ == '__main__':
    unittest.main()

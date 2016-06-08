import os
import shutil
import tempfile
import time
import unittest

import whisper
import carbonate_sync


# Super basic tests, mostly to check the syntax.
# TODO(c.chary): Implement more tests.
class TestCarbonateSync(unittest.TestCase):

    def test_run(self):
        ret = carbonate_sync._run('echo test la', shell=True)
        self.assertEquals(ret, ['test la'])

    def test_filter_metrics(self):
        exclude = ['^carbon\.',  '\.foo$']
        metrics = [
            'carbon.bar',
            'carbonfoo.bar',
            'bar.foo'
        ]
        metrics = carbonate_sync._filter_metrics(metrics, exclude)
        self.assertEquals(metrics, ['carbonfoo.bar'])

    def test_heal(self):
        staging_dir = tempfile.mkdtemp(prefix='staging')
        storage_dir = tempfile.mkdtemp(prefix='storage')
        carbonate_sync.STORAGE_DIR = storage_dir
        remote = os.path.join(staging_dir, 'foo.wsp')
        local = os.path.join(storage_dir, 'foo.wsp')
        resolution = [(1, 10)]

        now = int(time.time())

        whisper.create(local, resolution)
        whisper.create(remote, resolution)

        # N, N, N, 6, 5, 4, 3, N, N, N
        whisper.update(local, 6.0, now - 6)
        whisper.update(local, 5.0, now - 5)
        whisper.update(local, 4.0, now - 4)
        whisper.update(local, 3.0, now - 3)

        # N, N, N, 6, 6, N, 3, 2, 1, N
        whisper.update(remote, 6.0, now - 6)
        whisper.update(remote, 6.0, now - 5)
        whisper.update(remote, 3.0, now - 3)
        whisper.update(remote, 2.0, now - 2)
        whisper.update(remote, 1.0, now - 1)

        results = whisper.diff(local, remote)
        expected = [
            (0, [
                (now - 5, 5.0, 6.0),
                (now - 4, 4.0, None),
                (now - 2, None, 2.0),
                (now - 1, None, 1.0),
            ], 6)
        ]
        self.assertEqual(results, expected)

        metrics_fs = ['foo.wsp']
        start_time = now - 10
        end_time = now

        attr = {
            'staging_dir': staging_dir,
            'metrics_fs': metrics_fs,
            'start_time': start_time,
            'end_time': end_time,
            'remote_user': 'graphite',
            'remote_node': 'foo',
            'rsync_options': [],
            'ssh_options': [],
            'overwrite': False,
        }
        batch = carbonate_sync._Batch(**attr)

        carbonate_sync._heal(batch)

        # Check that we add missing points.
        expected = [(0, [(now - 5, 5.0, 6.0), (now - 4, 4.0, None)], 6)]
        results = whisper.diff(local, remote)
        self.assertEqual(results, expected)

        attr['overwrite'] = True
        batch = carbonate_sync._Batch(**attr)
        carbonate_sync._heal(batch)

        expected = [(0, [(now - 4, 4.0, None)], 6)]
        results = whisper.diff(local, remote)
        self.assertEqual(results, expected)

        shutil.rmtree(staging_dir)
        shutil.rmtree(storage_dir)


if __name__ == '__main__':
    unittest.main()

import unittest
import carbonate_sync


# Super basic tests, mostly to check the syntax.
# TODO(c.chary): Implement more tests.
class TestCarbonateSync(unittest.TestCase):

    def test_run(self):
        ret = carbonate_sync._run('echo test la', shell=True)
        self.assertEquals(ret, ['test la'])

    def test_filter_metrics(self):
        exclude = ['^carbon\.',  '.*\.foo$']
        metrics = [
            'carbon.bar',
            'carbonfoo.bar',
            'bar.foo'
        ]
        metrics = carbonate_sync._filter_metrics(metrics, exclude)
        self.assertEquals(metrics, ['carbonfoo.bar'])


if __name__ == '__main__':
    unittest.main()

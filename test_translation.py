import unittest

from cache import Db
from tempfile import mkdtemp
from translation import CachingClient


class TestCachingClient(unittest.TestCase):
    class MockTranslator:
        def translate(self, values):
            return values

    def setUp(self):
        dirname = mkdtemp(suffix='cachingclient', prefix='teststorage')
        self.db = Db(dirname, 'en')
        self.translation_caching_client = CachingClient(self.MockTranslator(), self.db)

    def teadDown(self):
        self.translation_caching_client.close()

    def test_hits_and_misses(self):
        members = 10
        times = 5

        for i in range(0, members * times):
            _ = self.translation_caching_client.translate(str(i % members))

        stats = self.translation_caching_client.stats()
        self.assertEqual(stats['misses'], members)
        self.assertEqual(stats['hits'], (members * times) - members)

    def test_misses(self):
        members = 43
        for i in range(0, members):
            _ = self.translation_caching_client.translate(str(i))

        stats = self.translation_caching_client.stats()
        self.assertEqual(stats['misses'], members)
        self.assertEqual(stats['hits'], 0)


if __name__ == '__main__':
    unittest.main()

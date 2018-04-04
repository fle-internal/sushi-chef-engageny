import unittest

from cache import Db
from tempfile import mkdtemp


class TestCacheDbMethods(unittest.TestCase):
    def setUp(self):
        dirname = mkdtemp(suffix='translation', prefix='teststorage')
        print(dirname)
        self.db = Db(dirname, 'es')

    def tearDown(self):
        self.db.close()

    def test_add_found(self):
        self.db.add(key='hola', data=u'mundo!')
        found, data = self.db.get('hola')
        assert found
        assert data == 'mundo!'
        stats = self.db.stats()
        assert stats['misses'] == 0
        assert stats['hits'] == 1

    def test_add_not_found(self):
        found, data = self.db.get('hola')
        assert not found
        assert not data
        stats = self.db.stats()
        assert stats['misses'] == 1
        assert stats['hits'] == 0

    def test_remove(self):
        self.db.add(key='hola', data=u'mundo!')
        self.db.remove('hola')
        found, data = self.db.get('hola')
        assert not found
        assert not data

    def test_change(self):
        self.db.add(key='hola', data=u'mundo!')
        found, data = self.db.get('hola')
        assert found
        assert data == 'mundo!'
        self.db.add(key='hola', data=u'world!')
        found, data = self.db.get('hola')
        assert found
        assert data == 'world!'

    def test_hits_and_misses(self):
        members = 10
        times = 5

        # Exercise the cache
        for i in range(0, members * times):
            found, data = self.db.get(str(i % members))
            if not found:
                key = str(i)
                self.db.add(key=key, data='value_{key}'.format(key=key))

        stats = self.db.stats()
        assert stats['misses'] == members
        assert stats['hits'] == (members * times) - members

if __name__ == '__main__':
    unittest.main()

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

    def test_add(self):
        self.db.add(key='hola', data=u'carnal!')
        found, data = self.db.get('hola')
        assert found
        assert data == 'carnal!'

if __name__ == '__main__':
    unittest.main()

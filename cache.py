import shelve2
from os.path import join
import hashlib
from json import dump

class Db:
    def __init__(self, basedir, lang):
        self.db_path = join(basedir, f'translation-cache-{lang}')
        self.db = shelve2.open2(self.db_path)
        self.hits = 0
        self.misses = 0

    def _genkey(self, text):
        return hashlib.sha256(text.encode('utf8')).hexdigest()

    def add(self, key, data):
        self.db[self._genkey(key)] = data

    def remove(self, key):
        del self.db[self._genkey(key)]

    def get(self, key):
        genkey = self._genkey(key)
        if genkey in self.db:
            self.hits += 1
            return (True, self.db[genkey])
        else:
            self.misses += 1
            return (False, None)

    def stats(self):
        return dict(hits=self.hits, misses=self.misses)

    def close(self):
        with open(self.db_path + '.cache_stats.txt', 'w') as f:
            dump(self.stats(), f)
        self.db.close()

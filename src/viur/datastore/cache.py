from typing import Dict, List, Union

from google.appengine.api import memcache

class Cache:
	_memcache_max_batch_size = 30
	_memcache_namespace = "viur-datastore"
	_memcache_timeout = 60 * 60
	_memcache_max_size = 1_000_000

	def __init__(self):
		self.client = memcache.Client()

	def get(self, keys: Union[str, List[str]]):
		return self.client.get_multi(keys, namespace=self._memcache_namespace)

	def put(self, cache_data: Dict):
		self.client.set_multi(cache_data, namespace=self._memcache_namespace, time=self._memcache_timeout)


cache = Cache()

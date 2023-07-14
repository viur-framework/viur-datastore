import sys
from typing import Dict, List, Union

from viur.datastore.config import conf
from viur.datastore.types import Entity, Key

memcache_max_batch_size = 30
memcache_namespace = "viur-datastore"
memcache_timeout = 60 * 60
memcache_max_size = 1_000_000


def get(keys: Union[str, Key, List[str], List[Key]]):
	if not isinstance(keys, list):
		keys = [keys]
	keys = [str(key) for key in keys]  # Enforce that all keys are strings
	res = {}

	while keys:
		res |= conf["memcache_client"].get_multi(keys[:memcache_max_batch_size], namespace=memcache_namespace)
		keys = keys[memcache_max_batch_size:]
	return res


def set(cache_data: Union[Dict, List[Entity]]):
	if isinstance(cache_data, dict):
		# Add only values to cache <= memcache_max_size (1.000.000)
		cache_data = {str(key): value for key, value in cache_data.items() if get_size(value) <= memcache_max_size}
	elif isinstance(cache_data, list):
		# Add only values to cache <= memcache_max_size (1.000.000)
		cache_data = {str(item.key): item for item in cache_data if get_size(item) <= memcache_max_size}
	keys = list(cache_data.keys())
	while keys:
		data = {key: cache_data[key] for key in keys[:memcache_max_batch_size]}
		conf["memcache_client"].set_multi(data, namespace=memcache_namespace, time=memcache_timeout)
		keys = keys[memcache_max_batch_size:]


def delete(keys: Union[str, Key, List[str], List[Key]]):
	if not isinstance(keys, list):
		keys = [keys]
	keys = [str(key) for key in keys]  # Enforce that all keys are strings
	while keys:
		conf["memcache_client"].delete_multi(keys[:memcache_max_batch_size], namespace=memcache_namespace)
		keys = keys[memcache_max_batch_size:]


def get_size(obj):
	"""
		Count the size of an object
	"""
	if isinstance(obj, dict):
		return sum([get_size([k, v]) for k, v in obj.items()])
	elif isinstance(obj, list):
		return sum([get_size(x) for x in obj])

	return sys.getsizeof(obj)

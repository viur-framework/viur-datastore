import pprint
import sys
from typing import Any, Dict, List, Union

from viur.datastore.config import conf
from viur.datastore.types import Entity, Key

MEMCACHE_MAX_BATCH_SIZE = 30
MEMCACHE_NAMESPACE = "viur-datastore"
MEMCACHE_TIMEOUT = 60 * 60
MEMCACHE_MAX_SIZE = 1_000_000


"""

	This Module controls the Interaction with the Memcache from Google
	To activate the cache copy this code in your main.py
	..  code-block:: python
	# Example

	if not conf["viur.instance.is_dev_server"]:
		from google.appengine.api.memcache import Client
		from viur.core import db
		db.config["use_memcache_client"] = True
		db.config["memcache_client"] = Client()

"""

__all__ = [
"MEMCACHE_MAX_BATCH_SIZE",
"MEMCACHE_NAMESPACE",
"MEMCACHE_TIMEOUT",
"MEMCACHE_MAX_SIZE",
"get",
"set",
"delete",
]

def get(keys: Union[str, Key, List[str], List[Key]]) -> dict:
	if not isinstance(keys, list):
		keys = [keys]
	keys = [str(key) for key in keys]  # Enforce that all keys are strings
	res = {}
	pprint.pprint(f"try get {keys=}")
	while keys:
		res |= conf["memcache_client"].get_multi(keys[:MEMCACHE_MAX_BATCH_SIZE], namespace=MEMCACHE_NAMESPACE)
		keys = keys[MEMCACHE_MAX_BATCH_SIZE:]
	pprint.pprint(f"get {res=}")
	return res


def set(cache_data: Union[Entity, Dict[Key, Entity], List[Entity]]):
	if isinstance(cache_data, list):
		cache_data = {item.key: item for item in cache_data}
	elif isinstance(cache_data, Entity):
		cache_data = {cache_data.key: cache_data}
	elif not isinstance(cache_data, dict):
		raise TypeError(f"Invalid type {type(cache_data)}. Expected a db.Entity, list or dict.")
	# Add only values to cache <= MEMCACHE_MAX_SIZE (1.000.000)
	cache_data = {str(key): value for key, value in cache_data.items() if get_size(value) <= MEMCACHE_MAX_SIZE}

	keys = list(cache_data.keys())
	pprint.pprint(f"set  {keys=}")
	while keys:
		data = {key: cache_data[key] for key in keys[:MEMCACHE_MAX_BATCH_SIZE]}
		conf["memcache_client"].set_multi(data, namespace=MEMCACHE_NAMESPACE, time=MEMCACHE_TIMEOUT)
		keys = keys[MEMCACHE_MAX_BATCH_SIZE:]


def delete(keys: Union[str, Key, List[str], List[Key]]) -> None:
	if not isinstance(keys, list):
		keys = [keys]
	keys = [str(key) for key in keys]  # Enforce that all keys are strings
	while keys:
		conf["memcache_client"].delete_multi(keys[:MEMCACHE_MAX_BATCH_SIZE], namespace=MEMCACHE_NAMESPACE)
		keys = keys[MEMCACHE_MAX_BATCH_SIZE:]


def get_size(obj: Any) -> int:
	"""
		Utility function that counts the size of an object in bytes.
	"""
	if isinstance(obj, dict):
		return sum(get_size([k, v]) for k, v in obj.items())
	elif isinstance(obj, list):
		return sum(get_size(x) for x in obj)

	return sys.getsizeof(obj)

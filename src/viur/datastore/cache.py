import logging

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
	"put",
	"delete",
]


def get(keys: Union[str, Key, List[str], List[Key]]) -> Dict[str, dict]:
	"""
		Reads data form the memcache.
		:param Union[str, Key, List[str], List[Key]] keys: Unique identifier(s) for one or more entry(s).
		:return: A dict with the entry(s) that found in the memcache.
	"""
	if not check_for_memcache():
		return {}
	if not isinstance(keys, list):
		keys = [keys]
	keys = [str(key) for key in keys]  # Enforce that all keys are strings
	res = {}
	while keys:
		res |= conf["memcache_client"].get_multi(keys[:MEMCACHE_MAX_BATCH_SIZE], namespace=MEMCACHE_NAMESPACE)
		keys = keys[MEMCACHE_MAX_BATCH_SIZE:]
	return res


def put(data: Union[Entity, Dict[Key, Entity], List[Entity]]):
	"""
		Writes Data to the memcache.

		:param Union[Entity, Dict[Key, Entity], List[Entity]] data: Data to write
	"""
	if not check_for_memcache():
		return
	if isinstance(data, list):
		data = {item.key: item for item in data}
	elif isinstance(data, Entity):
		data = {data.key: data}
	elif not isinstance(data, dict):
		raise TypeError(f"Invalid type {type(data)}. Expected a db.Entity, list or dict.")
	# Add only values to cache <= MEMMAX_SIZE (1.000.000)
	data = {str(key): value for key, value in data.items() if get_size(value) <= MEMCACHE_MAX_SIZE}

	keys = list(data.keys())
	while keys:
		data_batch = {key: data[key] for key in keys[:MEMCACHE_MAX_BATCH_SIZE]}
		conf["memclient"].set_multi(data_batch, namespace=MEMCACHE_NAMESPACE, time=MEMCACHE_TIMEOUT)
		keys = keys[MEMCACHE_MAX_BATCH_SIZE:]


def delete(keys: Union[str, Key, List[str], List[Key]]) -> None:
	"""
		Deletes an Entry form memcache.

		:param Union[str, Key, List[str], List[Key]] keys: Unique identifier(s) for one or more entry(s).
	"""
	if not check_for_memcache():
		return
	if not isinstance(keys, list):
		keys = [keys]
	keys = [str(key) for key in keys]  # Enforce that all keys are strings
	while keys:
		conf["memcache_client"].delete_multi(keys[:MEMCACHE_MAX_BATCH_SIZE], namespace=MEMCACHE_NAMESPACE)
		keys = keys[MEMCACHE_MAX_BATCH_SIZE:]


def flush():
	"""
		Deletes everything in memcache.
	"""
	if not check_for_memcache():
		return
	conf["memcache_client"].flush_all()


def get_size(obj: Any) -> int:
	"""
		Utility function that counts the size of an object in bytes.
	"""
	if isinstance(obj, dict):
		return sum(get_size([k, v]) for k, v in obj.items())
	elif isinstance(obj, list):
		return sum(get_size(x) for x in obj)

	return sys.getsizeof(obj)


def check_for_memcache() -> bool:
	if conf["memcache_client"] is None:
		logging.warning(f"""conf["memcache_client"] is 'None'. It can not be used.""")
		return False
	return True

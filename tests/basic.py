import typing as t
import unittest

from viur import datastore
from .base import BaseTestClass, datastoreSampleValues, testKindName, viurTypeToGoogleType

"""
	This test-set ensures that basic operations as get, put and delete work as expected
"""


class BasicFunctionTest(BaseTestClass):

	def test_put(self):
		"""
			Ensure we can store a simple entity
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)
		entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
		datastore.Put(entity)
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)

	def test_get(self):
		"""
			Ensure we can retrieve a simple entity
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)
		self.assertTrue(datastore.Get(datastore.Key(testKindName, "test-entity")) is None)
		e = self.datastoreClient.entity(self.datastoreClient.key(testKindName, "test-entity"))
		self.datastoreClient.put(e)  # Create the entity
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)
		self.assertTrue(datastore.Get(datastore.Key(testKindName, "test-entity")) is not None)

	def test_delete(self):
		"""
			Ensure we can delete a entity
		"""
		e = self.datastoreClient.entity(self.datastoreClient.key(testKindName, "test-entity"))
		self.datastoreClient.put(e)  # Create the entity
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)
		datastore.Delete(datastore.Key(testKindName, "test-entity"))
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)

	def test_delete_non_existant(self):
		"""
			Deleting a non-existing entity should not fail
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)
		datastore.Delete(datastore.Key(testKindName, "test-entity"))
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)

	def test_delete_empty_list(self):
		"""
			Deleting a emty list of keys should just be ignored
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)
		datastore.Delete([])
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)

	def test_datatypes(self):
		"""
			Ensure that we can store and retrieve all supported python types
		"""
		entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
		entity.update(datastoreSampleValues)
		datastore.Put(entity)
		entity2 = datastore.Get(datastore.Key(testKindName, "test-entity"))
		for k, v in datastoreSampleValues.items():
			self.assertEqual(entity2[k], v)
		# Also, double-check with the original google API
		entity3 = self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity"))
		for k, v in datastoreSampleValues.items():
			self.assertEqual(entity3[k], viurTypeToGoogleType(v))

	def test_empty_list(self):
		"""
			The indexed/unindexed flag is handled differently than all other datatypes.
			Ensure, we can store an empty list.
		"""
		testList = []
		entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
		entity["testlist"] = testList
		datastore.Put(entity)
		entity = datastore.Get(datastore.Key(testKindName, "test-entity"))
		self.assertEqual(entity["testlist"], testList)

	def test_unindexed_list(self):
		"""
			The indexed/unindexed flag is handled differently than all other datatypes.
			Ensure, we can set a list to un-indexed.
		"""
		testList = ["a" * 600, "b" * 600]
		entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
		entity["testlist"] = testList
		entity.exclude_from_indexes.add("testlist")
		datastore.Put(entity)
		entity = datastore.Get(datastore.Key(testKindName, "test-entity"))
		self.assertEqual(entity["testlist"], testList)
		self.assertTrue("testlist" in entity.exclude_from_indexes)

	def test_indexed_list(self):
		"""
			The indexed/unindexed flag is handled differently than all other datatypes.
			Ensure, we can store an indexed list.
		"""
		testList = ["a" * 300, "b" * 300]
		entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
		entity["testlist"] = testList
		datastore.Put(entity)
		entity = datastore.Get(datastore.Key(testKindName, "test-entity"))
		self.assertEqual(entity["testlist"], testList)
		self.assertFalse("testlist" in entity.exclude_from_indexes)

	def test_count(self):
		"""
			Ensure that datastore.Count() calls are also covered
		"""
		for x in range(10):  # Create 10 entities to test with
			e = datastore.Entity(datastore.Key(testKindName))
			e["test"] = x
			datastore.Put(e)
		self.assertEqual(datastore.Count(testKindName), 10)
		self.assertEqual(datastore.Count(testKindName, 4), 4)

	def test_key_init(self) -> None:
		key = datastore.Key(testKindName, 42)
		self.assertIsInstance(key.id, int)
		self.assertEqual(key.id, 42)
		self.assertIsNone(key.name)
		self.assertIsNone(key.parent)

		key = datastore.Key(testKindName, "1337")
		self.assertIsInstance(key.id, int)
		self.assertEqual(key.id, 1337)
		self.assertIsNone(key.name)
		self.assertIsNone(key.parent)

		key = datastore.Key(testKindName, "foo")
		self.assertEqual(key.name, "foo")
		self.assertIsNone(key.id)
		self.assertIsNone(key.parent)

		parent_key = datastore.Key(testKindName, "foo")
		key = datastore.Key(testKindName, "bar", parent_key)
		self.assertEqual(key.name, "bar")
		self.assertEqual(key.parent, parent_key)


class TestMulti(BaseTestClass):
	"""Test get single and multi without local memcache"""

	def setUp(self) -> None:
		super().setUp()
		for key in self._get_test_keys():
			entity = datastore.Entity(key)
			entity["foo"] = "bar"
			datastore.Put(entity)

	def _get_test_keys(self) -> t.Iterator[datastore.Key]:
		for name in range(1_000_000, 1_000_025):
			yield datastore.Key(testKindName, int(name))
		for name in range(1_000_025, 1_000_050):
			yield datastore.Key(testKindName, str(name))

	def test_get_single(self):
		"""
		Ensure we can retrieve a multiple entities at once
		"""
		key = next(self._get_test_keys())
		entity = datastore.Get(key)
		self.assertIsInstance(entity, datastore.Entity)
		self.assertEqual(key, entity.key)

	def test_get_multi(self):
		"""
		Ensure we can retrieve a multiple entities at once
		"""
		keys = list(self._get_test_keys())
		for i in range(3):
			result = datastore.Get(keys)
			self.assertIsInstance(result, list)
			self.assertEqual(len(result), len(keys))
			for src_key, entity in zip(keys, result):
				self.assertIsInstance(entity, datastore.Entity)
				self.assertEqual(src_key, entity.key)


class TestMultiMemcache(TestMulti):
	"""Test get single and multi WITH local memcache"""

	def setUp(self) -> None:
		super().setUp()
		datastore.config["memcache_client"] = datastore.cache.LocalMemcache()

	def tearDown(self) -> None:
		super().tearDown()
		datastore.config["memcache_client"] = None


if __name__ == '__main__':
	unittest.main()

import unittest
from viur import datastore
from .base import BaseTestClass, datastoreSampleValues, viurTypeToGoogleType

"""
	This test-set ensures that basic operations as get, put and delete work as expected
"""

class BasicFunctionTest(BaseTestClass):

	def test_put(self):
		"""
			Ensure we can store a simple entity
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key("test-kind", "test-entity")) is None)
		entity = datastore.Entity(datastore.Key("test-kind", "test-entity"))
		datastore.Put(entity)
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key("test-kind", "test-entity")) is not None)

	def test_get(self):
		"""
			Ensure we can retrieve a simple entity
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key("test-kind", "test-entity")) is None)
		self.assertTrue(datastore.Get(datastore.Key("test-kind", "test-entity")) is None)
		e = self.datastoreClient.entity(self.datastoreClient.key("test-kind", "test-entity"))
		self.datastoreClient.put(e)  # Create the entity
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key("test-kind", "test-entity")) is not None)
		self.assertTrue(datastore.Get(datastore.Key("test-kind", "test-entity")) is not None)

	def test_delete(self):
		"""
			Ensure we can delete a entity
		"""
		e = self.datastoreClient.entity(self.datastoreClient.key("test-kind", "test-entity"))
		self.datastoreClient.put(e)  # Create the entity
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key("test-kind", "test-entity")) is not None)
		datastore.Delete(datastore.Key("test-kind", "test-entity"))
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key("test-kind", "test-entity")) is None)

	def test_delete_non_existant(self):
		"""
			Deleting a non-existing entity should not fail
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key("test-kind", "test-entity")) is None)
		datastore.Delete(datastore.Key("test-kind", "test-entity"))
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key("test-kind", "test-entity")) is None)

	def test_delete_empty_list(self):
		"""
			Deleting a emty list of keys should just be ignored
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key("test-kind", "test-entity")) is None)
		datastore.Delete([])
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key("test-kind", "test-entity")) is None)

	def test_datatypes(self):
		"""
			Ensure that we can store and retrieve all supported python types
		"""
		entity = datastore.Entity(datastore.Key("test-kind", "test-entity"))
		entity.update(datastoreSampleValues)
		datastore.Put(entity)
		entity2 = datastore.Get(datastore.Key("test-kind", "test-entity"))
		for k, v in datastoreSampleValues.items():
			self.assertEqual(entity2[k], v)
		# Also, double-check with the original google API
		entity3 = self.datastoreClient.get(self.datastoreClient.key("test-kind", "test-entity"))
		for k, v in datastoreSampleValues.items():
			self.assertEqual(entity3[k], viurTypeToGoogleType(v))

	def test_empty_list(self):
		"""
			The indexed/unindexed flag is handled differently than all other datatypes.
			Ensure, we can store an empty list.
		"""
		testList = []
		entity = datastore.Entity(datastore.Key("test-kind", "test-entity"))
		entity["testlist"] = testList
		datastore.Put(entity)
		entity = datastore.Get(datastore.Key("test-kind", "test-entity"))
		self.assertEqual(entity["testlist"], testList)

	def test_unindexed_list(self):
		"""
			The indexed/unindexed flag is handled differently than all other datatypes.
			Ensure, we can set a list to un-indexed.
		"""
		testList = ["a"*600, "b"*600]
		entity = datastore.Entity(datastore.Key("test-kind", "test-entity"))
		entity["testlist"] = testList
		entity.exclude_from_indexes.add("testlist")
		datastore.Put(entity)
		entity = datastore.Get(datastore.Key("test-kind", "test-entity"))
		self.assertEqual(entity["testlist"], testList)
		self.assertTrue("testlist" in entity.exclude_from_indexes)

	def test_indexed_list(self):
		"""
			The indexed/unindexed flag is handled differently than all other datatypes.
			Ensure, we can store an indexed list.
		"""
		testList = ["a"*300, "b"*300]
		entity = datastore.Entity(datastore.Key("test-kind", "test-entity"))
		entity["testlist"] = testList
		datastore.Put(entity)
		entity = datastore.Get(datastore.Key("test-kind", "test-entity"))
		self.assertEqual(entity["testlist"], testList)
		self.assertFalse("testlist" in entity.exclude_from_indexes)

if __name__ == '__main__':
	unittest.main()

import unittest
from viur import datastore
from viur.datastore import utils
from .base import BaseTestClass, datastoreSampleValues, viurTypeToGoogleType, testKindName

"""
	This test-set ensures that basic operations as get, put and delete work as expected
"""

class DataAccessLogTest(BaseTestClass):

	def test_empty(self):
		"""
			Check the empty log (must be None unless initialized, in which case its the empty set)
		"""
		self.assertEqual(utils.endDataAccessLog(), None)
		utils.startDataAccessLog()
		self.assertEqual(utils.endDataAccessLog(), set())

	def test_put(self):
		"""
			Ensure, that puts are covered if the key was complete
		"""
		# Fist, test with an incomplete key (it should not get logged)
		utils.startDataAccessLog()
		entity = datastore.Entity(datastore.Key(testKindName))
		datastore.Put(entity)
		self.assertEqual(utils.endDataAccessLog(), set())
		# Recheck with a complete key
		utils.startDataAccessLog()
		fullKey = datastore.Key(testKindName, "testentry")
		entity = datastore.Entity(fullKey)
		datastore.Put(entity)
		self.assertEqual(utils.endDataAccessLog(), {fullKey})

	def test_delete(self):
		"""
			Ensure that datastore.Delete() calls are also covered
		"""
		utils.startDataAccessLog()
		fullKey = datastore.Key(testKindName, "testentry")
		datastore.Delete(fullKey)
		self.assertEqual(utils.endDataAccessLog(), {fullKey})


	def test_get(self):
		"""
			Ensure that datastore.Get() calls are also covered
		"""
		utils.startDataAccessLog()
		fullKey = datastore.Key(testKindName, "testentry")
		datastore.Get(fullKey)  # Doesn't matter if that key exists or not
		self.assertEqual(utils.endDataAccessLog(), {fullKey})


	def test_queries(self):
		"""
			A query should add the kind it's run on as a string to the log
		"""
		utils.startDataAccessLog()
		datastore.Query(testKindName).run(99)
		self.assertEqual(utils.startDataAccessLog(), {testKindName})

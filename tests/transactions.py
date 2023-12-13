import unittest, sys
from viur import datastore
from .base import BaseTestClass, datastoreSampleValues, viurTypeToGoogleType, testKindName
from time import sleep
from threading import Thread
from random import random

"""
	Check if transactions work (we can hold the isolation and atomar guarantees)
"""


class IncrementThread(Thread):

	def run(self):
		self.successCount = 0
		for x in range(0, 10):
			def incrementTxn():
				e = datastore.Get(datastore.Key(testKindName, "test-entity"))
				e["count"] += 1
				sleep(random())  # Sleep a random amount of time ([0-1) Seconds) to allow another thread to interfere
				datastore.Put(e)

			try:
				datastore.RunInTransaction(incrementTxn)
				sleep(random())
			except datastore.errors.CollisionError:
				continue
			self.successCount += 1


class IncrementThread_Context(Thread):
	def run(self):
		self.successCount = 0
		for x in range(0, 10):
			with datastore.Transaction():
				e = datastore.Get(datastore.Key(testKindName, "test-entity"))
				e["count"] += 1
				sleep(random())  # Sleep a random amount of time ([0-1) Seconds) to allow another thread to interfere
				datastore.Put(e)
				sleep(random())

			self.successCount += 1


class TransactionTest(BaseTestClass):

	def test_put_preset_key(self):
		"""
			Ensure we can write our test-entity inside a transaction with a pre-set key
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)

		def putTxn():
			entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
			entity.update(datastoreSampleValues)
			datastore.Put(entity)

		datastore.RunInTransaction(putTxn)
		entity2 = datastore.Get(datastore.Key(testKindName, "test-entity"))
		for k, v in datastoreSampleValues.items():
			self.assertEqual(entity2[k], v)

	def test_put_assign_key(self):
		"""
			Ensure we can write our test-entity inside a transaction and have it a key assigned
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)

		def putTxn():
			entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
			entity.update(datastoreSampleValues)
			datastore.Put(entity)
			return entity

		entity = datastore.RunInTransaction(putTxn)
		entity2 = datastore.Get(datastore.Key(testKindName, entity.key.id_or_name))
		for k, v in datastoreSampleValues.items():
			self.assertEqual(entity2[k], v)

	def test_put_visibility_success(self):
		"""
			Ensure that the entity does not get visible before the transaction succeeds
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)

		def putTxn():
			entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
			entity.update(datastoreSampleValues)
			datastore.Put(entity)
			sleep(2)
			# Try to fetch the entity with the google API (which does not care about our transactions)
			self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)
			return entity

		datastore.RunInTransaction(putTxn)
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)

	def test_put_visibility_error(self):
		"""
			Ensure that the entity does not get visible if the transaction aborts
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)

		def putTxn():
			entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
			entity.update(datastoreSampleValues)
			datastore.Put(entity)
			sleep(2)
			# Abort the transaction by raising an exception
			raise OSError

		try:
			datastore.RunInTransaction(putTxn)
		except OSError:
			pass
		sleep(2)
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)

	def test_get(self):
		"""
			Ensure we can retrieve a simple entity inside a transaction
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)
		self.assertTrue(datastore.Get(datastore.Key(testKindName, "test-entity")) is None)
		e = self.datastoreClient.entity(self.datastoreClient.key(testKindName, "test-entity"))
		e.update(viurTypeToGoogleType(datastoreSampleValues))
		self.datastoreClient.put(e)  # Create the entity

		def readTxn():
			return self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity"))

		entity = datastore.RunInTransaction(readTxn)
		for k, v in datastoreSampleValues.items():
			self.assertEqual(entity[k], viurTypeToGoogleType(v))

	def test_delete_txn_success(self):
		"""
			Ensure we can delete a entity inside a transaction
		"""
		e = self.datastoreClient.entity(self.datastoreClient.key(testKindName, "test-entity"))
		self.datastoreClient.put(e)  # Create the entity

		def deleteTxn():
			# Ensure the entity exists before
			self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)
			datastore.Delete(datastore.Key(testKindName, "test-entity"))
			sleep(2)
			# Assert the entity still exists (as the txn did not yet complete)
			self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)

		datastore.RunInTransaction(deleteTxn)
		# Assert the entity is gone
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)

	def test_delete_txn_abort(self):
		"""
			Ensure the entity does not get deleted if the txn aborts
		"""
		e = self.datastoreClient.entity(self.datastoreClient.key(testKindName, "test-entity"))
		self.datastoreClient.put(e)  # Create the entity

		def deleteTxn():
			# Ensure the entity exists before
			self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)
			datastore.Delete(datastore.Key(testKindName, "test-entity"))
			sleep(2)
			# Assert the entity still exists (as the txn did not yet complete)
			self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)
			raise OSError

		try:
			datastore.RunInTransaction(deleteTxn)
		except OSError:
			pass
		# Assert the entity is still there
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)

	def test_isolation(self):
		"""
			Ensure that there are no conflicting writes possible (we can hold the isolation guarantee)
		"""
		e = self.datastoreClient.entity(self.datastoreClient.key(testKindName, "test-entity"))
		e["count"] = 0
		self.datastoreClient.put(e)  # Create the entity, with initial count set to zero
		threadList = [IncrementThread() for _ in range(0, 5)]  # Create 5 of our increment threads
		for thread in threadList:  # Run each thread
			thread.start()
		for thread in threadList:  # Wait for each thread to finish
			thread.join()
		e = self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity"))
		# The number of increments on the datastore object must match the number of successful transactions
		self.assertEqual(e["count"], sum([thread.successCount for thread in threadList]))


class TransactionContextTest(BaseTestClass):
	def test_put_preset_key(self):
		"""
			Ensure we can write our test-entity inside a transaction with a pre-set key
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)
		with datastore.Transaction():
			entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
			entity.update(datastoreSampleValues)
			datastore.Put(entity)

		entity2 = datastore.Get(datastore.Key(testKindName, "test-entity"))
		for k, v in datastoreSampleValues.items():
			self.assertEqual(entity2[k], v)

	def test_put_assign_key(self):
		"""
			Ensure we can write our test-entity inside a transaction and have it a key assigned
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)
		with datastore.Transaction():
			entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
			entity.update(datastoreSampleValues)
			datastore.Put(entity)

		entity2 = datastore.Get(datastore.Key(testKindName, entity.key.id_or_name))
		for k, v in datastoreSampleValues.items():
			self.assertEqual(entity2[k], v)

	def test_put_visibility_success(self):
		"""
			Ensure that the entity does not get visible before the transaction succeeds
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)
		with datastore.Transaction():
			entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
			entity.update(datastoreSampleValues)
			datastore.Put(entity)
			sleep(2)
			# Try to fetch the entity with the google API (which does not care about our transactions)
			self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)

		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)

	def test_put_visibility_error(self):
		"""
			Ensure that the entity does not get visible if the transaction aborts
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)
		try:
			with datastore.Transaction():
				entity = datastore.Entity(datastore.Key(testKindName, "test-entity"))
				entity.update(datastoreSampleValues)
				datastore.Put(entity)
				sleep(2)
				# Abort the transaction by raising an exception
				raise OSError

		except OSError:
			pass
		sleep(2)
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)

	def test_get(self):
		"""
			Ensure we can retrieve a simple entity inside a transaction
		"""
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)
		self.assertTrue(datastore.Get(datastore.Key(testKindName, "test-entity")) is None)
		e = self.datastoreClient.entity(self.datastoreClient.key(testKindName, "test-entity"))
		e.update(viurTypeToGoogleType(datastoreSampleValues))
		self.datastoreClient.put(e)  # Create the entity
		with datastore.Transaction():
			entity = self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity"))

		for k, v in datastoreSampleValues.items():
			self.assertEqual(entity[k], viurTypeToGoogleType(v))

	def test_delete_txn_success(self):
		"""
			Ensure we can delete a entity inside a transaction
		"""
		e = self.datastoreClient.entity(self.datastoreClient.key(testKindName, "test-entity"))
		self.datastoreClient.put(e)  # Create the entity

		with datastore.Transaction():
			# Ensure the entity exists before
			self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)
			datastore.Delete(datastore.Key(testKindName, "test-entity"))
			sleep(2)
			# Assert the entity still exists (as the txn did not yet complete)
			self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)

		# Assert the entity is gone
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is None)

	def test_delete_txn_abort(self):
		"""
			Ensure the entity does not get deleted if the txn aborts
		"""
		e = self.datastoreClient.entity(self.datastoreClient.key(testKindName, "test-entity"))
		self.datastoreClient.put(e)  # Create the entity
		try:

			with datastore.Transaction():
				# Ensure the entity exists before
				self.assertTrue(
					self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)
				datastore.Delete(datastore.Key(testKindName, "test-entity"))
				sleep(2)
				# Assert the entity still exists (as the txn did not yet complete)
				self.assertTrue(
					self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)
				raise OSError

		except OSError:
			pass
		# Assert the entity is still there
		self.assertTrue(self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity")) is not None)

	def test_isolation(self):
		"""
			Ensure that there are no conflicting writes possible (we can hold the isolation guarantee)
		"""
		e = self.datastoreClient.entity(self.datastoreClient.key(testKindName, "test-entity"))
		e["count"] = 0
		self.datastoreClient.put(e)  # Create the entity, with initial count set to zero
		threadList = [IncrementThread_Context() for _ in range(0, 5)]  # Create 5 of our increment threads
		for thread in threadList:  # Run each thread
			thread.start()
		for thread in threadList:  # Wait for each thread to finish
			thread.join()
		e = self.datastoreClient.get(self.datastoreClient.key(testKindName, "test-entity"))
		# The number of increments on the datastore object must match the number of successful transactions
		print(f"""{e["count"]}""")
		self.assertEqual(e["count"], sum([thread.successCount for thread in threadList]))


if __name__ == '__main__':
	unittest.main()

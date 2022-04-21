import unittest, sys
from viur import datastore
from .base import BaseTestClass, datastoreSampleValues, viurTypeToGoogleType, testKindName
from datetime import datetime, timezone, timedelta

"""
	The viur-datastore provides some custom functions on queries (like customQueryMerge callbacks etc) which
	are beeing tested here.
"""


class QueryCustomFunctionsTest(BaseTestClass):
	def test_query_cursor(self):
		# Ensure that cursors work
		for x in range(-10, 10):  # Create 19 entities to test with
			e = datastore.Entity(datastore.Key(testKindName))
			e["intVal"] = x
			datastore.Put(e)
		qry = datastore.Query(testKindName).order(("intVal", datastore.SortOrder.Ascending))
		res = [x["intVal"] for x in qry.run(5)]
		self.assertEqual(res, [-10, -9, -8, -7, -6])  # First 5 Values
		startCursor = qry.getCursor()
		qry = datastore.Query(testKindName).order(("intVal", datastore.SortOrder.Ascending))
		qry.setCursor(startCursor)  # This new query should continue where the old left off
		res = [x["intVal"] for x in qry.run(5)]
		self.assertEqual(res, [-5, -4, -3, -2, -1])  # Next 5 Values
		endCursor = qry.getCursor()
		# This should return the same 5 values of before, despite we request 100 entities
		qry = datastore.Query(testKindName).order(("intVal", datastore.SortOrder.Ascending))
		qry.setCursor(startCursor, endCursor)
		res = [x["intVal"] for x in qry.run(100)]
		self.assertEqual(res, [-5, -4, -3, -2, -1])  # Same 5 Values due to end-cursor
		# we have to test that we eventually get a None cursor after we run over all 20 entries
		qry = datastore.Query(testKindName).order(("intVal", datastore.SortOrder.Ascending))
		qry.setCursor(endCursor)
		res = [x["intVal"] for x in qry.run(20)]  # request more than we inserted...
		self.assertEqual(res, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
		lastCursor = qry.getCursor()
		qry = datastore.Query(testKindName).order(("intVal", datastore.SortOrder.Ascending))
		qry.setCursor(lastCursor)
		res = [x["intVal"] for x in qry.run(10)]  # should be empty by now
		self.assertEqual(res, [])  # should be None now
		self.assertEqual(qry.getCursor(), None)  # should be None now

	def test_query_sort_orders(self):
		# Check our 4 sort-orders (Ascending, Descending, InvertedAscending and InvertedDescending)
		for x in range(-10, 10):  # Create 19 entities to test with
			e = datastore.Entity(datastore.Key(testKindName))
			e["intVal"] = x
			datastore.Put(e)
		qry = datastore.Query(testKindName).order(("intVal", datastore.SortOrder.Ascending))
		qry.run(10)
		centerCursor = qry.getCursor()
		# Query Ascending from this cursor, should return the next 5 Entries
		qry = datastore.Query(testKindName).order(("intVal", datastore.SortOrder.Ascending))
		qry.setCursor(centerCursor)
		res = [x["intVal"] for x in qry.run(5)]
		self.assertEqual(res, [0, 1, 2, 3, 4])
		# Query Descending - should return the negative values up from 0
		qry = datastore.Query(testKindName).order(("intVal", datastore.SortOrder.Descending))
		qry.setCursor(centerCursor)
		res = [x["intVal"] for x in qry.run(5)]
		self.assertEqual(res, [-1, -2, -3, -4, -5])
		# Query Descending, but flip the results (eg. count down from 4 to 0)
		# This should yield the results like in Ascending, but *before* the start-cursor)
		qry = datastore.Query(testKindName).order(("intVal", datastore.SortOrder.InvertedAscending))
		qry.setCursor(centerCursor)
		res = [x["intVal"] for x in qry.run(5)]
		self.assertEqual(res, [-5, -4, -3, -2, -1])
		# Query Ascending, but flip the results (eg. count down from -5 to -1)
		# This should yield the results like in Descending, but *before* the start-cursor)
		qry = datastore.Query(testKindName).order(("intVal", datastore.SortOrder.InvertedDescending))
		qry.setCursor(centerCursor)
		res = [x["intVal"] for x in qry.run(5)]
		self.assertEqual(res, [4, 3, 2, 1, 0])

	def test_query_distinct(self):
		# Validate distinct queries (deduplication)
		for _ in range(0, 3):
			# Create 3 identical sets of the values 0 -> 9
			for x in range(0, 10):
				e = datastore.Entity(datastore.Key(testKindName))
				e["intVal"] = x
				datastore.Put(e)
		qry = datastore.Query(testKindName).order(("intVal", datastore.SortOrder.Ascending))
		qry.distinctOn(["intVal"])
		res = [x["intVal"] for x in qry.run(5)]
		self.assertEqual(res, [0, 1, 2, 3, 4])

	def test_query_filter_hook(self):
		# Test filter-hooks (rewriting new filters as they're being added to the query)
		for x in range(0, 10):
			e = datastore.Entity(datastore.Key(testKindName))
			e["test.intVal"] = x
			datastore.Put(e)

		def filterHook(query, param, value):
			return "test.%s" % param, value

		qry = datastore.Query(testKindName)
		qry.setFilterHook(filterHook)
		qry.filter("intVal =", 5)
		self.assertEqual(qry.getEntry()["test.intVal"], 5)

	def test_query_order_hook(self):
		# Test order-hooks
		for x in range(0, 10):
			e = datastore.Entity(datastore.Key(testKindName))
			e["test.intVal"] = x
			datastore.Put(e)

		def orderHook(query, orderings):
			return [("test.%s" % param, dir) for param, dir in orderings]

		qry = datastore.Query(testKindName)
		qry.setOrderHook(orderHook)
		qry.order(("intVal", datastore.SortOrder.Ascending))
		self.assertEqual([x["test.intVal"] for x in qry.run(5)], [0, 1, 2, 3, 4])

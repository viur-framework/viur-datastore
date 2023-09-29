import unittest, sys
from viur import datastore
from .base import BaseTestClass, datastoreSampleValues, viurTypeToGoogleType, testKindName
from datetime import datetime, timezone, timedelta

"""
	Run a set of queries for each supported datatype. Not all query-types are supported on every datatype.
"""


class QueryValuesTest(BaseTestClass):
	def test_empty_query_run(self):
		# By default, testKindName is empty, so we should fetch an empty list
		self.assertTrue(len(datastore.Query(testKindName).run(10)) == 0)

	def test_empty_query_get(self):
		# By default, testKindName is empty, so we should get None
		self.assertTrue(datastore.Query(testKindName).getEntry() is None)

	def test_int_filter(self):
		for x in range(-10, 10):
			e = datastore.Entity(datastore.Key(testKindName))
			e["intVal"] = x
			datastore.Put(e)
		# Ensure, all 20 Values have been written and can be fetched
		self.assertTrue(len(datastore.Query(testKindName).run(100)) == 20)
		# Check each value individually
		for x in range(-10, 10):
			self.assertTrue(len(datastore.Query(testKindName).filter("intVal =", x).run(100)) == 1)
		# Check > and < filters
		self.assertTrue(len(datastore.Query(testKindName).filter("intVal >", 0).run(100)) == 9)
		self.assertTrue(len(datastore.Query(testKindName).filter("intVal >=", 0).run(100)) == 10)
		self.assertTrue(len(datastore.Query(testKindName).filter("intVal <", 0).run(100)) == 10)
		self.assertTrue(len(datastore.Query(testKindName).filter("intVal <=", 0).run(100)) == 11)
		# Check combined > and < filter
		self.assertTrue(len(datastore.Query(testKindName).filter("intVal >", -5).filter("intVal <", 5).run(100)) == 9)
		self.assertTrue(len(datastore.Query(testKindName).filter("intVal >=", -5).filter("intVal <=", 5).run(100)) == 11)
		# Check IN filter
		self.assertTrue(len(datastore.Query(testKindName).filter("intVal IN", [-2, 0, 1, 99]).run(100)) == 3)
		# Test entities with multiple values
		self.assertTrue(len(datastore.Query(testKindName).filter("intVal =", -2).filter("intVal =", 3).run(100)) == 0)
		# Create an entity with multiple values
		e = datastore.Entity(datastore.Key(testKindName))
		e["intVal"] = [-2, 0, 3]
		datastore.Put(e)
		self.assertTrue(len(datastore.Query(testKindName).filter("intVal =", -2).filter("intVal =", 3).run(100)) == 1)
		self.assertTrue(len(datastore.Query(testKindName).filter("intVal =", -2).filter("intVal =", 5).run(100)) == 0)

	def test_float_filter(self):
		floatValues = [-99.9999, 0.0, 1.000000000000001, 1.000000000000002]
		for x in floatValues:
			e = datastore.Entity(datastore.Key(testKindName))
			e["floatVal"] = x
			datastore.Put(e)
		# Ensure, all 20 Values have been written and can be fetched
		self.assertTrue(len(datastore.Query(testKindName).run(100)) == 4)
		# Check each value individually
		for x in floatValues:
			self.assertTrue(len(datastore.Query(testKindName).filter("floatVal =", x).run(100)) == 1)
		# Check > and < filters
		self.assertTrue(len(datastore.Query(testKindName).filter("floatVal >", 0.0).run(100)) == 2)
		self.assertTrue(len(datastore.Query(testKindName).filter("floatVal >=", 0.0).run(100)) == 3)
		self.assertTrue(len(datastore.Query(testKindName).filter("floatVal <", 0.0).run(100)) == 1)
		self.assertTrue(len(datastore.Query(testKindName).filter("floatVal <=", 0.0).run(100)) == 2)
		# Check combined > and < filter
		self.assertTrue(len(datastore.Query(testKindName).filter("floatVal >", -99.9999).filter("floatVal <", 1.000000000000002).run(100)) == 2)
		self.assertTrue(len(datastore.Query(testKindName).filter("floatVal >=", -99.9999).filter("floatVal <=", 1.000000000000002).run(100)) == 4)
		# Check IN filter
		self.assertTrue(len(datastore.Query(testKindName).filter("floatVal IN", [-99.9999, 1.000000000000001, 1.000000000000003]).run(100)) == 2)
		# Test entities with multiple values
		self.assertTrue(len(datastore.Query(testKindName).filter("floatVal =", -99.9999).filter("floatVal =", 1.000000000000001).run(100)) == 0)
		# Create an entity with multiple values
		e = datastore.Entity(datastore.Key(testKindName))
		e["floatVal"] = [-99.9999, 1.000000000000001, 1.000000000000002]
		datastore.Put(e)
		self.assertTrue(len(datastore.Query(testKindName).filter("floatVal =", -99.9999).filter("floatVal =", 1.000000000000001).run(100)) == 1)
		self.assertTrue(len(datastore.Query(testKindName).filter("floatVal =", -99.9999).filter("floatVal =", 1.000000000000003).run(100)) == 0)

	def test_asci_str_filter(self):
		hexChar = "ABCDEF"
		for charA in hexChar:
			for charB in hexChar:
				e = datastore.Entity(datastore.Key(testKindName))
				e["strVal"] = "%s%s" % (charA, charB)
				datastore.Put(e)
		# Ensure that all 36 Values have been written and can be fetched
		self.assertTrue(len(datastore.Query(testKindName).run(100)) == 36)
		# Check each value individually
		for charA in hexChar:
			for charB in hexChar:
				self.assertTrue(len(datastore.Query(testKindName).filter("strVal =", "%s%s" % (charA, charB)).run(100)) == 1)
		# Check > and < filters
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal >", "F").run(100)) == 6)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal >=", "F").run(100)) == 6)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal >", "FA").run(100)) == 5)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal >=", "FA").run(100)) == 6)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal <", "B").run(100)) == 6)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal <=", "B").run(100)) == 6)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal <", "BA").run(100)) == 6)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal <=", "BA").run(100)) == 7)
		# Check combined > and < filter
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal >", "B").filter("strVal <", "C").run(100)) == 6)
		# Check IN filter
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal IN", ["AA", "BB", "ZZ"]).run(100)) == 2)
		# Test entities with multiple values
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal =", "AA").filter("strVal =", "BB").run(100)) == 0)
		# Create an entity with multiple values
		e = datastore.Entity(datastore.Key(testKindName))
		e["strVal"] = ["AA", "BB", "CC"]
		datastore.Put(e)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal =", "AA").filter("strVal =", "BB").run(100)) == 1)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal =", "AA").filter("strVal =", "ZZ").run(100)) == 0)

	def test_unicode_str_filter(self):
		# Repeat basic queries on strings with unicode-values (we use german umlauts here)
		e = datastore.Entity(datastore.Key(testKindName))
		e["strVal"] = "äöü"
		datastore.Put(e)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal =", "äöü").run(100)) == 1)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal =", "üöä").run(100)) == 0)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal >", "äö").run(100)) == 1)
		self.assertTrue(len(datastore.Query(testKindName).filter("strVal <", "äö\ufffd").run(100)) == 1)

	def test_boolean_filter(self):
		for boolVal in [True, False]:
			e = datastore.Entity(datastore.Key(testKindName))
			e["boolVal"] = boolVal
			datastore.Put(e)
		self.assertTrue(len(datastore.Query(testKindName).filter("boolVal =", True).run(100)) == 1)
		self.assertTrue(len(datastore.Query(testKindName).filter("boolVal =", False).run(100)) == 1)

	def test_null_filter(self):
		# Check if we can filter by None. To be sure, we also add a non-None-Value to the testset
		for boolVal in [None, False]:
			e = datastore.Entity(datastore.Key(testKindName))
			e["nullVal"] = boolVal
			datastore.Put(e)
		self.assertTrue(len(datastore.Query(testKindName).filter("nullVal =", None).run(100)) == 1)

	def test_key_value(self):
		key_list = [datastore.Key(testKindName, 1234),
					datastore.Key(testKindName, 5678),
					datastore.Key(testKindName, "teststr"),
					datastore.Key(testKindName, "teststr", parent=datastore.Key(testKindName, "teststr")),
					datastore.Key(testKindName, "42"),  # String with only digits (#33)
					datastore.Key(testKindName, "1337", parent=datastore.Key(testKindName, "13")),
					]
		for keyVal in key_list:
			e = datastore.Entity(datastore.Key(testKindName))
			e["keyVal"] = keyVal
			datastore.Put(e)
		for keyVal in key_list:
			self.assertTrue(len(datastore.Query(testKindName).filter("keyVal =", keyVal).run(100)) == 1)
		# FIXME: HAS_ANCESTOR is not yet implemented

	def test_datetime_filter(self):
		now = datetime.now(timezone.utc)
		for offset in range(0,5):
			e = datastore.Entity(datastore.Key(testKindName))
			e["dateVal"] = now + timedelta(minutes=offset)
			datastore.Put(e)
		for offset in range(0,5):
			self.assertTrue(len(datastore.Query(testKindName).filter("dateVal =", now + timedelta(minutes=offset)).run(100)) == 1)
		# Check > and < filters
		self.assertTrue(len(datastore.Query(testKindName).filter("dateVal >", now + timedelta(minutes=2)).run(100)) == 2)
		self.assertTrue(len(datastore.Query(testKindName).filter("dateVal >=", now + timedelta(minutes=2)).run(100)) == 3)
		self.assertTrue(len(datastore.Query(testKindName).filter("dateVal <", now + timedelta(minutes=2)).run(100)) == 2)
		self.assertTrue(len(datastore.Query(testKindName).filter("dateVal <=", now + timedelta(minutes=2)).run(100)) == 3)
		# Check combined > and < filter
		self.assertTrue(len(datastore.Query(testKindName).filter("dateVal >", now + timedelta(minutes=1)).filter("dateVal <", now + timedelta(minutes=3)).run(100)) == 1)
		# Check IN filter
		self.assertTrue(len(datastore.Query(testKindName).filter("dateVal IN", [now + timedelta(minutes=1), now + timedelta(minutes=2), now + timedelta(minutes=11)]).run(100)) == 2)
		# Test entities with multiple values
		self.assertTrue(len(datastore.Query(testKindName).filter("dateVal =", now + timedelta(minutes=1)).filter("dateVal =", now + timedelta(minutes=2)).run(100)) == 0)
		# Create an entity with multiple values
		e = datastore.Entity(datastore.Key(testKindName))
		e["dateVal"] = [now + timedelta(minutes=1), now + timedelta(minutes=2), now + timedelta(minutes=11)]
		datastore.Put(e)
		self.assertTrue(len(datastore.Query(testKindName).filter("dateVal =", now + timedelta(minutes=1)).filter("dateVal =", now + timedelta(minutes=2)).run(100)) == 1)
		self.assertTrue(len(datastore.Query(testKindName).filter("dateVal =", now + timedelta(minutes=1)).filter("dateVal =", now + timedelta(minutes=12)).run(100)) == 0)

	def test_botched_inner_key(self):
		# It's possible to assign broken/partial keys to inner entries (in which case we'll read it as an
		# incomplete key). However, id=0 keys work fine on outer entries.
		outerEntry = datastore.Entity(datastore.Key(testKindName))
		innerEntry = datastore.Entity(datastore.Key(testKindName, 0))  # This key will be read back as incomplete
		outerEntry["innerEntry"] = innerEntry
		datastore.Put(outerEntry)
		self.assertEqual(datastore.Query(testKindName).getEntry(), outerEntry)

	def test_key_in_filter(self):
		# Ensure, we can use a __in__ filter on keys
		outerKeyList = []
		innerKeyList = []
		for idx in range(1, 4):
			outerEntry = datastore.Entity(datastore.Key(testKindName))
			innerEntry = datastore.Entity(datastore.Key(testKindName, idx))
			outerEntry["innerEntry"] = innerEntry
			datastore.Put(outerEntry)
			outerKeyList.append(outerEntry.key)
			innerKeyList.append(innerEntry.key)
		self.assertEqual(len(datastore.Query(testKindName).filter("__key__ IN", outerKeyList).run()), 3)
		self.assertEqual(len(datastore.Query(testKindName).filter("innerEntry.__key__ IN", innerKeyList).run()), 3)

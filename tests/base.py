import unittest, sys
from viur import datastore
from google.cloud import datastore as gcpdatastore, exceptions as gcpexceptions
from datetime import datetime, timezone

"""
	Base Class for testing our datastore API.
	It will provide a reference to the google datastore api aswell as ours,
	and ensure that each test starts with an empty test-kind database.
"""
__client__ = gcpdatastore.Client()
testKindName = "test-kind"

datastoreSampleValues = {
	"strTypeEmpty": "",
	"strTypeAscii": "abcdefghijklmnopqrstuvwxyz",
	"strTypeUnicode": "öäüÖÄÜ",
	"intType0": 0,
	"intType123456789": 123456789,
	"intType-123456789": -123456789,
	"intTypeMaxInt": sys.maxsize,
	"intTypeMinInt": -sys.maxsize - 1,
	"floatType0": 0.0,
	"floatType0.1": 0.1,
	"floatType-0.1": -0.1,
	"noneType": None,
	"boolTypeTrue": True,
	"boolTypeFalse": False,
	"keyTypeStr": datastore.Key(testKindName, "ref-key"),
	"keyTypeInt": datastore.Key(testKindName, 123456),
	"keyTypeParent": datastore.Key(testKindName, 123456, parent=datastore.Key(testKindName, "ref-key")),
	"bytesTypeEmpty": b"",
	"bytesTypeAscii": b"test",
	"bytesTypeControlChars": b"\0\r\n\1\2\3\255",
	"dateType": datetime.now(timezone.utc)
}

# Build a nested Entity
entity = datastore.Entity(datastore.Key(testKindName, "embed-key"))
entity.update(datastoreSampleValues)
datastoreSampleValues["entityType"] = entity

# Build a list of all values currently contained in the samplevalues dict
datastoreSampleValues["listType"] = list(datastoreSampleValues.values())

def viurTypeToGoogleType(val):
	"""
		Converts types used by our API (like datastore.key) to the corresponding type used by googles API
	"""
	def rewriteKey(key):
		if key:
			return datastore.Key(key.kind, key.id_or_name, parent=rewriteKey(key.parent)) # FIXME
	if isinstance(val, datastore.Key):
		return rewriteKey(val)
	elif isinstance(val, list):
		return [viurTypeToGoogleType(x) for x in val]
	elif isinstance(val, datastore.Entity):
		resEntity = __client__.entity(viurTypeToGoogleType(val.key))
		resEntity.update({k: viurTypeToGoogleType(v) for k, v in val.items()})
		return resEntity
	elif isinstance(val, dict):
		return {k: viurTypeToGoogleType(v) for k, v in val.items()}
	elif isinstance(val, datastore.Entity):
		return None ## FIXME
	else:
		return val

class BaseTestClass(unittest.TestCase):
	datastoreClient = __client__

	def setUp(self) -> None:
		query = __client__.query(kind=testKindName)
		for entity in query.fetch():
			__client__.delete(entity.key)

	def tearDown(self) -> None:
		query = __client__.query(kind=testKindName)
		for entity in query.fetch():
			__client__.delete(entity.key)


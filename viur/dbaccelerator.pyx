# distutils: sources = viur/simdjson.cpp
# distutils: language = c++
# cython: language_level=3
import pprint

from libc.stdint cimport int64_t, uint64_t
from libcpp cimport bool
from cpython.bytes cimport PyBytes_AsStringAndSize
from cython.operator cimport preincrement, dereference
import google.auth
from google.auth.transport import requests  # noqa  # pylint: disable=unused-import
from google.cloud import datastore
import json
from datetime import datetime, timezone
from base64 import b64decode, urlsafe_b64encode, urlsafe_b64decode
import logging
from requests.exceptions import ConnectionError as RequestsConnectionError
from typing import Union, Tuple, List, Dict, Any, Callable, Set, Optional
from datetime import datetime
from contextvars import ContextVar
from google.cloud.datastore import _app_engine_key_pb2

## Start of C-Imports

cdef extern from "Python.h":
	object PyUnicode_FromStringAndSize(const char *u, Py_ssize_t size)

cdef extern from "string_view" namespace "std":
	cppclass stringView "std::string_view":
		char * data()
		int length()
		int compare(char *)

cdef extern from "simdjson.h" namespace "simdjson::error_code":
	cdef enum simdjsonErrorCode "simdjson::error_code":
		SUCCESS,
		NO_SUCH_FIELD


cdef extern from "simdjson.h" namespace "simdjson::dom::element_type":
	cdef enum simdjsonElementType "simdjson::dom::element_type":
		OBJECT,
		ARRAY,
		STRING,
		INT64,
		UINT64,
		DOUBLE,
		BOOL,
		NULL_VALUE

cdef extern from "simdjson.h" namespace "simdjson::simdjson_result":
	cdef cppclass simdjsonResult "simdjson::simdjson_result<simdjson::dom::element>":
		simdjsonErrorCode error()
		simdjsonElement value()


cdef extern from "simdjson.h" namespace "simdjson::dom":
	cdef cppclass simdjsonObject "simdjson::dom::object":
		cppclass iterator:
			iterator()
			iterator operator++()
			bint operator !=(iterator)
			stringView key()
			simdjsonElement value()
		simdjsonObject()
		iterator begin()
		iterator end()

	cdef cppclass simdjsonArray "simdjson::dom::array":
		cppclass iterator:
			iterator()
			iterator operator++()
			bint operator !=(iterator)
			simdjsonElement operator *()
		simdjsonArray()
		iterator begin()
		iterator end()
		simdjsonElement at(int) except +
		simdjsonElement at_pointer(const char *) except +
		int size()

	cdef cppclass simdjsonElement "simdjson::dom::element":
		simdjsonElement()
		simdjsonElementType type() except +
		bool get_bool() except +
		int64_t get_int64() except +
		uint64_t get_uint64() except +
		double get_double() except +
		stringView get_string() except +
		simdjsonArray get_array() except +
		simdjsonObject get_object() except +
		simdjsonElement at_key(const char *) except +  # Raises if key not found
		simdjsonResult at(const char *)  # Same as at_key - sets result error code if key not found

	cdef cppclass simdjsonParser "simdjson::dom::parser":
		simdjsonParser()
		simdjsonElement parse(const char * buf, size_t len, bool realloc_if_needed) except +

## End of C-Imports

# Pointer to the current transaction this thread may be currently in
currentTransaction = ContextVar("CurrentTransaction", default=None)

class Entity(dict):
	def __init__(self, key=None, exclude_from_indexes=()):
		super(Entity, self).__init__()
		self.key = key
		self.exclude_from_indexes = exclude_from_indexes
		self.version = None


class Key:
	__slots__ = ["id", "name", "kind", "parent"]
	def __init__(self, kind: str, subKey: Union[int, str] = None, parent: 'Key' = None):
		super().__init__()
		self.kind = kind
		self.id = None
		self.name = None
		if isinstance(subKey, int):
			self.id = subKey
		elif isinstance(subKey, str):
			assert not subKey.isdigit(), "Digit-Only string keys are not permitted"
			self.name = subKey
		self.parent = parent

	@property
	def id_or_name(self):
		return self.id or self.name

	def __repr__(self):
		return "<viur.dbaccelerator.Key %s/%s, parent=%s>" % (self.kind, self.id_or_name, self.parent)

	def __hash__(self):
		return hash("%s.%s.%s" % (self.kind, self.id, self.name))

	def __eq__(self, other):
		return self.kind == other.kind and self.id == other.id and self.name == other.name and self.parent == other.parent

	def to_legacy_urlsafe(self):
		currentKey = self
		pathElements = []
		while currentKey:
			pathElements.insert(0, _app_engine_key_pb2.Path.Element(
				type=currentKey.kind,
				id=currentKey.id,
				name=currentKey.name,
			))
			currentKey = self.parent
		reference = _app_engine_key_pb2.Reference(
			app=projectID,
			path=_app_engine_key_pb2.Path(element=pathElements),
		)
		raw_bytes = reference.SerializeToString()
		return urlsafe_b64encode(raw_bytes).strip(b"=")

	@property
	def is_partial(self):
		return self.id_or_name is None

	@classmethod
	def from_legacy_urlsafe(cls, strKey):
		urlsafe = strKey.encode("ASCII")
		padding = b"=" * (-len(urlsafe) % 4)
		urlsafe += padding
		raw_bytes = urlsafe_b64decode(urlsafe)
		reference = _app_engine_key_pb2.Reference()
		reference.ParseFromString(raw_bytes)
		resultKey = None
		for elem in reference.path.element:
			resultKey = Key(elem.type, elem.id or elem.name, parent=resultKey)
		return resultKey



credentials, projectID = google.auth.default(scopes=["https://www.googleapis.com/auth/datastore"])
_http_internal = google.auth.transport.requests.AuthorizedSession(
	credentials,
	refresh_timeout=300,
)

cdef inline object toPyStr(stringView strView):
	# convert cpp stringview to a python str object
	return PyUnicode_FromStringAndSize(
		strView.data(),
		strView.length()
	)

cdef inline object parseKey(simdjsonElement v):
	# Parses a simdJsonObject to datastore.Key
	cdef simdjsonArray arr
	cdef simdjsonArray.iterator arrayIt, arrayItEnd
	pathArgs = []
	arr = v.at_key("path").get_array()
	arrayIt = arr.begin()
	arrayItEnd = arr.end()
	while arrayIt != arrayItEnd:
		element = dereference(arrayIt)
		if element.at("id").error() == SUCCESS:
			pathArgs.append(
				(toPyStr(element.at_key("kind").get_string()), int(toPyStr(element.at_key("id").get_string())))
			)
		else:
			# We don't expect to read incomplete keys from the datastore :)
			pathArgs.append(
				(toPyStr(element.at_key("kind").get_string()), toPyStr(element.at_key("name").get_string()))
			)
		preincrement(arrayIt)
	key = None
	for pathElem in pathArgs:
		key = Key(*pathElem, parent=key)
	return key

cdef inline object toEntityStructure(simdjsonElement v, bool isInitial = False):
	# Like toPythonStructure, but parses directly into db.Entity etc instead of normal python
	# objects like dicts
	cdef simdjsonElementType et = v.type()
	cdef simdjsonArray.iterator arrayIt, arrayItEnd
	cdef simdjsonObject.iterator objIterStart, objIterStartInner, objIterEnd, objIterEndInner
	cdef simdjsonElement element
	cdef simdjsonObject outerObject, innerObject
	cdef stringView strView, bytesView
	cdef simdjsonResult tmpResult
	if et == OBJECT:
		outerObject = v.get_object()
		objIterStart = outerObject.begin()
		objIterEnd = outerObject.end()
		while objIterStart != objIterEnd:
			strView = objIterStart.key()
			if strView.compare("entity") == 0 or strView.compare("entityValue") == 0:
				e = Entity()
				excludeList = set()
				tmpResult = objIterStart.value().at("key")
				if tmpResult.error() == SUCCESS:
					e.key = parseKey(tmpResult.value())
				tmpResult = objIterStart.value().at("properties")
				if tmpResult.error() == SUCCESS:
					innerObject = tmpResult.value().get_object()
					objIterStartInner = innerObject.begin()
					objIterEndInner = innerObject.end()
					while objIterStartInner != objIterEndInner:
						e[toPyStr(objIterStartInner.key())] = toEntityStructure(objIterStartInner.value())
						tmpResult = objIterStartInner.value().at("excludeFromIndexes")
						if tmpResult.error() == SUCCESS and tmpResult.value().get_bool():
							excludeList.add(toPyStr(objIterStartInner.key()))
						preincrement(objIterStartInner)
				e.exclude_from_indexes = excludeList
				return e
			elif (strView.compare("nullValue") == 0):
				return None
			elif (strView.compare("stringValue") == 0):
				return toPyStr(objIterStart.value().get_string())
			elif (strView.compare("doubleValue") == 0):
				return objIterStart.value().get_double()
			elif (strView.compare("integerValue") == 0):
				return int(toPyStr(objIterStart.value().get_string()))
			elif (strView.compare("arrayValue") == 0):
				tmpResult = objIterStart.value().at("values")
				if tmpResult.error() == SUCCESS:
					return toEntityStructure(tmpResult.value())
				else:
					return []
			elif (strView.compare("booleanValue") == 0):
				return objIterStart.value().get_bool()
			elif (strView.compare("timestampValue") == 0):
				dateStr = toPyStr(objIterStart.value().get_string())[:-1]  # Strip "Z" at the end
				if "." in dateStr:  # With milli-seconds
					return datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
				else:
					return datetime.strptime(dateStr, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
			elif (strView.compare("blobValue") == 0):
				return b64decode(toPyStr(objIterStart.value().get_string()))
			elif (strView.compare("keyValue") == 0):
				return parseKey(objIterStart.value())
			elif (strView.compare("geoPointValue") == 0):
				return objIterStart.value().at_key("latitude").get_double(), \
					   objIterStart.value().at_key("longitude").get_double()
			elif (strView.compare("excludeFromIndexes") == 0 or strView.compare("version") == 0):
				pass
			else:
				raise ValueError("Invalid key in entity json: %s" % toPyStr(strView))
	elif et == ARRAY:
		arr = v.get_array()
		arrayIt = arr.begin()
		arrayItEnd = arr.end()
		if isInitial:
			res = {}
		else:
			res = []
		while arrayIt != arrayItEnd:
			element = dereference(arrayIt)
			entity = toEntityStructure(element)
			if isInitial:
				res[entity.key] = entity
			else:
				res.append(entity)
			preincrement(arrayIt)
		return res

cdef inline object toPythonStructure(simdjsonElement v):
	# Convert json (sub-)tree to python objects
	cdef simdjsonElementType et = v.type()
	cdef simdjsonArray.iterator arrayIt, arrayItEnd
	cdef simdjsonObject.iterator objIterStart, objIterEnd
	cdef simdjsonElement element
	cdef simdjsonObject Object
	cdef stringView strView
	if et == OBJECT:
		Object = v.get_object()
		objIterStart = Object.begin()
		objIterEnd = Object.end()
		res = {}
		while objIterStart != objIterEnd:
			strView = objIterStart.key()
			res[toPyStr(strView)] = toPythonStructure(objIterStart.value())
			preincrement(objIterStart)
		return res
	elif et == ARRAY:
		arr = v.get_array()
		arrayIt = arr.begin()
		arrayItEnd = arr.end()
		res = []
		while arrayIt != arrayItEnd:
			element = dereference(arrayIt)
			res.append(toPythonStructure(element))
			preincrement(arrayIt)
		return res
	elif et == STRING:
		return toPyStr(v.get_string())
	elif et == INT64:
		return v.get_int64()
	elif et == UINT64:
		return v.get_uint64()
	elif et == DOUBLE:
		return v.get_double()
	elif et == NULL_VALUE:
		return None
	elif et == BOOL:
		return v.get_bool()
	else:
		raise ValueError("Unknown type")

def keyToPath(key):
	res = []
	while key:
		res.insert(0,
			{
				"kind": key.kind,
				"id": key.id,
			} if key.id else {
				"kind": key.kind,
				"name": key.name
			} if key.name else {
				"kind": key.kind,
			}
		)
		key = key.parent
	return res

def fetchMulti(keys: List[Key]):
	cdef simdjsonParser parser = simdjsonParser()
	cdef Py_ssize_t pysize
	cdef char * data_ptr
	cdef simdjsonElement element

	keyList = [
		{
			"partitionId": {
				"project_id": projectID,
			},
			"path": keyToPath(x)
		}
		for x in keys]
	res = {}
	currentTxn = currentTransaction.get()
	if currentTxn:
		readOptions = {"transaction": currentTxn["key"]}
	else:
		readOptions = {"readConsistency": "STRONG"}
	while keyList:
		postData = {
			"readOptions": readOptions,
			"keys": keyList[:300],
		}
		for i in range(0, 3):
			try:
				req = _http_internal.post(
					url="https://datastore.googleapis.com/v1/projects/%s:lookup" % projectID,
					data=json.dumps(postData).encode("UTF-8"),
				)
				break
			except RequestsConnectionError:
				logging.debug("Retrying http post request to datastore")
				if i == 2:
					raise
				continue
		assert req.status_code == 200
		assert PyBytes_AsStringAndSize(req.content, &data_ptr, &pysize) != -1
		element = parser.parse(data_ptr, pysize, 1)
		if (element.at("found").error() == SUCCESS):
			res.update(toEntityStructure(element.at_key("found"), isInitial=True))
		if (element.at("deferred").error() == SUCCESS):
			keyList = toPythonStructure(element.at_key("deferred")) + keyList[300:]
		else:
			keyList = keyList[300:]
	return [res.get(x) for x in keys]  # Sort by order of incoming keys


def pythonPropToJson(v):
	if v is True or v is False:
		return {
			"booleanValue": v
		}
	elif isinstance(v, int):
		return {
			"integerValue": str(v)
		}
	elif isinstance(v, float):
		return {
			"doubleValue":v
		}
	elif isinstance(v, str):
		return {
			"stringValue": v
		}
	elif isinstance(v, Key):
		return {
			"keyValue": {
				"partitionId": {
					"project_id": projectID,
				},
				"path": keyToPath(v)
			}
		}
	elif isinstance(v, datetime):
		return {
			"timestampValue" : v.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
		}
	elif isinstance(v, Entity):
		return {
			"key": pythonPropToJson(v.key)["keyValue"],
			"properties": {
				dictKey: pythonPropToJson(dictValue) for dictKey, dictValue in v.items()
			}
		}
	elif isinstance(v, list):
		return {
			"arrayValue": {
				"values": [pythonPropToJson(x) for x in v]
			}
		}
	print(v)
	assert False


def deleteMulti(keys: List[Key]):
	cdef simdjsonParser parser = simdjsonParser()
	cdef Py_ssize_t pysize
	cdef char * data_ptr
	cdef simdjsonElement element
	cdef simdjsonArray arrayElem
	postData = {
		"mode": "NON_TRANSACTIONAL", #"TRANSACTIONAL", #
		"mutations": [
			{
				"delete": pythonPropToJson(x)["keyValue"]
			}
			for x in keys
		]
	}
	currentTxn = currentTransaction.get()
	if currentTxn:
		currentTxn["mutations"].extend(postData["mutations"])
		return
	req = _http_internal.post(
		url="https://datastore.googleapis.com/v1/projects/%s:commit" % projectID,
		data=json.dumps(postData).encode("UTF-8"),
	)
	if req.status_code != 200:
		print("INVALID STATUS CODE RECEIVED")
		print(req.content)
		pprint.pprint(json.loads(req.content))
		raise ValueError("Invalid status code received from Datastore API")
	else:
		assert PyBytes_AsStringAndSize(req.content, &data_ptr, &pysize) != -1
		element = parser.parse(data_ptr, pysize, 1)
		if (element.at("mutationResults").error() != SUCCESS):
			print(req.content)
			raise ValueError("No mutation-results received")
		arrayElem = element.at_key("mutationResults").get_array()
		if arrayElem.size() != len(keys):
			print(req.content)
			raise ValueError("Invalid number of mutation-results received")


def putMulti(entities):
	cdef simdjsonParser parser = simdjsonParser()
	cdef Py_ssize_t pysize
	cdef char * data_ptr
	cdef simdjsonElement element, innerArrayElem
	cdef simdjsonArray arrayElem
	cdef simdjsonArray.iterator arrayIt

	postData = {
		"mode": "NON_TRANSACTIONAL", # Always NON_TRANSACTIONAL; if we're inside a transaction we'll abort below
		"mutations": [
			{
				"upsert": pythonPropToJson(x)
			}
			for x in entities
		]
	}
	currentTxn = currentTransaction.get()
	if currentTxn:  # We're currently inside a transaction, just queue the changes
		currentTxn["mutations"].extend(postData["mutations"])
		return
	req = _http_internal.post(
		url="https://datastore.googleapis.com/v1/projects/%s:commit" % projectID,
		data=json.dumps(postData).encode("UTF-8"),
	)
	if req.status_code != 200:
		print("INVALID STATUS CODE RECEIVED")
		print(req.content)
		pprint.pprint(json.loads(req.content))
		raise ValueError("Invalid status code received from Datastore API")
	else:
		assert PyBytes_AsStringAndSize(req.content, &data_ptr, &pysize) != -1
		element = parser.parse(data_ptr, pysize, 1)
		if (element.at("mutationResults").error() != SUCCESS):
			print(req.content)
			raise ValueError("No mutation-results received")
		arrayElem = element.at_key("mutationResults").get_array()
		if arrayElem.size() != len(entities):
			print(req.content)
			raise ValueError("Invalid number of mutation-results received")
		arrayIt = arrayElem.begin()
		idx = 0
		while arrayIt != arrayElem.end():
			innerArrayElem = dereference(arrayIt)
			if innerArrayElem.at("key").error() == SUCCESS:  # We got a new key assigned
				entities[idx].key = parseKey(innerArrayElem.at_key("key"))
			entities[idx].version = toPyStr(innerArrayElem.at_key("version").get_string())
			preincrement(arrayIt)
			idx += 1
	return entities

def runInTransaction(callback, *args, **kwargs):
	oldTxn = currentTransaction.get()
	allowOverriding = kwargs.pop("__allowOverriding__", None)
	nestingIndex = kwargs.pop("__nestingIndex__", 0)
	if oldTxn:
		if not allowOverriding:
			raise RecursionError("Cannot call runInTransaction while inside a transaction!")
		txnOptions = {
			"previousTransaction": oldTxn["key"]
		}
	else:
		txnOptions = {}
	postData = {
		"transactionOptions": {
			"readWrite": txnOptions
		}
	}
	req = _http_internal.post(
		url="https://datastore.googleapis.com/v1/projects/%s:beginTransaction" % projectID,
		data=json.dumps(postData).encode("UTF-8"),
	)
	if req.status_code != 200:
		print("INVALID STATUS CODE RECEIVED")
		print(req.content)
		pprint.pprint(json.loads(req.content))
		raise ValueError("Invalid status code received from Datastore API")
	else:
		txnKey = json.loads(req.content)["transaction"]
		try:
			currentTxn = {"key": txnKey, "mutations": []}
			currentTransaction.set(currentTxn)
			try:
				res = callback(*args, **kwargs)
			except:
				print("EXCEPTION IN CALLBACK")
				_rollbackTxn(txnKey)
				raise
			if currentTxn["mutations"]:
				# Commit TXN
				postData = {
					"mode":"TRANSACTIONAL", #
					"transaction": txnKey,
					"mutations": currentTxn["mutations"]
				}
				req = _http_internal.post(
					url="https://datastore.googleapis.com/v1/projects/%s:commit" % projectID,
					data=json.dumps(postData).encode("UTF-8"),
				)
				if req.status_code != 200:
					print("INVALID STATUS CODE RECEIVED")
					print(req.content)
					pprint.pprint(json.loads(req.content))
					raise ValueError("Invalid status code received from Datastore API")
				else:
					return res
			else:  # No changes have been made - free txn
				_rollbackTxn(txnKey)
		finally:  # Ensure, currentTransaction is always set back to none
			currentTransaction.set(None)

def _rollbackTxn(txnKey):
	postData = {
		"transaction": txnKey
	}
	req = _http_internal.post(
		url="https://datastore.googleapis.com/v1/projects/%s:rollback" % projectID,
		data=json.dumps(postData).encode("UTF-8"),
	)


def runSingleFilter(queryDefinition, limit):
	cdef simdjsonParser parser = simdjsonParser()
	cdef Py_ssize_t pysize
	cdef char * data_ptr
	cdef simdjsonElement element
	res = []
	internalStartCursor = None  # Will be set if we need to fetch more than one batch
	currentTxn = currentTransaction.get()
	if currentTxn:
		readOptions = {"transaction": currentTxn["key"]}
	else:
		readOptions = {"readConsistency": "STRONG"}
	while True:  # We might need to fetch more than one batch
		postData = {
			"partitionId": {
				"project_id": projectID,
			},
			"readOptions": readOptions,
			"query": {
				"kind": [
					{
						"name": queryDefinition.kind,
					}
				],
				"limit": limit
			},
		}
		if queryDefinition.filters:
			filterList = []
			for k, v in queryDefinition.filters.items():
				key, op = k.split(" ")
				if op == "=":
					op = "EQUAL"
				elif op == "<":
					op = "LESS_THAN"
				elif op == "<=":
					op = "LESS_THAN_OR_EQUAL"
				elif op == ">":
					op = "GREATER_THAN"
				elif op == ">=":
					op = "GREATER_THAN_OR_EQUAL"
				else:
					raise ValueError("Invalid op %s" % op)
				filterList.append({
					"propertyFilter": {
						"property": {
							"name": key,
						},
						"op": op,
						"value": pythonPropToJson(v)
					}
				}
				)
			if len(filterList) == 1:  # Special, single filter
				postData["query"]["filter"] = filterList[0]
			else:
				postData["query"]["filter"] = {
					"compositeFilter": {
						"op": "AND",
						"filters": filterList
					}
				}
		if queryDefinition.orders:
			postData["query"]["order"] = [
				{
					"property": {"name": sortOrder[0]},
					"direction": "ASCENDING" if sortOrder[1].value in [1, 4] else "DESCENDING"
				} for sortOrder in queryDefinition.orders
			]
		if queryDefinition.distinct:
			postData["query"]["distinctOn"] = [
				{
					"name": distinctKey
				} for distinctKey in queryDefinition.distinct
			]
		if queryDefinition.startCursor or internalStartCursor:
			postData["query"]["startCursor"] = internalStartCursor or queryDefinition.startCursor
		if queryDefinition.endCursor:
			postData["query"]["endCursor"] = queryDefinition.endCursor
		req = _http_internal.post(
			url="https://datastore.googleapis.com/v1/projects/%s:runQuery" % projectID,
			data=json.dumps(postData).encode("UTF-8"),
		)
		if req.status_code != 200:
			print("INVALID STATUS CODE RECEIVED")
			print(req.content)
			pprint.pprint(json.loads(req.content))
			raise ValueError("Invalid status code received from Datastore API")
		assert PyBytes_AsStringAndSize(req.content, &data_ptr, &pysize) != -1
		element = parser.parse(data_ptr, pysize, 1)
		if element.at("batch").error() != SUCCESS:
			print("INVALID RESPONSE RECEIVED")
			pprint.pprint(json.loads(req.content))
		#	res.update(toEntityStructure(element.at_key("batch"), isInitial=True))
		element = element.at_key("batch")
		if element.at("entityResults").error() == SUCCESS:
			res.extend(toEntityStructure(element.at_key("entityResults"), isInitial=False))
		else: # No results received
			break
		if toPyStr(element.at_key("moreResults").get_string()) != "NOT_FINISHED":
			break
		internalStartCursor = toPyStr(element.at_key("endCursor").get_string())
	return res


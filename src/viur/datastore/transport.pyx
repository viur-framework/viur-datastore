# distutils: sources = src/viur/datastore/simdjson.cpp
# distutils: language = c++
# cython: language_level=3
import base64

import google.auth
import requests
from libcpp cimport bool as boolean_type
from viur.datastore.types import currentTransaction, Entity, Key, QueryDefinition, currentDbAccessLog
from viur.datastore.config import conf
from viur.datastore.errors import *
from cython.operator cimport preincrement, dereference
from libc.stdint cimport int64_t, uint64_t
from datetime import datetime, timezone
from cpython.bytes cimport PyBytes_AsStringAndSize
import pprint, json
from base64 import b64decode, b64encode
from typing import Union, List, Any
from requests.exceptions import ConnectionError as RequestsConnectionError
import logging
from time import sleep


## Start of CPP-Imports required for the simdjson->python bridge

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
		boolean_type get_bool() except +
		int64_t get_int64() except +
		uint64_t get_uint64() except +
		double get_double() except +
		stringView get_string() except +
		simdjsonArray get_array() except +
		simdjsonObject get_object() except +
		simdjsonElement at_key(const char *) except +  # Raises if key not found
		simdjsonResult at_pointer(const char *)  # Same as at_key - sets result error code if key not found

	cdef cppclass simdjsonParser "simdjson::dom::parser":
		simdjsonParser()
		simdjsonElement parse(const char * buf, size_t len, boolean_type realloc_if_needed) except +

## End of C-Imports


credentials, projectID = google.auth.default(scopes=["https://www.googleapis.com/auth/datastore"])
_http_internal = google.auth.transport.requests.AuthorizedSession(
	credentials,
	refresh_timeout=300,
)

def authenticatedRequest(url:str, data: bytes) -> requests.Response:
	"""
		Runs one http request to the datastore rest api, authenticated with the current projects service account.
		Will retry up to three times in case the connection cannot be established the first time.

		:param url: The url to post the data to
		:param data:: The data to include in the post request
		:return: The Response object
	"""
	for i in range(0, 3):
		try:
			return _http_internal.post(
				url=url,
				data=data,
			)
		except RequestsConnectionError:
			logging.debug("Retrying http post request to datastore")
			if i == 2:
				raise
			continue

def keyToPath(key: Key) -> List[dict]:
	"""
		Converts a Key object to the PathElements expected by the rest API.
		See https://cloud.google.com/datastore/docs/reference/data/rest/v1/Key#PathElement

		:param key: The key object to convert
		:return: The list of path elements corresponding to this key
	"""
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

def pythonPropToJson(v) -> dict:
	"""
		Convert a python type to the object representation expected by the datastore rest API.
		See https://cloud.google.com/datastore/docs/reference/data/rest/v1/projects/runQuery#Value

		:param v: The python object to convert
		:return: The representation of that object as expected by the rest api.
	"""
	if v is True or v is False:
		return {
			"booleanValue": v
		}
	elif v is None:
		return {"nullValue": None}
	elif isinstance(v, int):
		return {
			"integerValue": str(v)
		}
	elif isinstance(v, float):
		return {
			"doubleValue": v
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
			"timestampValue": v.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
		}
	elif isinstance(v, Entity):
		excludedProperties: set = set(v.exclude_from_indexes)
		properties = {}
		for dictKey, dictValue in v.items():
			if isinstance(dictValue, list) and dictKey in excludedProperties:
				# Lists cannot be not indexed (but each of its value can be). So we have to forward our not-indexed
				# flag to our children. See https://github.com/googleapis/google-cloud-node/issues/2615.
				resList = []
				for value in dictValue:
					resDict = pythonPropToJson(value)
					resDict["excludeFromIndexes"] = True
					resList.append(resDict)
				properties[dictKey] = {
					"arrayValue": {
						"values": resList
					}
				}
			else:
				resDict = pythonPropToJson(dictValue)
				if dictKey in excludedProperties:
					resDict["excludeFromIndexes"] = True
				properties[dictKey] = resDict
		return {
			"entityValue": {
				"key": pythonPropToJson(v.key)["keyValue"] if isinstance(v, Entity) and v.key else None,
				"properties": properties
			}
		}
	elif isinstance(v, dict):
		# We hande dicts separately as they don't have keys nor can keys be excluded from being indexed
		return {
			"entityValue": {
				"key": None,
				"properties": {
					dictKey: pythonPropToJson(dictValue) for dictKey, dictValue in v.items()
				}
			}
		}
	elif isinstance(v, list):
		return {
			"arrayValue": {
				"values": [pythonPropToJson(x) for x in v]
			}
		}
	elif isinstance(v, bytes):
		return {
			"blobValue": b64encode(v).decode("ASCII")
		}
	assert False, "%s (%s) is not supported" % (v, type(v))

cdef inline object toPyStr(stringView strView):
	"""
		Converts a cpp stringview to a python str object
		:param strView: The stringview object
		:return: The corresponding python string object
	"""
	return PyUnicode_FromStringAndSize(
		strView.data(),
		strView.length()
	)

cdef inline object parseKey(simdjsonElement v):
	"""
		Parses a simdJsonObject representing a key to a datastore.Key instance.

		:param v: The simdJsonElement containing the key
		:return: The corresponding datastore.Key instance
	"""
	cdef simdjsonArray arr
	cdef simdjsonArray.iterator arrayIt, arrayItEnd
	pathArgs = []
	arr = v.at_key("path").get_array()
	arrayIt = arr.begin()
	arrayItEnd = arr.end()
	while arrayIt != arrayItEnd:
		element = dereference(arrayIt)
		if element.at_pointer("/id").error() == SUCCESS:
			pathArgs.append(
				(toPyStr(element.at_key("kind").get_string()), int(toPyStr(element.at_key("id").get_string())))
			)
		elif element.at_pointer("/name").error() == SUCCESS:
			pathArgs.append(
				(toPyStr(element.at_key("kind").get_string()), toPyStr(element.at_key("name").get_string()))
			)
		else:
			# We read an incomplete/null key from the datastore. This is likely a bug.
			# Can happen if you manually created a key like datastore.Key("kind", 0).
			logging.error("We read an incomplete/null key from the datastore. This is likely a bug!")
			pathArgs.append((toPyStr(element.at_key("kind").get_string()), 0))
		preincrement(arrayIt)
	key = None
	for pathElem in pathArgs:
		key = Key(*pathElem, parent=key)
	return key

cdef inline object toEntityStructure(simdjsonElement v, boolean_type isInitial = False):
	"""
		Parses a simdJsonElement into the corresponding python datatypes.
		:param v: The simdJsonElement to parse
		:param isInitial: If true, we'll return a dictionary of key->entity instead of a list of entities
		:return: The corresponding python datatype.

		TODO: While working for now, this should probably refactored to return the parsed value and it's index flag
			to avoid additional loops over objects.
	"""
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
				tmpResult = objIterStart.value().at_pointer("/key")
				if tmpResult.error() == SUCCESS:
					e.key = parseKey(tmpResult.value())
				tmpResult = objIterStart.value().at_pointer("/properties")
				if tmpResult.error() == SUCCESS:
					innerObject = tmpResult.value().get_object()
					objIterStartInner = innerObject.begin()
					objIterEndInner = innerObject.end()
					while objIterStartInner != objIterEndInner:
						e[toPyStr(objIterStartInner.key())] = toEntityStructure(objIterStartInner.value())
						if objIterStartInner.value().at_pointer("/arrayValue/values").error() == SUCCESS:
							# We have to collect the non-indexed flag from the children of lists
							arr = objIterStartInner.value().at_pointer("/arrayValue/values").value().get_array()
							arrayIt = arr.begin()
							arrayItEnd = arr.end()
							allExcluded = True
							while arrayIt != arrayItEnd:
								element = dereference(arrayIt)
								tmpResult = element.at_pointer("/excludeFromIndexes")
								if tmpResult.error() != SUCCESS or not tmpResult.value().get_bool():
									allExcluded = False
									break
								preincrement(arrayIt)
							if allExcluded:
								excludeList.add(toPyStr(objIterStartInner.key()))
						else:
							# For *all* other datatypes, we can simply check the dict it's defined in
							tmpResult = objIterStartInner.value().at_pointer("/excludeFromIndexes")
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
				tmpResult = objIterStart.value().at_pointer("/values")
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

def runSingleFilter(queryDefinition: QueryDefinition, limit: int) -> List[Entity]:
	"""
		Runs a single Query as defined by queryDefinition. The limit of the queryDefinition is ignored and must
		be specified separately to prevent _calculateInternalMultiQueryLimit from modifying the queryDefinition.

		:param queryDefinition: The query to run
		:param limit:  How many entities to return at maximum
		:return: The list of entities fetched from the datastore
	"""
	cdef simdjsonParser parser = simdjsonParser()
	cdef Py_ssize_t pysize
	cdef char * data_ptr
	cdef simdjsonElement element
	res = []
	internalStartCursor = None  # Will be set if we need to fetch more than one batch
	flipResults = False  # If set, we'll reverse the list returned (Sortorder was Inverted*)
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
				"limit": limit - len(res)
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
				if not isinstance(v, list):
					# An entity can have a list of values for a single property, so it's possible to enforce
					# more an one constraint for a a single property (e.g. x==5 and x==7 can be true), so
					# enforce we always have a list here
					v = [v]
				for singleValue in v:
					filterList.append({
						"propertyFilter": {
							"property": {
								"name": key,
							},
							"op": op,
							"value": pythonPropToJson(singleValue)
						}
					})
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
			flipResults = queryDefinition.orders[0][1].value > 2  # Either InvertedAscending or InvertedDescending
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
		req = authenticatedRequest(
			url="https://datastore.googleapis.com/v1/projects/%s:runQuery" % projectID,
			data=json.dumps(postData).encode("UTF-8"),
		)

		is_viur_datastore_request_ok(req)
		assert PyBytes_AsStringAndSize(req.content, &data_ptr, &pysize) != -1
		element = parser.parse(data_ptr, pysize, 1)
		if element.at_pointer("/batch").error() != SUCCESS:
			print("INVALID RESPONSE RECEIVED")
			pprint.pprint(json.loads(req.content))
		#	res.update(toEntityStructure(element.at_key("batch"), isInitial=True))
		element = element.at_key("batch")
		if element.at_pointer("/entityResults").error() == SUCCESS:
			res.extend(toEntityStructure(element.at_key("entityResults"), isInitial=False))
		else:  # No results received
			break
		if toPyStr(element.at_key("moreResults").get_string()) == "NO_MORE_RESULTS":
			internalStartCursor = None
		else:
			internalStartCursor = toPyStr(element.at_key("endCursor").get_string())
		if toPyStr(element.at_key("moreResults").get_string()) != "NOT_FINISHED" or len(res) == limit:
			break
	queryDefinition.currentCursor = internalStartCursor
	if conf["traceQueries"]:
		orders = queryDefinition.orders
		filters = queryDefinition.filters
		distinctOn = " distinct on %s" % str(queryDefinition.distinct) if queryDefinition.distinct else ""
		logging.debug("Queried %s with filter %s and orders %s%s. Returned %s results" % (
				queryDefinition.kind, filters, orders, distinctOn, len(res)))
	if flipResults:
		return res[::-1]
	return res

def Get(keys: Union[Key, List[Key]]) -> Union[None, Entity, List[Entity]]:
	"""
		Fetches the entities determined by keys from the datastore. Returns or inserts None if a key is not found.
		:param keys: A Key or a List of Keys to fetch
		:return: The entity or None for the given key, a list of Entities/None if a list has been supplied
	"""
	cdef simdjsonParser parser = simdjsonParser()
	cdef Py_ssize_t pysize
	cdef char * data_ptr
	cdef simdjsonElement element
	isMulti = True
	if isinstance(keys, Key):
		keys = [keys]
		isMulti = False
	accessLog = currentDbAccessLog.get()
	if isinstance(accessLog, set):
		accessLog.update(set(keys))
	keyList = [
		{
			"partitionId": {
				"project_id": projectID,
			},
			"path": keyToPath(x)
		}
		for x in keys]

	currentTxn = currentTransaction.get()
	if currentTxn:
		readOptions = {"transaction": currentTxn["key"]}
	else:
		readOptions = {"readConsistency": "STRONG"}
	res = {}
	while keyList:
		requestedKeys = keyList[:300]
		postData = {
			"readOptions": readOptions,
			"keys": requestedKeys,
		}
		req = authenticatedRequest(
			url="https://datastore.googleapis.com/v1/projects/%s:lookup" % projectID,
			data=json.dumps(postData).encode("UTF-8"),
		)
		assert req.status_code == 200
		assert PyBytes_AsStringAndSize(req.content, &data_ptr, &pysize) != -1
		element = parser.parse(data_ptr, pysize, 1)
		if (element.at_pointer("/found").error() == SUCCESS):
			res.update(toEntityStructure(element.at_key("found"), isInitial=True))
		if (element.at_pointer("/deferred").error() == SUCCESS):
			keyList = toPythonStructure(element.at_key("deferred")) + keyList[300:]
		else:
			keyList = keyList[300:]
	if not isMulti:
		return res.get(keys[0])
	else:
		return [res.get(x) for x in keys]  # Sort by order of incoming keys

def Delete(keys: Union[Key, List[Key], Entity, List[Entity]]) -> None:
	"""
		Deletes the entities stored under the given key(s).
		If a key is not found, it's silently ignored.
		A maximum of 300 Keys can be deleted at once.

		:param keys: A Key or a List of Keys
	"""
	cdef simdjsonParser parser = simdjsonParser()
	cdef Py_ssize_t pysize
	cdef char * data_ptr
	cdef simdjsonElement element
	cdef simdjsonArray arrayElem
	if isinstance(keys, Key):
		keys = [keys]
	elif isinstance(keys, Entity):
		keys = [keys.key]
	keys = [(x.key if isinstance(x, Entity) else x) for x in keys]
	accessLog = currentDbAccessLog.get()
	if isinstance(accessLog, set):
		accessLog.update(set(keys))
	if not keys:  # We got an empty list (probably a query that returned no results), noting to do here
		return
	postData = {
		"mode": "NON_TRANSACTIONAL",  #"TRANSACTIONAL", #
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
		# Insert placeholders into affectedEntities as we receive a mutation-result for each key deleted
		currentTxn["affectedEntities"].extend([None] * len(keys))
		return
	req = authenticatedRequest(
		url="https://datastore.googleapis.com/v1/projects/%s:commit" % projectID,
		data=json.dumps(postData).encode("UTF-8"),
	)
	if is_viur_datastore_request_ok(req):
		assert PyBytes_AsStringAndSize(req.content, &data_ptr, &pysize) != -1
		element = parser.parse(data_ptr, pysize, 1)
		if (element.at_pointer("/mutationResults").error() != SUCCESS):
			pprint(req.content)
			raise NoMutationResultsError("No mutation results received")
		arrayElem = element.at_key("mutationResults").get_array()
		if arrayElem.size() != abs(len(keys)):
			print(req.content)
			raise ValueError("Invalid number of mutation-results received")

def Put(entities: Union[Entity, List[Entity]]) -> Union[Entity, List[Entity]]:
	"""
		Writes the given entities into the datastore. The entities can be from different kinds. If an entity has an
		complete key, and there's already an entity stored under the given key, it's overwritten.

		:param entities: The entities to store
		:return: The Entity or List of Entities as supplied, with partial keys replaced by full ones (unless called
			inside a transaction, in which case we return None as no Keys have been determined yet)
	"""
	cdef simdjsonParser parser = simdjsonParser()
	cdef Py_ssize_t pysize
	cdef char * data_ptr
	cdef simdjsonElement element, innerArrayElem
	cdef simdjsonArray arrayElem
	cdef simdjsonArray.iterator arrayIt
	if isinstance(entities, Entity):
		entities = [entities]
	accessLog = currentDbAccessLog.get()
	if isinstance(accessLog, set):
		accessLog.update(set([x.key for x in entities if not x.key.is_partial]))
	postData = {
		"mode": "NON_TRANSACTIONAL",  # Always NON_TRANSACTIONAL; if we're inside a transaction we'll abort below
		"mutations": [
			{
				"upsert": pythonPropToJson(x)["entityValue"]
			}
			for x in entities
		]
	}
	currentTxn = currentTransaction.get()
	if currentTxn:  # We're currently inside a transaction, just queue the changes
		currentTxn["mutations"].extend(postData["mutations"])
		currentTxn["affectedEntities"].extend(entities)
		return
	req = authenticatedRequest(
		url="https://datastore.googleapis.com/v1/projects/%s:commit" % projectID,
		data=json.dumps(postData).encode("UTF-8"),
	)

	if is_viur_datastore_request_ok(req):
		assert PyBytes_AsStringAndSize(req.content, &data_ptr, &pysize) != -1
		element = parser.parse(data_ptr, pysize, 1)
		if (element.at_pointer("/mutationResults").error() != SUCCESS):
			print(req.content)
			raise ValueError("No mutation-results received")
		arrayElem = element.at_key("mutationResults").get_array()
		if arrayElem.size() != abs(len(entities)):
			pprint(req.content)
			raise NoMutationResultsError("Invalid number of mutation-results received")
		arrayIt = arrayElem.begin()
		idx = 0
		while arrayIt != arrayElem.end():
			innerArrayElem = dereference(arrayIt)
			if innerArrayElem.at_pointer("/key").error() == SUCCESS:  # We got a new key assigned
				entities[idx].key = parseKey(innerArrayElem.at_key("key"))
			entities[idx].version = toPyStr(innerArrayElem.at_key("version").get_string())
			preincrement(arrayIt)
			idx += 1
	return entities


def RunInTransaction(callback: callable, *args, **kwargs) -> Any:
	"""
		Runs the given function inside a AID transaction.

		:param callback: The function to run inside a transaction
		:param args: Args to pass to the function
		:param kwargs: Kwargs to pass to the function
		:return: The return-value of the callback function
	"""
	cdef simdjsonParser parser = simdjsonParser()
	cdef Py_ssize_t pysize
	cdef char * data_ptr
	cdef simdjsonElement element, innerArrayElem
	cdef simdjsonArray arrayElem
	cdef simdjsonArray.iterator arrayIt
	for exponential_backoff in range(1, 4):
		try:
			oldTxn = currentTransaction.get()
			allowOverriding = kwargs.pop("__allowOverriding__", None)
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
			req = authenticatedRequest(
				url="https://datastore.googleapis.com/v1/projects/%s:beginTransaction" % projectID,
				data=json.dumps(postData).encode("UTF-8"),
			)
			if is_viur_datastore_request_ok(req):
				txnKey = json.loads(req.content)["transaction"]
				try:
					currentTxn = {"key": txnKey, "mutations": [], "affectedEntities": []}
					currentTransaction.set(currentTxn)
					try:
						res = callback(*args, **kwargs)
					except:
						_rollbackTxn(txnKey)
						raise
					if currentTxn["mutations"]:
						# Commit TXN
						postData = {
							"mode": "TRANSACTIONAL",  #
							"transaction": txnKey,
							"mutations": currentTxn["mutations"]
						}
						req = authenticatedRequest(
							url="https://datastore.googleapis.com/v1/projects/%s:commit" % projectID,
							data=json.dumps(postData).encode("UTF-8"),
						)

						is_viur_datastore_request_ok(req)
						assert PyBytes_AsStringAndSize(req.content, &data_ptr, &pysize) != -1
						element = parser.parse(data_ptr, pysize, 1)
						if (element.at_pointer("/mutationResults").error() != SUCCESS):
							pprint(req.content)
							raise NoMutationResultsError("No mutation-results received")
						arrayElem = element.at_key("mutationResults").get_array()
						if arrayElem.size() != abs(len(currentTxn["affectedEntities"])):
							print(req.content)
							raise ViurDatastoreError("Invalid number of mutation-results received")
						arrayIt = arrayElem.begin()
						idx = 0
						while arrayIt != arrayElem.end():
							innerArrayElem = dereference(arrayIt)
							if innerArrayElem.at_pointer("/key").error() == SUCCESS:  # We got a new key assigned
								affectedEntity = currentTxn["affectedEntities"][idx]
								if not affectedEntity:
									print(req.content)
									raise ViurDatastoreError("Received an unexpected key-update")
								affectedEntity.key = parseKey(innerArrayElem.at_key("key"))
								affectedEntity.version = toPyStr(innerArrayElem.at_key("version").get_string())
							preincrement(arrayIt)
							idx += 1
						return res
					else:  # No changes have been made - free txn
						_rollbackTxn(txnKey)
						return res
				finally:  # Ensure, currentTransaction is always set back to none
					currentTransaction.set(None)
		except CollisionError:  # Got a collision; retry the entire transaction
			sleep(2 ** exponential_backoff)
	raise CollisionError("All retries are exhausted for this transaction") # If we made it here, all tries are exhausted


def _rollbackTxn(txnKey: str):
	"""
		Internal helper that aborts the given transaction. It's important to abort pending transactions (instead
		of letting them expire server-side) so that other requests that are stalled waiting for this transaction to
		complete can continue.

		:param txnKey: The ID of the transaction that should be aborted
	"""
	postData = {
		"transaction": txnKey
	}
	req = authenticatedRequest(
		url="https://datastore.googleapis.com/v1/projects/%s:rollback" % projectID,
		data=json.dumps(postData).encode("UTF-8"),
	)

def AllocateIDs(keys: Union[Key, List[Key]]) -> Union[Key, List[Key]]:
	"""
		Allocates numeric IDs for the keys given.
		:return: The complete Key (or a list hereof)

		.. warning: This function does not support transactions! Even if called inside transactions, the keys will
			be allocated immediately, even if the transaction aborts.
	"""
	cdef simdjsonParser parser = simdjsonParser()
	cdef Py_ssize_t pysize
	cdef char * data_ptr
	cdef simdjsonElement element
	isMulti = True
	if isinstance(keys, Key):
		keys = [keys]
		isMulti = False
	keyList = [
		{
			"partitionId": {
				"project_id": projectID,
			},
			"path": keyToPath(x)
		}
		for x in keys]

	requestedKeys = keyList[:300]
	postData = {
		"keys": requestedKeys,
	}
	req = authenticatedRequest(
		url="https://datastore.googleapis.com/v1/projects/%s:allocateIds" % projectID,
		data=json.dumps(postData).encode("UTF-8"),
	)
	assert req.status_code == 200
	assert PyBytes_AsStringAndSize(req.content, &data_ptr, &pysize) != -1
	element = parser.parse(data_ptr, pysize, 1)
	res = []
	if (element.at_pointer("/keys").error() == SUCCESS):
		arrayElem = element.at_key("keys").get_array()
		arrayIt = arrayElem.begin()
		while arrayIt != arrayElem.end():
			innerArrayElem = dereference(arrayIt)
			res.append( parseKey(innerArrayElem))
			preincrement(arrayIt)
		if not res:
			raise ValueError("Empty response received from Datastore API")
		elif not isMulti:
			return res[0]
		else:
			return res
	else:
		logging.error("Invalid data received from Datastore API")
		logging.error(req.content)
		raise ValueError("Invalid data received from Datastore API")

def Count(kind: str = None, up_to= 2 ** 63 - 1, queryDefinition: QueryDefinition = None) -> Union[Key, List[Key]]:
	"""
		Count all entries in a kind if there is only a kind is provided
		Count the entries for an given query.
		:param kind name of the module (kind) we want to count
		:param up_to can be sigend int 64 bit (max positive 2^31-1)
		:param queryDefinition: The query to run
		:return: The count as an int

		.. warning: This function does not support transactions! Even if called inside transactions, the keys will
			be allocated immediately, even if the transaction aborts.
	"""
	cdef simdjsonParser parser = simdjsonParser()
	cdef Py_ssize_t pysize
	cdef char * data_ptr
	cdef simdjsonElement element
	cdef simdjsonElement element_inner
	cdef simdjsonArray array_element, array_element_inner
	logging.warning("The 'Count() aggregation' query is a technical preview and cannot be guaranteed to work at this time!!!")
	if not kind:
		kind = queryDefinition.kind

	post_data = {

		"partitionId": {
			"project_id": projectID,
		}, "aggregation_query": {
			"aggregations": {"count": {"up_to": up_to}},
			"nested_query": {"kind": [{"name": kind}]}
		},

	}
	if queryDefinition:
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
				if not isinstance(v, list):
					# An entity can have a list of values for a single property, so it's possible to enforce
					# more an one constraint for a a single property (e.g. x==5 and x==7 can be true), so
					# enforce we always have a list here
					v = [v]
				for singleValue in v:
					filterList.append({
						"propertyFilter": {
							"property": {
								"name": key,
							},
							"op": op,
							"value": pythonPropToJson(singleValue)
						}
					})
			if len(filterList) == 1:  # Special, single filter
				post_data["aggregation_query"]["nested_query"]["filter"] = filterList[0]
			else:
				post_data["aggregation_query"]["nested_query"]["filter"] = {
					"compositeFilter": {
						"op": "AND",
						"filters": filterList
					}
				}
	req = authenticatedRequest(
		url="https://datastore.googleapis.com/v1/projects/%s:runAggregationQuery" % projectID,
		data=json.dumps(post_data).encode("UTF-8"),
	)
	assert req.status_code == 200
	assert PyBytes_AsStringAndSize(req.content, &data_ptr, &pysize) != -1
	element = parser.parse(data_ptr, pysize, 1)
	if element.at_pointer("/batch").error() != SUCCESS:
		print("INVALID RESPONSE RECEIVED")
		pprint.pprint(json.loads(req.content))
	element = element.at_key("batch")
	# TODO  maybe this can be solved more elegant
	return int(toPythonStructure(element)["aggregationResults"][0]["aggregateProperties"]["property_1"]["integerValue"])

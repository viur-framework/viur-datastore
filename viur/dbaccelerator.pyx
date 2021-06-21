# distutils: sources = viur/simdjson.cpp
# distutils: language = c++
# cython: language_level=3

from libc.stdint cimport int64_t, uint64_t
from libcpp cimport bool
from cpython.bytes cimport PyBytes_AsStringAndSize
from cython.operator cimport preincrement, dereference
import google.auth
from google.auth.transport import requests  # noqa  # pylint: disable=unused-import
from google.cloud import datastore
import json
from datetime import datetime, timezone
from typing import List
from base64 import b64decode

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
	keyKwargs = {
		#FIXME: Namespace?
		"project": toPyStr(v.at_key("partitionId").at_key("projectId").get_string())
	}
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
		key = datastore.Key(*pathElem, **keyKwargs, parent=key)
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
				e = datastore.Entity()
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
			}
		)
		key = key.parent
	return res

def fetchMulti(keys: List[datastore.Key]):
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
	while keyList:
		postData = {
			"readOptions": {
				"readConsistency": "STRONG",
			},
			"keys": keyList,
		}
		req = _http_internal.post(
			url="https://datastore.googleapis.com/v1/projects/%s:lookup" % projectID,
			data=json.dumps(postData).encode("UTF-8"),
		)
		assert req.status_code == 200
		assert PyBytes_AsStringAndSize(req.content, &data_ptr, &pysize) != -1
		element = parser.parse(data_ptr, pysize, 1)
		if (element.at("found").error() == SUCCESS):
			res.update(toEntityStructure(element.at_key("found"), isInitial=True))
		if (element.at("deferred").error() == SUCCESS):
			keyList = toPythonStructure(element.at_key("deferred"))
		else:
			keyList = []
	return [res.get(x) for x in keys]  # Sort by order of incoming keys

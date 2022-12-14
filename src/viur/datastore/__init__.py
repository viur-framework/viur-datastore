from viur.datastore.config import conf as config
from viur.datastore.errors import *
from viur.datastore.query import Query
from viur.datastore.transport import AllocateIDs, Delete, Get, Put, RunInTransaction, Count
from viur.datastore.types import (
	currentDbAccessLog,
	DATASTORE_BASE_TYPES,
	Entity,
	KEY_SPECIAL_PROPERTY,
	Key,
	SortOrder,
	SkelListRef,
	QueryDefinition)
from viur.datastore.utils import (
	fixUnindexableProperties,
	normalizeKey,
	keyHelper,
	IsInTransaction,
	GetOrInsert,
	encodeKey,
	acquireTransactionSuccessMarker,
	startDataAccessLog,
	endDataAccessLog)



import logging
# silencing requests' debugging
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)


__all__ = [
	"KEY_SPECIAL_PROPERTY",
	"DATASTORE_BASE_TYPES",
	"SortOrder",
	"SkelListRef",
	"Entity",
	"QueryDefinition",
	"Key",
	"Query",
	"fixUnindexableProperties",
	"normalizeKey",
	"keyHelper",
	"Get",
	"Count",
	"Put",
	"Delete",
	"RunInTransaction",
	"IsInTransaction",
	"currentDbAccessLog",
	"GetOrInsert",
	"encodeKey",
	"acquireTransactionSuccessMarker",
	"AllocateIDs",
	"config",
	"startDataAccessLog",
	"endDataAccessLog",
	"ViurDatastoreError",
	"AbortedError",
	"CollisionError",
	"DeadlineExceededError",
	"FailedPreconditionError",
	"InternalError",
	"InvalidArgumentError",
	"NotFoundError",
	"PermissionDeniedError",
	"ResourceExhaustedError",
	"UnauthenticatedError",
	"UnavailableError",
	"NoMutationResultsError",
	"is_viur_datastore_request_ok",
]

"""
The constants, global variables and container classes used in the datastore api
"""
from __future__ import annotations

import typing as t
from base64 import urlsafe_b64decode, urlsafe_b64encode
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import date, datetime, time
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple, Union

import google.auth
from google.cloud.datastore import _app_engine_key_pb2

if t.TYPE_CHECKING:
    from viur.core.skeleton import SkeletonInstance

# The property name pointing to an entities key in a query
KEY_SPECIAL_PROPERTY = "__key__"
# List of types that can be used in a datastore query
DATASTORE_BASE_TYPES = Union[None, str, int, float, bool, datetime, date, time, 'Key']  #
# Pointer to the current transaction this thread may be currently in
currentTransaction = ContextVar("CurrentTransaction", default=None)
# If set to a set for the current thread/request, we'll log all entities / kinds accessed
currentDbAccessLog: ContextVar[Optional[Set[Union[Key, str]]]] = ContextVar("Database-Accesslog", default=None)
# The current projectID, which can't be imported from transport.pyx
_, projectID = google.auth.default(scopes=["https://www.googleapis.com/auth/datastore"])


class SortOrder(Enum):
    Ascending = 1  # Sort A->Z
    Descending = 2  # Sort Z->A
    InvertedAscending = 3  # Fetch Z->A, then flip the results (useful in pagination to go from a start cursor backwards)
    InvertedDescending = 4  # Fetch A->Z, then flip the results (useful in pagination)


class SkelListRef(list):
    """
        This class is used to hold multiple skeletons together with other, commonly used information.

        SkelLists are returned by Skel().all()...fetch()-constructs and provide additional information
        about the data base query, for fetching additional entries.

        :ivar cursor: Holds the cursor within a query.
        :vartype cursor: str
    """

    __slots__ = ["baseSkel", "getCursor", "get_orders", "customQueryInfo", "renderPreparation"]

    def __init__(self, baseSkel: t.Optional["SkeletonInstance"] = None):
        """
            :param baseSkel: The baseclass for all entries in this list
        """
        super(SkelListRef, self).__init__()
        self.baseSkel = baseSkel or {}
        self.getCursor = lambda: None
        self.get_orders = lambda: None
        self.renderPreparation = None
        self.customQueryInfo = {}


class Key:
    """
        The python representation of one datastore key. Unlike the original implementation, we don't store a
        reference to the project the key lives in. This is always expected to be the current project as ViUR
        does not support accessing data in multiple projects.
    """

    __slots__ = ["id", "name", "kind", "parent"]

    def __init__(self, kind: str, subKey: Union[int, str, None] = None, parent: 'Key' = None):
        super().__init__()
        self.kind = kind
        self.id = None
        self.name = None
        if isinstance(subKey, int):
            self.id = subKey
        elif isinstance(subKey, str):
            if subKey.isdigit():
                self.id = int(subKey)
            else:
                self.name = subKey
        elif subKey is not None:
            raise ValueError(f"Invalid argument type {subKey = }")
        self.parent = parent

    @property
    def id_or_name(self) -> Union[None, str, int]:
        """
            :return: This key's id or name (or none, if this key is partial)
        """
        return self.id or self.name

    def __str__(self):
        return self.to_legacy_urlsafe().decode("ASCII")

    def __repr__(self):
        return "<viur.datastore.Key %s/%s, parent=%s>" % (self.kind, self.id_or_name, self.parent)

    def __hash__(self):
        return hash("%s.%s.%s" % (self.kind, self.id, self.name))

    def __eq__(self, other):
        return isinstance(other, Key) and self.kind == other.kind and self.id == other.id and self.name == other.name \
            and self.parent == other.parent

    def to_legacy_urlsafe(self) -> bytes:
        """
            Converts this key into the (urlsafe) protobuf string representation.
            :return: The urlsafe string representation of this key
        """
        currentKey = self
        pathElements = []
        while currentKey:
            pathElements.insert(0, _app_engine_key_pb2.Path.Element(
                type=currentKey.kind,
                id=currentKey.id,
                name=currentKey.name,
            ))
            currentKey = currentKey.parent
        reference = _app_engine_key_pb2.Reference(
            app=projectID,
            path=_app_engine_key_pb2.Path(element=pathElements),
        )
        raw_bytes = reference.SerializeToString()
        return urlsafe_b64encode(raw_bytes).strip(b"=")

    @property
    def is_partial(self) -> bool:
        """
            Checks if this key is partial (ie it belongs to an entity that has not been saved to the datastore).
            If the entity is saved, this key will be replaced by a full key (having an id or a name assigned)
            :return: True if this key is partial
        """
        return self.id_or_name is None

    @classmethod
    def from_legacy_urlsafe(cls, strKey: str) -> Key:
        """
            Parses the string representation generated by :meth:to_legacy_urlsafe into a new Key object
            :param strKey: The string key to parse
            :return: The new Key object constructed from the string key
        """
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


class Entity(dict):
    """
        The python representation of one datastore entity. The values of this entity are stored inside this dictionary,
        while the meta-data (it's key, the list of properties excluded from indexing and our version) as property values.
    """
    __slots__ = ["key", "_exclude_from_indexes", "version"]

    def __init__(self, key: Optional[Key] = None, exclude_from_indexes: Optional[Set[str]] = None):
        super(Entity, self).__init__()
        assert not key or isinstance(key, Key), "Key must be a Key-Object (or None for an embedded entity)"
        self.key = key
        self.exclude_from_indexes = exclude_from_indexes or set()
        assert isinstance(self.exclude_from_indexes, set)
        self.version = None

    @property
    def exclude_from_indexes(self) -> set[str]:
        return self._exclude_from_indexes

    @exclude_from_indexes.setter
    def exclude_from_indexes(self, value: set[str] | list[str] | tuple[str]) -> None:
        self._exclude_from_indexes = set(value)


@dataclass
class QueryDefinition:
    """
        A single Query that will be run against the datastore.
    """
    kind: Optional[str]  # The datastore kind to run the query on. Can be None for kindles queries.
    filters: Dict[str, DATASTORE_BASE_TYPES]  # A dictionary of constrains to apply to the query.
    orders: List[Tuple[str, SortOrder]]  # The list of fields to sort the results by.
    distinct: Union[None, List[str]] = None  # If set, a list of fields that we should return distinct values of
    limit: int = 30  # The maximum amount of entities that should be returned
    startCursor: Optional[str] = None  # If set, we'll only return entities that appear after this cursor in the index.
    endCursor: Optional[str] = None  # If set, we'll only return entities up to this cursor in the index.
    currentCursor: Optional[
        str] = None  # Will be set after this query has been run, pointing after the last entity returned

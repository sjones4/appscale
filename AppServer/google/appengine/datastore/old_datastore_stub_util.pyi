from typing import Callable, List, Text, Tuple
from google.appengine.datastore import datastore_v3_pb2

def _GetScatterProperty(entity_proto : datastore_v3_pb2.EntityProto) -> datastore_v3_pb2.Property:
    ...

def GetInvisibleSpecialPropertyNames() -> List[object]:
    ...

def _PrepareSpecialProperties(entity_proto : datastore_v3_pb2.EntityProto, is_load : bool) -> None:
    ...


def PrepareSpecialPropertiesForStore(entity_proto : datastore_v3_pb2.EntityProto) -> None:
    ...

def PrepareSpecialPropertiesForLoad(entity_proto : datastore_v3_pb2.EntityProto) -> None:
    ...

def ValidateQuery(query : datastore_v3_pb2.Query,
                  filters : List[datastore_v3_pb2.Query.Filter],
                  orders : List[datastore_v3_pb2.Query.Order],
                  max_query_components : int) -> None:
    ...

class ValueRange(object):
    def Update(self : ValueRange,
               rel_op : datastore_v3_pb2.Query.Filter.Operator,
               limit : object) -> None:
        ...

    def Contains(self : ValueRange, value : int) -> bool:
        ...

    def Remap(self : ValueRange, mapper : Callable[[object], object]) -> None:
        ...

    def MapExtremes(self : ValueRange, mapper : Callable[[int, bool, bool], int]) -> None:
        ...


def ParseKeyFilteredQuery(filters : List[datastore_v3_pb2.Query.Filter],
                          orders : List[datastore_v3_pb2.Query.Order]) -> ValueRange:
    ...

def ParseKindQuery(query : datastore_v3_pb2.Query,
                   filters : List[datastore_v3_pb2.Query.Filter],
                   orders : List[datastore_v3_pb2.Query.Order]) -> ValueRange:
    ...

def _KindKeyToString(key : object) -> Text:
    ...

def ParseNamespaceQuery(query : datastore_v3_pb2.Query,
                        filters : List[datastore_v3_pb2.Query.Filter],
                        orders : List[datastore_v3_pb2.Query.Order]) -> ValueRange:
    ...

def _NamespaceKeyToString(key : object) -> str:
    ...

def ParsePropertyQuery(query : datastore_v3_pb2.Query,
                       filters : List[datastore_v3_pb2.Query.Filter],
                       orders : List[datastore_v3_pb2.Query.Order]) -> ValueRange:
    ...

def _PropertyKeyToString(key : object, default_property : object) -> object:
    ...

def SynthesizeUserId(email : Text) -> Text:
    ...

def FillUsersInQuery(filters: List[datastore_v3_pb2.Query.Filter]) -> None:
    ...

def FillUser(property : datastore_v3_pb2.Property):
    ...

class BaseCursor(object):
    def PopulateCursor(self : BaseCursor, query_result : datastore_v3_pb2.QueryResult):
        ...

    @classmethod
    def _AcquireCursorID(cls : object) -> object:
        ...

class ListCursor(BaseCursor):
    @staticmethod
    def _GetCursorOffset(results : List[datastore_v3_pb2.EntityProto],
                         cursor_entity : datastore_v3_pb2.EntityProto,
                         inclusive : bool,
                         compare : Callable[[datastore_v3_pb2.EntityProto,datastore_v3_pb2.EntityProto], int]) -> int:
        ...

    def _ValidateQuery(self : ListCursor,
                       query : datastore_v3_pb2.Query,
                       query_info : datastore_v3_pb2.Query) -> None:
        ...

    def _MinimalQueryInfo(self : ListCursor,
                          query : datastore_v3_pb2.Query) -> datastore_v3_pb2.Query:
        ...

    def _MinimalEntityInfo(self : ListCursor,
                           entity_proto : datastore_v3_pb2.EntityProto,
                           query : datastore_v3_pb2.Query) -> datastore_v3_pb2.EntityProto:
        ...

    def _DecodeCompiledCursor(self : ListCursor,
                              query : datastore_v3_pb2.Query,
                              compiled_cursor : datastore_v3_pb2.CompiledCursor) -> Tuple[datastore_v3_pb2.EntityProto,bool]:
        ...

    def _EncodeCompiledCursor(self : ListCursor,
                              query : datastore_v3_pb2.Query,
                              compiled_cursor : datastore_v3_pb2.CompiledCursor) -> None:
        ...

    def Count(self : ListCursor) -> int:
        ...

    def PopulateQueryResult(self : ListCursor,
                            result : datastore_v3_pb2.QueryResult,
                            count : int,
                            offset : int,
                            compile : bool) -> None:
        ...


def CompareEntityPbByKey(a : datastore_v3_pb2.EntityProto, b : datastore_v3_pb2.EntityProto) -> int:
    ...
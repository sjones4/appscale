from typing import List, Text, Tuple
from google.appengine.datastore import datastore_v3_pb2

def IndexDefinitionsToKeys(indexes: IndexDefinitions) -> Set[Tuple[Text,bool,Tuple[Text,datastore_v3_pb2.Query.Order.Direction]]]:
    ...

def IndexToKey(index: Index) -> (Text, bool, Tuple[Text,datastore_v3_pb2.Query.Order.Direction]):
    ...

def Normalize(filters : List[datastore_v3_pb2.Query.Filter],
              orders : List[datastore_v3_pb2.Query.Order],
              exists : List[str]) \
        -> (List[datastore_v3_pb2.Query.Filter], List[datastore_v3_pb2.Query.Order]):
    ...

def RemoveNativelySupportedComponents(filters : List[datastore_v3_pb2.Query.Filter],
                                      orders : List[datastore_v3_pb2.Query.Order],
                                      exists : List[str]) \
        -> (List[datastore_v3_pb2.Query.Filter], List[datastore_v3_pb2.Query.Order]):
    ...

def CompositeIndexForQuery(query : datastore_v3_pb2.Query):
    ...

def GetRecommendedIndexProperties(properties):
    ...

def _MatchPostfix(postfix_props, index_props):
    ...

def MinimalCompositeIndexForQuery(query : datastore_v3_pb2.Query, index_defs : List[Index]) \
        -> (bool, Text, bool, List[(Text,datastore_v3_pb2.Query.Order.Direction)]):
    ...

def IndexYamlForQuery(kind : Text, ancestor : bool, props : List[(Text,datastore_v3_pb2.Query.Order.Direction)]) -> Text:
    ...

def IndexXmlForQuery(kind : Text, ancestor : bool, props : List[(Text,datastore_v3_pb2.Query.Order.Direction)]) -> Text:
    ...

def IndexDefinitionToProto(app_id : Text, index_definition : Index) -> datastore_v3_pb2.Index:
    ...

def IndexDefinitionsToProtos(app_id : Text, index_definitions : List[Index]) -> List[datastore_v3_pb2.Index]:
    ...

def ProtoToIndexDefinition(proto : datastore_v3_pb2.Index) -> Index:
    ...

def ProtosToIndexDefinitions(protos : List[datastore_v3_pb2.Index]) -> List[Index]:
    ...

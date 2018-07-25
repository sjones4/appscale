#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#



from google.appengine.datastore.datastore_protobuf_adapter import *
from google.net.proto import ProtocolBuffer
import array
import dummy_thread as thread

__pychecker__ = """maxreturns=0 maxbranches=0 no-callinit
                   unusednames=printElemNumber,debug_strs no-special"""

if hasattr(ProtocolBuffer, 'ExtendableProtocolMessage'):
  _extension_runtime = True
  _ExtendableProtocolMessage = ProtocolBuffer.ExtendableProtocolMessage
else:
  _extension_runtime = False
  _ExtendableProtocolMessage = ProtocolBuffer.ProtocolMessage

from google.appengine.api.api_base_pb import Integer64Proto;
from google.appengine.api.api_base_pb import StringProto;
from google.appengine.api.api_base_pb import VoidProto;

if _extension_runtime:
  pass

__all__ = ['Transaction','Query','Query_Filter','Query_Order','CompiledQuery','CompiledQuery_PrimaryScan','CompiledQuery_MergeJoinScan','CompiledQuery_EntityFilter','CompiledCursor','CompiledCursor_PositionIndexValue','CompiledCursor_Position','Cursor','Error','Cost','Cost_CommitCost','GetRequest','GetResponse','GetResponse_Entity','PutRequest','PutResponse','TouchRequest','TouchResponse','DeleteRequest','DeleteResponse','NextRequest','QueryResult','AllocateIdsRequest','AllocateIdsResponse','CompositeIndices','AddActionsRequest','AddActionsResponse','BeginTransactionRequest','CommitResponse','CommitResponse_Version']

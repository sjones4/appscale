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


from google.appengine.datastore import datastore_protobuf_adapter
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

PropertyValue = datastore_protobuf_adapter.PropertyValue
PropertyValue.kint64Value = 1
PropertyValue.kbooleanValue = 2
PropertyValue.kstringValue = 3
PropertyValue.kdoubleValue = 4
PropertyValue.kPointValueGroup = 5
PropertyValue.kPointValuex = 6
PropertyValue.kPointValuey = 7
PropertyValue.kUserValueGroup = 8
PropertyValue.kUserValueemail = 9
PropertyValue.kUserValueauth_domain = 10
PropertyValue.kUserValuenickname = 11
PropertyValue.kUserValuegaiaid = 18
PropertyValue.kUserValueobfuscated_gaiaid = 19
PropertyValue.kUserValuefederated_identity = 21
PropertyValue.kUserValuefederated_provider = 22
PropertyValue.kReferenceValueGroup = 12
PropertyValue.kReferenceValueapp = 13
PropertyValue.kReferenceValuename_space = 20
PropertyValue.kReferenceValuePathElementGroup = 14
PropertyValue.kReferenceValuePathElementtype = 15
PropertyValue.kReferenceValuePathElementid = 16
PropertyValue.kReferenceValuePathElementname = 17

__all__ = ['PropertyValue','PropertyValue_ReferenceValuePathElement','PropertyValue_PointValue','PropertyValue_UserValue','PropertyValue_ReferenceValue','Property','Path','Path_Element','Reference','User','EntityProto','CompositeProperty','Index','Index_Property','CompositeIndex','IndexPostfix_IndexValue','IndexPostfix']

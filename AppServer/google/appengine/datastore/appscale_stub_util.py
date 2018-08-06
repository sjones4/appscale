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

"""Utility functions used by the datstore server. """

try:
  import hashlib
  _MD5_FUNC = hashlib.md5
except ImportError:
  import md5
  _MD5_FUNC = md5.new

import struct
import threading

from google.appengine.api import datastore_types
from google.appengine.api.datastore_errors import BadRequestError
from google.appengine.datastore import datastore_index
from google.appengine.datastore import datastore_pb
from google.appengine.datastore.datastore_stub_util import Check
from google.appengine.datastore.datastore_stub_util import _GuessOrders
from google.appengine.runtime import apiproxy_errors
from google.appengine.datastore import entity_pb





_PROPERTY_TYPE_NAMES = {
    0: 'NULL',
    entity_pb.PropertyValue.kint64Value: 'INT64',
    entity_pb.PropertyValue.kbooleanValue: 'BOOLEAN',
    entity_pb.PropertyValue.kstringValue: 'STRING',
    entity_pb.PropertyValue.kdoubleValue: 'DOUBLE',
    entity_pb.PropertyValue.kPointValueGroup: 'POINT',
    entity_pb.PropertyValue.kUserValueGroup: 'USER',
    entity_pb.PropertyValue.kReferenceValueGroup: 'REFERENCE'
    }


_SCATTER_PROPORTION = 32768

def _GetScatterProperty(entity_proto):
  """Gets the scatter property for an object.

  For ease of implementation, this is not synchronized with the actual
  value on the App Engine server, but should work equally well.

  Note: This property may change, either here or in production. No client
  other than the mapper framework should rely on it directly.

  Returns:
    The PropertyValue of the scatter property or None if this entity should not
    have a scatter property.
  """
  hash_obj = _MD5_FUNC()
  for element in entity_proto.key().path().element_list():
    if element.has_name():
      hash_obj.update(element.name())
    elif element.has_id():
      hash_obj.update(str(element.id()))
  hash_bytes = hash_obj.digest()[0:2]
  (hash_int,) = struct.unpack('H', hash_bytes)

  if hash_int >= _SCATTER_PROPORTION:
    return None

  scatter_property = entity_pb.Property()
  scatter_property.set_name('__scatter__')
  scatter_property.set_meaning(entity_pb.Property.BYTESTRING)
  scatter_property.set_multiple(False)
  property_value = scatter_property.mutable_value()
  property_value.set_stringvalue(hash_bytes)
  return scatter_property





_SPECIAL_PROPERTY_MAP = {
    '__scatter__' : (False, True, _GetScatterProperty)
    }

def GetInvisibleSpecialPropertyNames():
  """Gets the names of all non user-visible special properties."""
  invisible_names = []
  for name, value in _SPECIAL_PROPERTY_MAP.items():
    is_visible, _, _ = value
    if not is_visible:
      invisible_names.append(name)
  return invisible_names

def _PrepareSpecialProperties(entity_proto, is_load):
  """Computes special properties for loading or storing.
  Strips other special properties."""
  for i in xrange(entity_proto.property_size() - 1, -1, -1):
    if _SPECIAL_PROPERTY_MAP.has_key(entity_proto.property(i).name()):
      del entity_proto.property_list()[i]

  for is_visible, is_stored, property_func in _SPECIAL_PROPERTY_MAP.values():
    if is_load:
      should_process = is_visible
    else:
      should_process = is_stored

    if should_process:
      special_property = property_func(entity_proto)
      if special_property:
        entity_proto.property_list().append(special_property)


def PrepareSpecialPropertiesForStore(entity_proto):
  """Computes special properties for storing.
  Strips other special properties."""
  _PrepareSpecialProperties(entity_proto, False)


def PrepareSpecialPropertiesForLoad(entity_proto):
  """Computes special properties that are user-visible.
  Strips other special properties."""
  _PrepareSpecialProperties(entity_proto, True)


def ValidateQuery(query, filters, orders, max_query_components):
  """Validate a datastore query with normalized filters, orders.

  Raises an ApplicationError when any of the following conditions are violated:
  - transactional queries have an ancestor
  - queries that are not too large
    (sum of filters, orders, ancestor <= max_query_components)
  - ancestor (if any) app and namespace match query app and namespace
  - kindless queries only filter on __key__ and only sort on __key__ ascending
  - multiple inequality (<, <=, >, >=) filters all applied to the same property
  - filters on __key__ compare to a reference in the same app and namespace as
    the query
  - if an inequality filter on prop X is used, the first order (if any) must
    be on X

  Args:
    query: query to validate
    filters: normalized (by datastore_index.Normalize) filters from query
    orders: normalized (by datastore_index.Normalize) orders from query
    max_query_components: limit on query complexity
  """

  def BadRequest(message):
    """ Exception raised if there was a bad request. """
    raise apiproxy_errors.ApplicationError(
        datastore_pb.Error.BAD_REQUEST, message)

  key_prop_name = datastore_types._KEY_SPECIAL_PROPERTY
  unapplied_log_timestamp_us_name = (
      datastore_types._UNAPPLIED_LOG_TIMESTAMP_SPECIAL_PROPERTY)

  if query.has_transaction():

    if not query.has_ancestor():
      BadRequest('Only ancestor queries are allowed inside transactions.')

  num_components = len(filters) + len(orders)
  if query.has_ancestor():
    num_components += 1
  if num_components > max_query_components:
    BadRequest('query is too large. may not have more than %s filters'
               ' + sort orders ancestor total' % max_query_components)

  if query.has_ancestor():
    ancestor = query.ancestor()
    if query.app() != ancestor.app():
      BadRequest('query app is %s but ancestor app is %s' %
                 (query.app(), ancestor.app()))
    if query.name_space() != ancestor.name_space():
      BadRequest('query namespace is %s but ancestor namespace is %s' %
                 (query.name_space(), ancestor.name_space()))



  ineq_prop_name = None
  for filter in filters:
    if filter.property_size() != 1:
      BadRequest('Filter has %d properties, expected 1' %
                 filter.property_size())

    prop = filter.property(0)
    prop_name = prop.name().decode('utf-8')

    if prop_name == key_prop_name:



      if not prop.value().has_referencevalue():
        BadRequest('%s filter value must be a Key' % key_prop_name)
      ref_val = prop.value().referencevalue()
      if ref_val.app() != query.app():
        BadRequest('%s filter app is %s but query app is %s' %
                   (key_prop_name, ref_val.app(), query.app()))
      if ref_val.name_space() != query.name_space():
        BadRequest('%s filter namespace is %s but query namespace is %s' %
                   (key_prop_name, ref_val.name_space(), query.name_space()))

    if (filter.op() in datastore_index.INEQUALITY_OPERATORS and
        prop_name != unapplied_log_timestamp_us_name):
      if ineq_prop_name is None:
        ineq_prop_name = prop_name
      elif ineq_prop_name != prop_name:
        BadRequest(('Only one inequality filter per query is supported.  '
                    'Encountered both %s and %s') % (ineq_prop_name, prop_name))

  if ineq_prop_name is not None and orders:

    first_order_prop = orders[0].property().decode('utf-8')
    if first_order_prop != ineq_prop_name:
      BadRequest('The first sort property must be the same as the property '
                 'to which the inequality filter is applied.  In your query '
                 'the first sort property is %s but the inequality filter '
                 'is on %s' % (first_order_prop, ineq_prop_name))

  if not query.has_kind():

    for filter in filters:
      prop_name = filter.property(0).name().decode('utf-8')
      if (prop_name != key_prop_name and
          prop_name != unapplied_log_timestamp_us_name):
        BadRequest('kind is required for non-__key__ filters')
    for order in orders:
      prop_name = order.property().decode('utf-8')
      if not (prop_name == key_prop_name and
              order.direction() is datastore_pb.Query_Order.ASCENDING):
        BadRequest('kind is required for all orders except __key__ ascending')


class ValueRange(object):
  """A range of values defined by its two extremes (inclusive or exclusive)."""

  def __init__(self):
    """Constructor.

    Creates an unlimited range.
    """
    self.__start = self.__end = None
    self.__start_inclusive = self.__end_inclusive = False

  def Update(self, rel_op, limit):
    """Filter the range by 'rel_op limit'.

    Args:
      rel_op: relational operator from datastore_pb.Query_Filter.
      limit: the value to limit the range by.
    """

    if rel_op == datastore_pb.Query_Filter.LESS_THAN:
      if self.__end is None or limit <= self.__end:
        self.__end = limit
        self.__end_inclusive = False
    elif (rel_op == datastore_pb.Query_Filter.LESS_THAN_OR_EQUAL or
          rel_op == datastore_pb.Query_Filter.EQUAL):
      if self.__end is None or limit < self.__end:
        self.__end = limit
        self.__end_inclusive = True

    if rel_op == datastore_pb.Query_Filter.GREATER_THAN:
      if self.__start is None or limit >= self.__start:
        self.__start = limit
        self.__start_inclusive = False
    elif (rel_op == datastore_pb.Query_Filter.GREATER_THAN_OR_EQUAL or
          rel_op == datastore_pb.Query_Filter.EQUAL):
      if self.__start is None or limit > self.__start:
        self.__start = limit
        self.__start_inclusive = True

  def Contains(self, value):
    """Check if the range contains a specific value.

    Args:
      value: the value to check.
    Returns:
      True iff value is contained in this range.
    """
    if self.__start is not None:
      if self.__start_inclusive and value < self.__start: 
        return False
      if not self.__start_inclusive and value <= self.__start: 
        return False
    if self.__end is not None:
      if self.__end_inclusive and value > self.__end: 
        return False
      if not self.__end_inclusive and value >= self.__end: 
        return False
    return True

  def Remap(self, mapper):
    """Transforms the range extremes with a function.

    The function mapper must preserve order, i.e.
      x rel_op y iff mapper(x) rel_op y

    Args:
      mapper: function to apply to the range extremes.
    """
    self.__start = self.__start and mapper(self.__start)
    self.__end = self.__end and mapper(self.__end)

  def MapExtremes(self, mapper):
    """Evaluate a function on the range extremes.

    Args:
      mapper: function to apply to the range extremes.
    Returns:
      (x, y) where x = None if the range has no start,
                       mapper(start, start_inclusive, False) otherwise
                   y = None if the range has no end,
                       mapper(end, end_inclusive, True) otherwise
    """
    return (
        self.__start and mapper(self.__start, self.__start_inclusive, False),
        self.__end and mapper(self.__end, self.__end_inclusive, True))


def ParseKeyFilteredQuery(filters, orders):
  """Parse queries which only allow filters and ascending-orders on __key__.

  Raises exceptions for illegal queries.
  Args:
    filters: the normalized filters of a query.
    orders: the normalized orders of a query.
  Returns:
     The key range (a ValueRange over datastore_types.Key) requested in the
     query.
  """

  remaining_filters = []
  key_range = ValueRange()
  key_prop = datastore_types._KEY_SPECIAL_PROPERTY
  for f in filters:
    op = f.op()
    if not (f.property_size() == 1 and
            f.property(0).name() == key_prop and
            not (op == datastore_pb.Query_Filter.IN or
                 op == datastore_pb.Query_Filter.EXISTS)):
      remaining_filters.append(f)
      continue

    val = f.property(0).value()
    if not val.has_referencevalue():
      raise BadRequestError('__key__ kind must be compared to a key')
    limit = datastore_types.FromReferenceProperty(val)
    key_range.Update(op, limit)


  remaining_orders = []
  for o in orders:
    if not (o.direction() == datastore_pb.Query_Order.ASCENDING and
            o.property() == datastore_types._KEY_SPECIAL_PROPERTY):
      remaining_orders.append(o)
    else:
      break



  if remaining_filters:
    raise BadRequestError(
        'Only comparison filters on ' + key_prop + ' supported')
  if remaining_orders:
    raise BadRequestError('Only ascending order on ' + key_prop + ' supported')

  return key_range


def ParseKindQuery(query, filters, orders):
  """Parse __kind__ (schema) queries.

  Raises exceptions for illegal queries.
  Args:
    query: A Query PB.
    filters: the normalized filters from query.
    orders: the normalized orders from query.
  Returns:
     The kind range (a ValueRange over string) requested in the query.
  """

  if query.has_ancestor():
    raise BadRequestError('ancestor queries on __kind__ not allowed')

  key_range = ParseKeyFilteredQuery(filters, orders)
  key_range.Remap(_KindKeyToString)

  return key_range


def _KindKeyToString(key):
  """Extract kind name from __kind__ key.

  Raises an ApplicationError if the key is not of the form '__kind__'/name.

  Args:
    key: a key for a __kind__ instance.
  Returns:
    kind specified by key.
  """
  key_path = key.to_path()
  if (len(key_path) == 2 and key_path[0] == '__kind__' and
      isinstance(key_path[1], basestring)):
    return key_path[1]
  raise BadRequestError('invalid Key for __kind__ table')


def ParseNamespaceQuery(query, filters, orders):
  """Parse __namespace__  queries.

  Raises exceptions for illegal queries.
  Args:
    query: A Query PB.
    filters: the normalized filters from query.
    orders: the normalized orders from query.
  Returns:
     The kind range (a ValueRange over string) requested in the query.
  """

  if query.has_ancestor():
    raise BadRequestError('ancestor queries on __namespace__ not allowed')

  key_range = ParseKeyFilteredQuery(filters, orders)
  key_range.Remap(_NamespaceKeyToString)

  return key_range


def _NamespaceKeyToString(key):
  """Extract namespace name from __namespace__ key.

  Raises an ApplicationError if the key is not of the form '__namespace__'/name
  or '__namespace__'/_EMPTY_NAMESPACE_ID.

  Args:
    key: a key for a __namespace__ instance.
  Returns:
    namespace specified by key.
  """
  key_path = key.to_path()
  if len(key_path) == 2 and key_path[0] == '__namespace__':
    if key_path[1] == datastore_types._EMPTY_NAMESPACE_ID:
      return ''
    if isinstance(key_path[1], basestring):
      return key_path[1]
  raise BadRequestError('invalid Key for __namespace__ table')


def ParsePropertyQuery(query, filters, orders):
  """Parse __property__  queries.

  Raises exceptions for illegal queries.
  Args:
    query: A Query PB.
    filters: the normalized filters from query.
    orders: the normalized orders from query.
  Returns:
     The kind range (a ValueRange over (kind, property) pairs) requested
     in the query.
  """

  if query.has_transaction():
    raise BadRequestError('transactional queries on __property__ not allowed')

  key_range = ParseKeyFilteredQuery(filters, orders)
  key_range.Remap(lambda x: _PropertyKeyToString(x, ''))

  if query.has_ancestor():
    ancestor = datastore_types.Key._FromPb(query.ancestor())
    ancestor_kind, ancestor_property = _PropertyKeyToString(ancestor, None)


    if ancestor_property is not None:
      key_range.Update(datastore_pb.Query_Filter.EQUAL,
                       (ancestor_kind, ancestor_property))
    else:

      key_range.Update(datastore_pb.Query_Filter.GREATER_THAN_OR_EQUAL,
                       (ancestor_kind, ''))
      key_range.Update(datastore_pb.Query_Filter.LESS_THAN_OR_EQUAL,
                       (ancestor_kind + '\0', ''))
    query.clear_ancestor()

  return key_range

def _PropertyKeyToString(key, default_property):
  """Extract property name from __property__ key.

  Raises an ApplicationError if the key is not of the form
  '__kind__'/kind, '__property__'/property or '__kind__'/kind

  Args:
    key: a key for a __property__ instance.
    default_property: property value to return when key only has a kind.
  Returns:
    kind, property if key = '__kind__'/kind, '__property__'/property
    kind, default_property if key = '__kind__'/kind
  """
  key_path = key.to_path()
  if (len(key_path) == 2 and
      key_path[0] == '__kind__' and isinstance(key_path[1], basestring)):
    return (key_path[1], default_property)
  if (len(key_path) == 4 and
      key_path[0] == '__kind__' and isinstance(key_path[1], basestring) and
      key_path[2] == '__property__' and isinstance(key_path[3], basestring)):
    return (key_path[1], key_path[3])

  raise BadRequestError('invalid Key for __property__ table')


def SynthesizeUserId(email):
  """Return a synthetic user ID from an email address.

  Note that this is not the same user ID found in the production system.

  Args:
    email: An email address.

  Returns:
    A string userid derived from the email address.
  """

  user_id_digest = _MD5_FUNC(email.lower()).digest()
  user_id = '1' + ''.join(['%02d' % ord(x) for x in user_id_digest])[:20]
  return user_id


def FillUsersInQuery(filters):
  """Fill in a synthetic user ID for all user properties in a set of filters.

  Args:
    filters: The normalized filters from query.
  """
  for filter in filters:
    for property in filter.property_list():
      FillUser(property)


def FillUser(property):
  """Fill in a synthetic user ID for a user properties.

  Args:
    property: A Property which may have a user value.
  """
  if property.value().has_uservalue():
    uid = SynthesizeUserId(property.value().uservalue().email())
    if uid:
      property.mutable_value().mutable_uservalue().set_obfuscated_gaiaid(uid)


def order_property_names(query):
  """ Generates a list of relevant order properties from the query.

  Returns:
    A set of Property objects.
  """
  filters, orders = datastore_index.Normalize(query.filter_list(),
    query.order_list(), [])
  orders = _GuessOrders(filters, orders)
  return set(order.property()
             for order in orders if order.property() != '__key__')


class BaseCursor(object):
  """A base query cursor over a list of entities.

  Public properties:
    cursor: the integer cursor
    app: the app for which this cursor was created

  Class attributes:
    _next_cursor: the next cursor to allocate
    _next_cursor_lock: protects _next_cursor
  """
  _next_cursor = 1
  _next_cursor_lock = threading.Lock()

  def __init__(self, app):
    """Constructor.

    Args:
      app: The app this cursor is being created for.
    """
    self.app = app
    self.cursor = self._AcquireCursorID()

  def PopulateCursor(self, query_result):
    """ Creates cursor for the given query result. """
    if query_result.more_results():
      cursor = query_result.mutable_cursor()
      cursor.set_app(self.app)
      cursor.set_cursor(self.cursor)

  @classmethod
  def _AcquireCursorID(cls):
    """Acquires the next cursor id in a thread safe manner."""
    cls._next_cursor_lock.acquire()
    try:
      cursor_id = cls._next_cursor
      cls._next_cursor += 1
    finally:
      cls._next_cursor_lock.release()
    return cursor_id


class QueryResult(object):
    has_cursor_ = 0
    cursor_ = None
    has_skipped_results_ = 0
    skipped_results_ = 0
    has_more_results_ = 0
    more_results_ = 0
    has_keys_only_ = 0
    keys_only_ = 0
    has_index_only_ = 0
    index_only_ = 0
    has_small_ops_ = 0
    small_ops_ = 0
    has_compiled_query_ = 0
    compiled_query_ = None
    has_compiled_cursor_ = 0
    compiled_cursor_ = None

    def __init__(self, contents=None):
        self.result_ = []
        self.index_ = []
        if contents is not None: self.MergeFromString(contents)

    def cursor(self):
        if self.cursor_ is None: self.cursor_ = Cursor()
        return self.cursor_

    def mutable_cursor(self): self.has_cursor_ = 1; return self.cursor()

    def clear_cursor(self):
        if self.has_cursor_:
            self.has_cursor_ = 0;
            if self.cursor_ is not None: self.cursor_.Clear()

    def has_cursor(self): return self.has_cursor_

    def result_size(self): return len(self.result_)
    def result_list(self): return self.result_

    def result(self, i):
        return self.result_[i]

    # def mutable_result(self, i):
    #     return self.result_[i]
    #
    # def add_result(self):
    #     x = EntityProto()
    #     self.result_.append(x)
    #     return x
    #
    # def clear_result(self):
    #     self.result_ = []
    # def skipped_results(self): return self.skipped_results_
    #
    # def set_skipped_results(self, x):
    #     self.has_skipped_results_ = 1
    #     self.skipped_results_ = x
    #
    # def clear_skipped_results(self):
    #     if self.has_skipped_results_:
    #         self.has_skipped_results_ = 0
    #         self.skipped_results_ = 0
    #
    # def has_skipped_results(self): return self.has_skipped_results_
    #
    # def more_results(self): return self.more_results_
    #
    # def set_more_results(self, x):
    #     self.has_more_results_ = 1
    #     self.more_results_ = x
    #
    # def clear_more_results(self):
    #     if self.has_more_results_:
    #         self.has_more_results_ = 0
    #         self.more_results_ = 0
    #
    # def has_more_results(self): return self.has_more_results_

    def keys_only(self): return self.keys_only_

    def set_keys_only(self, x):
        self.has_keys_only_ = 1
        self.keys_only_ = x

    def clear_keys_only(self):
        if self.has_keys_only_:
            self.has_keys_only_ = 0
            self.keys_only_ = 0

    def has_keys_only(self): return self.has_keys_only_

    def index_only(self): return self.index_only_

    def set_index_only(self, x):
        self.has_index_only_ = 1
        self.index_only_ = x

    def clear_index_only(self):
        if self.has_index_only_:
            self.has_index_only_ = 0
            self.index_only_ = 0

    def has_index_only(self): return self.has_index_only_

    def small_ops(self): return self.small_ops_

    def set_small_ops(self, x):
        self.has_small_ops_ = 1
        self.small_ops_ = x

    def clear_small_ops(self):
        if self.has_small_ops_:
            self.has_small_ops_ = 0
            self.small_ops_ = 0

    def has_small_ops(self): return self.has_small_ops_

    # def compiled_query(self):
    #     if self.compiled_query_ is None:
    #         self.lazy_init_lock_.acquire()
    #         try:
    #             if self.compiled_query_ is None: self.compiled_query_ = CompiledQuery()
    #         finally:
    #             self.lazy_init_lock_.release()
    #     return self.compiled_query_
    #
    # def mutable_compiled_query(self): self.has_compiled_query_ = 1; return self.compiled_query()
    #
    # def clear_compiled_query(self):
    #
    #     if self.has_compiled_query_:
    #         self.has_compiled_query_ = 0;
    #         if self.compiled_query_ is not None: self.compiled_query_.Clear()
    #
    # def has_compiled_query(self): return self.has_compiled_query_
    #
    # def compiled_cursor(self):
    #     if self.compiled_cursor_ is None:
    #         self.lazy_init_lock_.acquire()
    #         try:
    #             if self.compiled_cursor_ is None: self.compiled_cursor_ = CompiledCursor()
    #         finally:
    #             self.lazy_init_lock_.release()
    #     return self.compiled_cursor_
    #
    # def mutable_compiled_cursor(self): self.has_compiled_cursor_ = 1; return self.compiled_cursor()
    #
    # def clear_compiled_cursor(self):
    #
    #     if self.has_compiled_cursor_:
    #         self.has_compiled_cursor_ = 0;
    #         if self.compiled_cursor_ is not None: self.compiled_cursor_.Clear()
    #
    # def has_compiled_cursor(self): return self.has_compiled_cursor_

    def index_size(self): return len(self.index_)
    def index_list(self): return self.index_

    def index(self, i):
        return self.index_[i]

    # def mutable_index(self, i):
    #     return self.index_[i]
    #
    # def add_index(self):
    #     x = CompositeIndex()
    #     self.index_.append(x)
    #     return x
    #
    # def clear_index(self):
    #     self.index_ = []

    def MergeFrom(self, x):
        assert x is not self
        if (x.has_cursor()): self.mutable_cursor().MergeFrom(x.cursor())
        for i in xrange(x.result_size()): self.add_result().CopyFrom(x.result(i))
        if (x.has_skipped_results()): self.set_skipped_results(x.skipped_results())
        if (x.has_more_results()): self.set_more_results(x.more_results())
        if (x.has_keys_only()): self.set_keys_only(x.keys_only())
        if (x.has_index_only()): self.set_index_only(x.index_only())
        if (x.has_small_ops()): self.set_small_ops(x.small_ops())
        if (x.has_compiled_query()): self.mutable_compiled_query().MergeFrom(x.compiled_query())
        if (x.has_compiled_cursor()): self.mutable_compiled_cursor().MergeFrom(x.compiled_cursor())
        for i in xrange(x.index_size()): self.add_index().CopyFrom(x.index(i))

    def Equals(self, x):
        if x is self: return 1
        if self.has_cursor_ != x.has_cursor_: return 0
        if self.has_cursor_ and self.cursor_ != x.cursor_: return 0
        if len(self.result_) != len(x.result_): return 0
        for e1, e2 in zip(self.result_, x.result_):
            if e1 != e2: return 0
        if self.has_skipped_results_ != x.has_skipped_results_: return 0
        if self.has_skipped_results_ and self.skipped_results_ != x.skipped_results_: return 0
        if self.has_more_results_ != x.has_more_results_: return 0
        if self.has_more_results_ and self.more_results_ != x.more_results_: return 0
        if self.has_keys_only_ != x.has_keys_only_: return 0
        if self.has_keys_only_ and self.keys_only_ != x.keys_only_: return 0
        if self.has_index_only_ != x.has_index_only_: return 0
        if self.has_index_only_ and self.index_only_ != x.index_only_: return 0
        if self.has_small_ops_ != x.has_small_ops_: return 0
        if self.has_small_ops_ and self.small_ops_ != x.small_ops_: return 0
        if self.has_compiled_query_ != x.has_compiled_query_: return 0
        if self.has_compiled_query_ and self.compiled_query_ != x.compiled_query_: return 0
        if self.has_compiled_cursor_ != x.has_compiled_cursor_: return 0
        if self.has_compiled_cursor_ and self.compiled_cursor_ != x.compiled_cursor_: return 0
        if len(self.index_) != len(x.index_): return 0
        for e1, e2 in zip(self.index_, x.index_):
            if e1 != e2: return 0
        return 1

    def IsInitialized(self, debug_strs=None):
        initialized = 1
        if (self.has_cursor_ and not self.cursor_.IsInitialized(debug_strs)): initialized = 0
        for p in self.result_:
            if not p.IsInitialized(debug_strs): initialized=0
        if (not self.has_more_results_):
            initialized = 0
            if debug_strs is not None:
                debug_strs.append('Required field: more_results not set.')
        if (self.has_compiled_query_ and not self.compiled_query_.IsInitialized(debug_strs)): initialized = 0
        if (self.has_compiled_cursor_ and not self.compiled_cursor_.IsInitialized(debug_strs)): initialized = 0
        for p in self.index_:
            if not p.IsInitialized(debug_strs): initialized=0
        return initialized

    def ByteSize(self):
        n = 0
        if (self.has_cursor_): n += 1 + self.lengthString(self.cursor_.ByteSize())
        n += 1 * len(self.result_)
        for i in xrange(len(self.result_)): n += self.lengthString(self.result_[i].ByteSize())
        if (self.has_skipped_results_): n += 1 + self.lengthVarInt64(self.skipped_results_)
        if (self.has_keys_only_): n += 2
        if (self.has_index_only_): n += 2
        if (self.has_small_ops_): n += 2
        if (self.has_compiled_query_): n += 1 + self.lengthString(self.compiled_query_.ByteSize())
        if (self.has_compiled_cursor_): n += 1 + self.lengthString(self.compiled_cursor_.ByteSize())
        n += 1 * len(self.index_)
        for i in xrange(len(self.index_)): n += self.lengthString(self.index_[i].ByteSize())
        return n + 2

    def ByteSizePartial(self):
        n = 0
        if (self.has_cursor_): n += 1 + self.lengthString(self.cursor_.ByteSizePartial())
        n += 1 * len(self.result_)
        for i in xrange(len(self.result_)): n += self.lengthString(self.result_[i].ByteSizePartial())
        if (self.has_skipped_results_): n += 1 + self.lengthVarInt64(self.skipped_results_)
        if (self.has_more_results_):
            n += 2
        if (self.has_keys_only_): n += 2
        if (self.has_index_only_): n += 2
        if (self.has_small_ops_): n += 2
        if (self.has_compiled_query_): n += 1 + self.lengthString(self.compiled_query_.ByteSizePartial())
        if (self.has_compiled_cursor_): n += 1 + self.lengthString(self.compiled_cursor_.ByteSizePartial())
        n += 1 * len(self.index_)
        for i in xrange(len(self.index_)): n += self.lengthString(self.index_[i].ByteSizePartial())
        return n

    def Clear(self):
        self.clear_cursor()
        self.clear_result()
        self.clear_skipped_results()
        self.clear_more_results()
        self.clear_keys_only()
        self.clear_index_only()
        self.clear_small_ops()
        self.clear_compiled_query()
        self.clear_compiled_cursor()
        self.clear_index()

    def OutputUnchecked(self, out):
        if (self.has_cursor_):
            out.putVarInt32(10)
            out.putVarInt32(self.cursor_.ByteSize())
            self.cursor_.OutputUnchecked(out)
        for i in xrange(len(self.result_)):
            out.putVarInt32(18)
            out.putVarInt32(self.result_[i].ByteSize())
            self.result_[i].OutputUnchecked(out)
        out.putVarInt32(24)
        out.putBoolean(self.more_results_)
        if (self.has_keys_only_):
            out.putVarInt32(32)
            out.putBoolean(self.keys_only_)
        if (self.has_compiled_query_):
            out.putVarInt32(42)
            out.putVarInt32(self.compiled_query_.ByteSize())
            self.compiled_query_.OutputUnchecked(out)
        if (self.has_compiled_cursor_):
            out.putVarInt32(50)
            out.putVarInt32(self.compiled_cursor_.ByteSize())
            self.compiled_cursor_.OutputUnchecked(out)
        if (self.has_skipped_results_):
            out.putVarInt32(56)
            out.putVarInt32(self.skipped_results_)
        for i in xrange(len(self.index_)):
            out.putVarInt32(66)
            out.putVarInt32(self.index_[i].ByteSize())
            self.index_[i].OutputUnchecked(out)
        if (self.has_index_only_):
            out.putVarInt32(72)
            out.putBoolean(self.index_only_)
        if (self.has_small_ops_):
            out.putVarInt32(80)
            out.putBoolean(self.small_ops_)

    def OutputPartial(self, out):
        if (self.has_cursor_):
            out.putVarInt32(10)
            out.putVarInt32(self.cursor_.ByteSizePartial())
            self.cursor_.OutputPartial(out)
        for i in xrange(len(self.result_)):
            out.putVarInt32(18)
            out.putVarInt32(self.result_[i].ByteSizePartial())
            self.result_[i].OutputPartial(out)
        if (self.has_more_results_):
            out.putVarInt32(24)
            out.putBoolean(self.more_results_)
        if (self.has_keys_only_):
            out.putVarInt32(32)
            out.putBoolean(self.keys_only_)
        if (self.has_compiled_query_):
            out.putVarInt32(42)
            out.putVarInt32(self.compiled_query_.ByteSizePartial())
            self.compiled_query_.OutputPartial(out)
        if (self.has_compiled_cursor_):
            out.putVarInt32(50)
            out.putVarInt32(self.compiled_cursor_.ByteSizePartial())
            self.compiled_cursor_.OutputPartial(out)
        if (self.has_skipped_results_):
            out.putVarInt32(56)
            out.putVarInt32(self.skipped_results_)
        for i in xrange(len(self.index_)):
            out.putVarInt32(66)
            out.putVarInt32(self.index_[i].ByteSizePartial())
            self.index_[i].OutputPartial(out)
        if (self.has_index_only_):
            out.putVarInt32(72)
            out.putBoolean(self.index_only_)
        if (self.has_small_ops_):
            out.putVarInt32(80)
            out.putBoolean(self.small_ops_)

    def TryMerge(self, d):
        while d.avail() > 0:
            tt = d.getVarInt32()
            if tt == 10:
                length = d.getVarInt32()
                tmp = ProtocolBuffer.Decoder(d.buffer(), d.pos(), d.pos() + length)
                d.skip(length)
                self.mutable_cursor().TryMerge(tmp)
                continue
            if tt == 18:
                length = d.getVarInt32()
                tmp = ProtocolBuffer.Decoder(d.buffer(), d.pos(), d.pos() + length)
                d.skip(length)
                self.add_result().TryMerge(tmp)
                continue
            if tt == 24:
                self.set_more_results(d.getBoolean())
                continue
            if tt == 32:
                self.set_keys_only(d.getBoolean())
                continue
            if tt == 42:
                length = d.getVarInt32()
                tmp = ProtocolBuffer.Decoder(d.buffer(), d.pos(), d.pos() + length)
                d.skip(length)
                self.mutable_compiled_query().TryMerge(tmp)
                continue
            if tt == 50:
                length = d.getVarInt32()
                tmp = ProtocolBuffer.Decoder(d.buffer(), d.pos(), d.pos() + length)
                d.skip(length)
                self.mutable_compiled_cursor().TryMerge(tmp)
                continue
            if tt == 56:
                self.set_skipped_results(d.getVarInt32())
                continue
            if tt == 66:
                length = d.getVarInt32()
                tmp = ProtocolBuffer.Decoder(d.buffer(), d.pos(), d.pos() + length)
                d.skip(length)
                self.add_index().TryMerge(tmp)
                continue
            if tt == 72:
                self.set_index_only(d.getBoolean())
                continue
            if tt == 80:
                self.set_small_ops(d.getBoolean())
                continue


            if (tt == 0): raise ProtocolBuffer.ProtocolBufferDecodeError
            d.skipData(tt)


    def __str__(self, prefix="", printElemNumber=0):
        res=""
        if self.has_cursor_:
            res+=prefix+"cursor <\n"
            res+=self.cursor_.__str__(prefix + "  ", printElemNumber)
            res+=prefix+">\n"
        cnt=0
        for e in self.result_:
            elm=""
            if printElemNumber: elm="(%d)" % cnt
            res+=prefix+("result%s <\n" % elm)
            res+=e.__str__(prefix + "  ", printElemNumber)
            res+=prefix+">\n"
            cnt+=1
        if self.has_skipped_results_: res+=prefix+("skipped_results: %s\n" % self.DebugFormatInt32(self.skipped_results_))
        if self.has_more_results_: res+=prefix+("more_results: %s\n" % self.DebugFormatBool(self.more_results_))
        if self.has_keys_only_: res+=prefix+("keys_only: %s\n" % self.DebugFormatBool(self.keys_only_))
        if self.has_index_only_: res+=prefix+("index_only: %s\n" % self.DebugFormatBool(self.index_only_))
        if self.has_small_ops_: res+=prefix+("small_ops: %s\n" % self.DebugFormatBool(self.small_ops_))
        if self.has_compiled_query_:
            res+=prefix+"compiled_query <\n"
            res+=self.compiled_query_.__str__(prefix + "  ", printElemNumber)
            res+=prefix+">\n"
        if self.has_compiled_cursor_:
            res+=prefix+"compiled_cursor <\n"
            res+=self.compiled_cursor_.__str__(prefix + "  ", printElemNumber)
            res+=prefix+">\n"
        cnt=0
        for e in self.index_:
            elm=""
            if printElemNumber: elm="(%d)" % cnt
            res+=prefix+("index%s <\n" % elm)
            res+=e.__str__(prefix + "  ", printElemNumber)
            res+=prefix+">\n"
            cnt+=1
        return res


    # def _BuildTagLookupTable(sparse, maxtag, default=None):
    #     return tuple([sparse.get(i, default) for i in xrange(0, 1+maxtag)])
    #
    # kcursor = 1
    # kresult = 2
    # kskipped_results = 7
    # kmore_results = 3
    # kkeys_only = 4
    # kindex_only = 9
    # ksmall_ops = 10
    # kcompiled_query = 5
    # kcompiled_cursor = 6
    # kindex = 8
    #
    # _TEXT = _BuildTagLookupTable({
    #     0: "ErrorCode",
    #     1: "cursor",
    #     2: "result",
    #     3: "more_results",
    #     4: "keys_only",
    #     5: "compiled_query",
    #     6: "compiled_cursor",
    #     7: "skipped_results",
    #     8: "index",
    #     9: "index_only",
    #     10: "small_ops",
    # }, 10)
    #
    # _TYPES = _BuildTagLookupTable({
    #     0: ProtocolBuffer.Encoder.NUMERIC,
    #     1: ProtocolBuffer.Encoder.STRING,
    #     2: ProtocolBuffer.Encoder.STRING,
    #     3: ProtocolBuffer.Encoder.NUMERIC,
    #     4: ProtocolBuffer.Encoder.NUMERIC,
    #     5: ProtocolBuffer.Encoder.STRING,
    #     6: ProtocolBuffer.Encoder.STRING,
    #     7: ProtocolBuffer.Encoder.NUMERIC,
    #     8: ProtocolBuffer.Encoder.STRING,
    #     9: ProtocolBuffer.Encoder.NUMERIC,
    #     10: ProtocolBuffer.Encoder.NUMERIC,
    # }, 10, ProtocolBuffer.Encoder.MAX_TYPE)



class QueryCursor(object):
  """Encapsulates a database cursor and provides methods to fetch results."""

  def __init__(self, query, results, last_ent):
    """Constructor.

    Args:
      query: A Query PB.
      results: A list of EntityProtos.
      last_ent: The last entity (used for cursors).
    """
    self.__order_property_names = order_property_names(query)
    self.__results = results
    self.__query = query
    self.__last_ent = last_ent
    self.app = query.app()

    if query.has_limit():
      self.limit = query.limit() + query.offset()
    else:
      self.limit = None

  def _EncodeCompiledCursor(self, compiled_cursor):
    """Converts the current state of the cursor into a compiled_cursor.

    Args:
      query: the datastore_pb.Query this cursor is related to
      compiled_cursor: an empty datstore_pb.CompiledCursor
    """
    last_result = None
    if self.__results:
      last_result = self.__results[-1]
    elif self.__last_ent:
      last_result = entity_pb.EntityProto()
      last_result.ParseFromString(self.__last_ent)

    if last_result is not None:
      position = compiled_cursor.add_position()
      position.mutable_key().MergeFrom(last_result.key())
      for prop in last_result.property_list():
        if prop.name() in self.__order_property_names:
          indexvalue = position.add_indexvalue()
          indexvalue.set_property(prop.name())
          indexvalue.mutable_value().CopyFrom(prop.value())
      position.set_start_inclusive(False)

  def PopulateQueryResult(self, count, offset, result):
    """Populates a QueryResult PB with results from the cursor.

    Args:
      count: The number of results to retrieve.
      offset: The number of results to skip.
      result: out: A query_result PB.
    """
    result.set_skipped_results(min(count, offset))
    result_list = result.result_list()
    if self.__results:
      if self.__query.keys_only():
        for entity in self.__results:
          entity.clear_property()
          entity.clear_raw_property()
          result_list.append(entity)
      else:
        result_list.extend(self.__results)
    else:
      result_list = []
    result.set_keys_only(self.__query.keys_only())
    result.set_more_results(offset < count)
    if self.__results or self.__last_ent:
      self._EncodeCompiledCursor(result.mutable_compiled_cursor())
 

class ListCursor(BaseCursor):
  """A query cursor over a list of entities.

  Public properties:
    keys_only: whether the query is keys_only
  """

  def __init__(self, query):
    """Constructor.

    Args:
      query: the query request proto
    """
    super(ListCursor, self).__init__(query.app())

    self.__order_property_names = order_property_names(query)
    if query.has_compiled_cursor() and query.compiled_cursor().position_list():
      self.__last_result, _ = (self._DecodeCompiledCursor(
          query.compiled_cursor()))
    else:
      self.__last_result = None

    if query.has_end_compiled_cursor():
      if query.end_compiled_cursor().position_list():
        self.__end_result, _ = self._DecodeCompiledCursor(
            query.end_compiled_cursor())
    else:
      self.__end_result = None

    self.__query = query
    self.__offset = 0
    self.__count = query.limit()


    self.keys_only = query.keys_only()
    
  def _GetLastResult(self):
    """ Protected access to private member. """
    return self.__last_result

  def _GetEndResult(self):                                                      
    """ Protected access to private member for last entity. """                 
    return self.__end_result           

  @staticmethod
  def _GetCursorOffset(results, cursor_entity, inclusive, compare):
    """Converts a cursor entity into a offset into the result set even if the
    cursor_entity no longer exists.

    Args:
      results: the query's results (sequence of datastore_pb.EntityProto)
      cursor_entity: the datastore_pb.EntityProto from the compiled query
      inclusive: boolean that specifies if to offset past the cursor_entity
      compare: a function that takes two datastore_pb.EntityProto and compares
        them.
    Returns:
      the integer offset
    """
    lo = 0
    hi = len(results)
    if inclusive:

      while lo < hi:
        mid = (lo + hi) // 2
        if compare(results[mid], cursor_entity) < 0:
          lo = mid + 1
        else:
          hi = mid
    else:

      while lo < hi:
        mid = (lo + hi) // 2
        if compare(cursor_entity, results[mid]) < 0:
          hi = mid
        else:
          lo = mid + 1
    return lo

  def _DecodeCompiledCursor(self, compiled_cursor):
    """Converts a compiled_cursor into a cursor_entity.

    Args:
      compiled_cursor: The datastore_pb.CompiledCursor to decode.

    Returns:
      (cursor_entity, inclusive): a datastore_pb.EntityProto and if it should
      be included in the result set.
    """
    assert len(compiled_cursor.position_list()) == 1

    position = compiled_cursor.position(0)




    remaining_properties = self.__order_property_names.copy()
    cursor_entity = datastore_pb.EntityProto()
    cursor_entity.mutable_key().CopyFrom(position.key())
    for indexvalue in position.indexvalue_list():
      property = cursor_entity.add_property()
      property.set_name(indexvalue.property())
      property.mutable_value().CopyFrom(indexvalue.value())
      remaining_properties.remove(indexvalue.property())

    Check(not remaining_properties,
          'Cursor does not match query: missing values for %r' %
          remaining_properties)

    return (cursor_entity, position.start_inclusive())

  def Count(self):
    """Counts results, up to the query's limit.

    Note this method does not deduplicate results, so the query it was generated
    from should have the 'distinct' clause applied.

    Returns:
      int: Result count.
    """
    return self.__count


def CompareEntityPbByKey(a, b):
  """Compare two entity protobuf's by key.

  Args:
    a: datastore_pb.EntityProto to compare
    b: datastore_pb.EntityProto to compare
  Returns:
     <0 if a's key is before b's, =0 if they are the same key, and >0 otherwise.
  """
  return cmp(datastore_types.Key._FromPb(a.key()),
             datastore_types.Key._FromPb(b.key()))

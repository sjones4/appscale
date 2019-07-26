import base64
import datetime
import logging
import os
import random
import re
import sys
import threading
import time

from kazoo.exceptions import NoNodeError
from tornado import gen
from tornado.ioloop import IOLoop

from appscale.common import appscale_info
from appscale.common import constants
from appscale.common.unpackaged import APPSCALE_PYTHON_APPSERVER
from appscale.common.unpackaged import DASHBOARD_DIR
from appscale.datastore import appscale_datastore_batch
from appscale.datastore import dbconstants
from appscale.datastore import helper_functions
from appscale.datastore import utils
from appscale.datastore.datastore_distributed import DatastoreDistributed
from appscale.datastore.fdb.fdb_datastore import FDBDatastore
from appscale.datastore.index_manager import IndexManager
from appscale.datastore.utils import (
    tornado_synchronous, UnprocessedQueryResult)
from appscale.datastore.zkappscale.entity_lock import EntityLock
from appscale.datastore.zkappscale import zktransaction as zk

sys.path.append(APPSCALE_PYTHON_APPSERVER)
from google.appengine.api import apiproxy_stub_map
from google.appengine.api import datastore_distributed
from google.appengine.api.memcache import memcache_distributed
from google.appengine.datastore import datastore_pb
from google.appengine.datastore import entity_pb
from google.appengine.datastore.datastore_pbs import (
    get_entity_converter)
from google.appengine.datastore.datastore_query import Cursor
from google.appengine.ext import db
from google.appengine.ext.db import Key
from google.appengine.ext.db import stats
from google.appengine.ext.db import metadata
from google.appengine.api import datastore_errors

logger = logging.getLogger(__name__)


class DatastoreGroomer(threading.Thread):
  """ Scans the entire database for each application. """

  # The amount of seconds between polling to get the groomer lock.
  # Each datastore server does this poll, so it happens the number
  # of datastore servers within this lock period.
  LOCK_POLL_PERIOD = 4 * 60 * 60 # <- 4 hours

  # Retry sleep on datastore error in seconds.
  DB_ERROR_PERIOD = 30

  # The number of entities retrieved in a datastore request.
  BATCH_SIZE = 100

  # Any kind that is of __*__ is private and should not have stats.
  PRIVATE_KINDS = '__(.*)__'

  # Any kind that is of _*_ is protected and should not have stats.
  PROTECTED_KINDS = '_(.*)_'

  # The amount of time in seconds before we want to clean up task name holders.
  TASK_NAME_TIMEOUT = 24 * 60 * 60

  # The amount of time before logs are considered too old.
  LOG_STORAGE_TIMEOUT = 24 * 60 * 60 * 7

  # Do not generate stats for AppScale internal apps.
  APPSCALE_APPLICATIONS = ['apichecker', 'appscaledashboard']

  # The path in ZooKeeper where the groomer state is stored.
  GROOMER_STATE_PATH = '/appscale/groomer_state'

  # The characters used to separate values when storing the groomer state.
  GROOMER_STATE_DELIMITER = '||'

  # The ID for the task to clean up entities.
  CLEAN_ENTITIES_TASK = 'entities'

  # The ID for the task to clean up old logs.
  CLEAN_LOGS_TASK = 'logs'

  # The ID for the task to clean up old tasks.
  CLEAN_TASKS_TASK = 'tasks'

  # Log progress every time this many seconds have passed.
  LOG_PROGRESS_FREQUENCY = 60 * 5

  def __init__(self, zoo_keeper, table_name, ds_path):
    """ Constructor.

    Args:
      zk: ZooKeeper client.
      table_name: Ignored, this groomer is for foundationdb
      ds_path: The connection path to the datastore_server.
    """
    logger.info("Logging started")

    threading.Thread.__init__(self)
    self.zoo_keeper = zoo_keeper
    self.ds_access = None
    self.datastore_path = ds_path
    self.stats = {}
    self.namespace_info = {}
    self.num_deletes = 0
    self.entities_checked = 0
    self.last_logged = time.time()
    self.groomer_state = []

  def stop(self):
    """ Stops the groomer thread. """
    self.zoo_keeper.close()

  def run(self):
    """ Starts the main loop of the groomer thread. """
    while True:

      logger.debug("Trying to get groomer lock.")
      if self.get_groomer_lock():
        logger.info("Got the groomer lock.")
        self.run_groomer()
        try:
          self.zoo_keeper.release_lock_with_path(zk.DS_GROOM_LOCK_PATH)
        except zk.ZKTransactionException, zk_exception:
          logger.error("Unable to release zk lock {0}.".\
            format(str(zk_exception)))
        except zk.ZKInternalException, zk_exception:
          logger.error("Unable to release zk lock {0}.".\
            format(str(zk_exception)))
      else:
        logger.info("Did not get the groomer lock.")
      sleep_time = random.randint(1, self.LOCK_POLL_PERIOD)
      logger.info('Sleeping for {:.1f} minutes.'.format(sleep_time/60.0))
      time.sleep(sleep_time)

  def get_groomer_lock(self):
    """ Tries to acquire the lock to the datastore groomer.

    Returns:
      True on success, False otherwise.
    """
    return self.zoo_keeper.get_lock_with_path(zk.DS_GROOM_LOCK_PATH)

  def get_projects_from_zookeeper(self):
      """ Wrapper function for getting list of projects from zookeeper. """
      try:
          return sorted(self.zoo_keeper.handle.get_children('/appscale/projects'))
      except NoNodeError:
          return []

  def get_entity_batch(self, last_key):
    """ Gets a batch of entites to operate on.

    Args:
      last_key: The last key from a previous query.
    Returns:
      A list of encoded entities.
    """
    app_id = ''
    key = None
    if last_key:
      key = Key(last_key)
      app_id = key.app()
      found_app = False

    for project_app_id in self.get_projects_from_zookeeper():
      if project_app_id >= app_id:
        if project_app_id > app_id:
            key = None  # clear key that was for previous app

        logger.debug('Getting entity batch for project {} from key {}'.format(project_app_id, key))
        batch = self.results_batch(project_app_id, key)
        if batch:
          logger.debug('Got entity batch for project {} of size {}'.format(project_app_id, str(len(batch))))
          return batch

    return None

  def results_batch(self, app_id, last_key):
    # Returns:
    #   ordered list of encoded entities
    query = datastore_pb.Query()
    query.set_app(app_id)
    query.set_limit(self.BATCH_SIZE)

    if last_key:
      filter = query.add_filter()
      filter.set_op(3)  # >
      prop = filter.add_property()
      prop.set_multiple(False)
      prop.set_name('__key__')
      value = prop.mutable_value()
      get_entity_converter().v3_reference_to_v3_property_value(last_key._ToPb(), value)

    clone_qr_pb = UnprocessedQueryResult()
    tornado_synchronous(self.ds_access._dynamic_run_query)(query, clone_qr_pb)
    # TODO do we need to filter these results?
    logger.info("Results batch {}".format(str(clone_qr_pb)))
    return clone_qr_pb.result_list()

  def reset_statistics(self):
    """ Reinitializes statistics. """
    self.stats = {}
    self.namespace_info = {}
    self.num_deletes = 0

  def initialize_kind(self, app_id, kind):
    """ Puts a kind into the statistics object if
        it does not already exist.
    Args:
      app_id: The application ID.
      kind: A string representing an entity kind.
    """
    if app_id not in self.stats:
      self.stats[app_id] = {kind: {'size': 0, 'number': 0}}
    if kind not in self.stats[app_id]:
      self.stats[app_id][kind] = {'size': 0, 'number': 0}

  def initialize_namespace(self, app_id, namespace):
    """ Puts a namespace into the namespace object if
        it does not already exist.
    Args:
      app_id: The application ID.
      namespace: A string representing a namespace.
    """
    if app_id not in self.namespace_info:
      self.namespace_info[app_id] = {namespace: {'size': 0, 'number': 0}}

    if namespace not in self.namespace_info[app_id]:
      self.namespace_info[app_id] = {namespace: {'size': 0, 'number': 0}}
    if namespace not in self.namespace_info[app_id]:
      self.stats[app_id][namespace] = {'size': 0, 'number': 0}

  def process_statistics(self, key, entity, size):
    """ Processes an entity and adds to the global statistics.

    Args:
      key: The key to the entity table.
      entity: EntityProto entity.
      size: A int of the size of the entity.
    Returns:
      True on success, False otherwise.
    """
    kind = utils.get_entity_kind(entity.key())
    namespace = entity.key().name_space()

    if not kind:
      logger.warning("Entity did not have a kind {0}"\
        .format(entity))
      return False

    if re.match(self.PROTECTED_KINDS, kind):
      return True

    if re.match(self.PRIVATE_KINDS, kind):
      return True

    app_id = entity.key().app()
    if not app_id:
      logger.warning("Entity of kind {0} did not have an app id"\
        .format(kind))
      return False

    # Do not generate statistics for applications which are internal to
    # AppScale.
    if app_id in self.APPSCALE_APPLICATIONS:
      return True

    self.initialize_kind(app_id, kind)
    self.initialize_namespace(app_id, namespace)
    self.namespace_info[app_id][namespace]['size'] += size
    self.namespace_info[app_id][namespace]['number'] += 1
    self.stats[app_id][kind]['size'] += size
    self.stats[app_id][kind]['number'] += 1
    return True

  def process_entity(self, entity):
    """ Processes an entity by updating statistics.

    Args:
      entity: The entity to operate on.
    Returns:
      True on success, False otherwise.
    """
    one_entity = entity

    logger.debug("Entity value: {0}".format(entity))

    ent_proto = entity_pb.EntityProto()
    ent_proto.ParseFromString(one_entity)
    key = ent_proto.key()
    self.process_statistics(key, ent_proto, len(one_entity))
    return base64.urlsafe_b64encode(key.Encode())

  def create_namespace_entry(self, namespace, size, number, timestamp):
    """ Puts a namespace into the datastore.

    Args:
      namespace: A string, the namespace.
      size: An int representing the number of bytes taken by a namespace.
      number: The total number of entities in a namespace.
      timestamp: A datetime.datetime object.
    """
    entities_to_write = []
    namespace_stat = stats.NamespaceStat(subject_namespace=namespace,
                               bytes=size,
                               count=number,
                               timestamp=timestamp)
    entities_to_write.append(namespace_stat)

    # All application are assumed to have the default namespace.
    if namespace != "":
      namespace_entry = metadata.Namespace(key_name=namespace)
      entities_to_write.append(namespace_entry)

    db.put(entities_to_write)
    logger.debug("Done creating namespace stats")

  def create_kind_stat_entry(self, kind, size, number, timestamp):
    """ Puts a kind statistic into the datastore.

    Args:
      kind: The entity kind.
      size: An int representing the number of bytes taken by entity kind.
      number: The total number of entities.
      timestamp: A datetime.datetime object.
    """
    kind_stat = stats.KindStat(kind_name=kind,
                               bytes=size,
                               count=number,
                               timestamp=timestamp)
    kind_entry = metadata.Kind(key_name=kind)
    entities_to_write = [kind_stat, kind_entry]
    db.put(entities_to_write)
    logger.debug("Done creating kind stat")

  def create_global_stat_entry(self, app_id, size, number, timestamp):
    """ Puts a global statistic into the datastore.

    Args:
      app_id: The application identifier.
      size: The number of bytes of all entities.
      number: The total number of entities of an application.
      timestamp: A datetime.datetime object.
    """
    global_stat = stats.GlobalStat(key_name=app_id,
                                   bytes=size,
                                   count=number,
                                   timestamp=timestamp)
    db.put(global_stat)
    logger.debug("Done creating global stat")

  def clean_up_entities(self):
    # If we have state information beyond what function to use,
    # load the last seen key.
    if (len(self.groomer_state) > 1 and
      self.groomer_state[0] == self.CLEAN_ENTITIES_TASK):
      last_key = self.groomer_state[1]
    else:
      last_key = ""
    while True:
      try:
        logger.debug('Fetching {} entities'.format(self.BATCH_SIZE))
        entities = self.get_entity_batch(last_key)

        if not entities:
          break

        for entity in entities:
          last_key = self.process_entity(entity)

        self.entities_checked += len(entities)
        if time.time() > self.last_logged + self.LOG_PROGRESS_FREQUENCY:
          logger.info('Checked {} entities'.format(self.entities_checked))
          self.last_logged = time.time()
        self.update_groomer_state([self.CLEAN_ENTITIES_TASK, last_key])
      except datastore_errors.Error, error:
        logger.error("Error getting a batch: {0}".format(error))
        time.sleep(self.DB_ERROR_PERIOD)
      except dbconstants.AppScaleDBConnectionError, connection_error:
        logger.error("Error getting a batch: {0}".format(connection_error))
        time.sleep(self.DB_ERROR_PERIOD)

  def register_db_accessor(self, app_id):
    """ Gets a distributed datastore object to interact with
        the datastore for a certain application.

    Args:
      app_id: The application ID.
    Returns:
      A distributed_datastore.DatastoreDistributed object.
    """
    ds_distributed = datastore_distributed.DatastoreDistributed(
      app_id, self.datastore_path)
    apiproxy_stub_map.apiproxy.RegisterStub('datastore_v3', ds_distributed)
    apiproxy_stub_map.apiproxy.RegisterStub('memcache',
      memcache_distributed.MemcacheService(app_id))
    os.environ['APPLICATION_ID'] = app_id
    os.environ['APPNAME'] = app_id
    os.environ['AUTH_DOMAIN'] = "appscale.com"
    return ds_distributed

  def remove_old_statistics(self):
    """ Does a range query on the current batch of statistics and
        deletes them.
    """
    #TODO only remove statistics older than 30 days.
    for app_id in self.stats.keys():
      self.register_db_accessor(app_id)
      query = stats.KindStat.all()
      entities = query.run()
      logger.debug("Result from kind stat query: {0}".format(str(entities)))
      for entity in entities:
        logger.debug("Removing kind {0}".format(entity))
        entity.delete()

      query = stats.GlobalStat.all()
      entities = query.run()
      logger.debug("Result from global stat query: {0}".format(str(entities)))
      for entity in entities:
        logger.debug("Removing global {0}".format(entity))
        entity.delete()
      logger.debug("Done removing old stats for app {0}".format(app_id))

  def update_namespaces(self, timestamp):
    """ Puts the namespace information into the datastore for applications to
        access.

    Args:
      timestamp: A datetime time stamp to know which stat items belong
        together.
    """
    for app_id in self.namespace_info.keys():
      ds_distributed = self.register_db_accessor(app_id)
      namespaces = self.namespace_info[app_id].keys()
      for namespace in namespaces:
        size = self.namespace_info[app_id][namespace]['size']
        number = self.namespace_info[app_id][namespace]['number']
        try:
          self.create_namespace_entry(namespace, size, number, timestamp)
        except (datastore_errors.BadRequestError,
                datastore_errors.InternalError) as error:
          logger.error('Unable to insert namespace info: {}'.format(error))

      logger.info("Namespace for {0} are {1}"\
        .format(app_id, self.namespace_info[app_id]))
      del ds_distributed

  def update_statistics(self, timestamp):
    """ Puts the statistics into the datastore for applications
        to access.

    Args:
      timestamp: A datetime time stamp to know which stat items belong
        together.
    """
    for app_id in self.stats.keys():
      ds_distributed = self.register_db_accessor(app_id)
      total_size = 0
      total_number = 0
      kinds = self.stats[app_id].keys()
      for kind in kinds:
        size = self.stats[app_id][kind]['size']
        number = self.stats[app_id][kind]['number']
        total_size += size
        total_number += number
        try:
          self.create_kind_stat_entry(kind, size, number, timestamp)
        except (datastore_errors.BadRequestError,
                datastore_errors.InternalError) as error:
          logger.error('Unable to insert kind stat: {}'.format(error))

      try:
        self.create_global_stat_entry(app_id, total_size, total_number,
                                      timestamp)
      except (datastore_errors.BadRequestError,
              datastore_errors.InternalError) as error:
        logger.error('Unable to insert global stat: {}'.format(error))

      logger.info("Kind stats for {0} are {1}"\
        .format(app_id, self.stats[app_id]))
      logger.info("Global stats for {0} are total size of {1} with " \
        "{2} entities".format(app_id, total_size, total_number))
      logger.info("Number of hard deletes: {0}".format(self.num_deletes))
      del ds_distributed

  def update_groomer_state(self, state):
    """ Updates the groomer's internal state and persists the state to
    ZooKeeper.

    Args:
      state: A list of strings representing the ID of the task to resume along
        with any additional data about the task.
    """
    zk_data = self.GROOMER_STATE_DELIMITER.join(state)

    # We don't want to crash the groomer if we can't update the state.
    try:
      self.zoo_keeper.update_node(self.GROOMER_STATE_PATH, zk_data)
    except zk.ZKInternalException as zkie:
      logger.exception(zkie)
    self.groomer_state = state

  def run_groomer(self, ds_access):
    """ Runs the grooming process. Loops on the entire dataset sequentially
        and updates stats, indexes, and transactions.
    """
    self.ds_access = ds_access
    index_manager = IndexManager(self.zoo_keeper.handle, self.ds_access)
    self.ds_access.index_manager.composite_index_manager = index_manager

    logger.info("Groomer started")
    start = time.time()

    self.reset_statistics()

    tasks = [
      {
        'id': self.CLEAN_ENTITIES_TASK,
        'description': 'clean up entities',
        'function': self.clean_up_entities,
        'args': []
      }
    ]
    groomer_state = self.zoo_keeper.get_node(self.GROOMER_STATE_PATH)
    logger.info('groomer_state: {}'.format(groomer_state))
    if groomer_state:
      self.update_groomer_state(
        groomer_state[0].split(self.GROOMER_STATE_DELIMITER))

    for task_number in range(len(tasks)):
      task = tasks[task_number]
      if (len(self.groomer_state) > 0 and self.groomer_state[0] != '' and
        self.groomer_state[0] != task['id']):
        continue
      logger.info('Starting to {}'.format(task['description']))
      try:
        task['function'](*task['args'])
        if task_number != len(tasks) - 1:
          next_task = tasks[task_number + 1]
          self.update_groomer_state([next_task['id']])
      except Exception as exception:
        logger.error('Exception encountered while trying to {}:'.
          format(task['description']))
        logger.exception(exception)

    self.update_groomer_state([])

    timestamp = datetime.datetime.utcnow().replace(microsecond=0)

    self.update_statistics(timestamp)
    self.update_namespaces(timestamp)

    del self.ds_access

    time_taken = time.time() - start
    logger.info("Groomer took {0} seconds".format(str(time_taken)))


def main():
  """ This main function allows you to run the groomer manually. """
  logging.getLogger('appscale').setLevel(logging.INFO)

  zk_connection_locations = appscale_info.get_zk_locations_string()
  zookeeper = zk.ZKTransaction(host=zk_connection_locations)

  datastore_path = ':'.join([appscale_info.get_db_proxy(),
                             str(constants.DB_SERVER_PORT)])
  ds_groomer = DatastoreGroomer(zookeeper, 'fdb', datastore_path)

  ds_access = FDBDatastore()
  ds_access.start()

  io_loop = IOLoop.current()

  def groom():
    logger.debug("Trying to get groomer lock.")
    try:
      if ds_groomer.get_groomer_lock():
        logger.info("Got the groomer lock.")
        try:
          ds_groomer.run_groomer(ds_access)
        except Exception as exception:
          logger.exception('Encountered exception {} while running the groomer.'
            .format(str(exception)))
        try:
          ds_groomer.zoo_keeper.release_lock_with_path(zk.DS_GROOM_LOCK_PATH)
        except zk.ZKTransactionException, zk_exception:
          logger.error("Unable to release zk lock {0}.".\
            format(str(zk_exception)))
        except zk.ZKInternalException, zk_exception:
          logger.error("Unable to release zk lock {0}.".\
            format(str(zk_exception)))
        finally:
          zookeeper.close()
      else:
        logger.info("Did not get the groomer lock.")
    finally:
      logger.info("Stopping IO loop")
      io_loop.stop()

  thread = threading.Thread(target=groom, args=())
  thread.start()

  logger.info("Starting IO loop")
  io_loop.start()

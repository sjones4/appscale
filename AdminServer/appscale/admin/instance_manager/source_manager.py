""" Fetches and prepares the source code for revisions. """

import errno
import json
import logging
import os
import random
import shutil

from kazoo.exceptions import NodeExistsError
from kazoo.exceptions import NoNodeError
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.options import options

from appscale.common.appscale_utils import get_md5
from appscale.common.appscale_info import get_secret
from appscale.common.async_retrying import retry_children_watch_coroutine
from appscale.common.constants import VERSION_PATH_SEPARATOR
from .utils import fetch_file
from ..constants import (
  DASHBOARD_APP_ID,
  InvalidSource,
  SOURCES_DIRECTORY,
  UNPACK_ROOT
)
from ..utils import (download_source, extract_source, source_archive_path_for_revision)

logger = logging.getLogger(__name__)


class AlreadyHoster(Exception):
  pass


class SourceManager(object):
  """ Fetches and prepares the source code for revisions. """
  def __init__(self, zk_client, thread_pool):
    """ Creates a new SourceManager.

    Args:
      zk_client: A KazooClient.
      thread_pool: A ThreadPoolExecutor.
    """
    self.zk_client = zk_client
    self.thread_pool = thread_pool
    self.source_futures = {}
    self.projects_manager = None
    self.fetched_revisions = None

  def configure_automatic_fetch(self, project_manager):
    """ Ensures that SourceManager watches zookeeper nodes describing
    application archives and fetches it as soon as new archive
    is available.

    Args:
      project_manager: an instance of GlobalProjectManager.
    """
    if self.projects_manager is not None:
      logger.debug("Automatic fetch of new application archives "
                   "was already configured")
      return
    self.projects_manager = project_manager
    self.fetched_revisions = set()
    self.zk_client.ChildrenWatch('/apps', self._update_apps_watch)
    logger.debug("Configured automatic fetch of new application archives")

  def _update_apps_watch(self, new_apps_list):
    """ Schedules fetch of new sources if there are any available.

    Args:
      new_apps_list: a list of revisions with archive
        available somewhere in deployment.
    """
    persistent_update_apps = retry_children_watch_coroutine(
      '/apps', self._handle_apps_update
    )
    main_io_loop = IOLoop.current()
    main_io_loop.add_callback(persistent_update_apps, new_apps_list)

  @gen.coroutine
  def _handle_apps_update(self, new_apps_list):
    """ Fetches new available sources if there are any.

    Args:
      new_apps_list: a list of revisions with archive
        available somewhere in deployment.
    """
    for revision_key in new_apps_list:
      if revision_key in self.fetched_revisions:
        continue
      revision_key_parts = revision_key.split(VERSION_PATH_SEPARATOR)
      project_id = revision_key_parts[0]
      service_id = revision_key_parts[1]
      version_id = revision_key_parts[2]
      revision_id = revision_key_parts[3]

      service_manager = self.projects_manager[project_id][service_id]
      version_details = service_manager[version_id].version_details
      runtime = version_details['runtime']
      source_archive = version_details['deployment']['zip']['sourceUrl']
      last_revision_id = version_details['revision']
      if revision_id == last_revision_id:
        yield self.ensure_source(revision_key, source_archive, runtime)
        self.fetched_revisions.add(revision_key)

  @gen.coroutine
  def fetch_archive(self, revision_key, source_location):
    """ Copies the source archive from a machine that has it.

    Args:
      revision_key: A string specifying a revision key.
      source_location: A string specifying the location of the version's
        source archive.
    Returns:
      A string specifying the source archive's MD5 hex digest.
    Raises:
      AlreadyHoster if local machine is hosting archive.
    """
    local_source_archive = source_archive_path_for_revision(revision_key)
    hosts_with_archive = yield self.thread_pool.submit(
      self.zk_client.get_children, '/apps/{}'.format(revision_key))
    hosts_with_archive = hosts_with_archive or []

    if (os.path.isfile(local_source_archive) and
            options.private_ip in hosts_with_archive):
      raise AlreadyHoster('{} is already a hoster of {}'
                         .format(options.private_ip, revision_key))

    if not source_location.startswith("gs://"):
      assert hosts_with_archive, '{} has no hosters'.format(revision_key)
      host = random.choice(hosts_with_archive)
      host_node = '/apps/{}/{}'.format(revision_key, host)
      original_md5, _ = yield self.thread_pool.submit(
          self.zk_client.get, host_node)
      if os.path.isfile(local_source_archive):
        md5 = yield self.thread_pool.submit(get_md5, local_source_archive)
        if md5 == original_md5:
          raise gen.Return(md5)
        else:
          logger.warning('Source MD5 does not match. Re-fetching archive.')
      yield self.thread_pool.submit(fetch_file, host, local_source_archive)
    else:
      original_md5 = None
      gcs_config_json, _ = yield self.thread_pool.submit(self.zk_client.get, '/appscale/config/gcs')
      try:
        gcs_config = json.loads(gcs_config_json)
      except NoNodeError:
        raise InvalidSource('Source location uses gcs, but gcs not configured')
      yield self.thread_pool.submit(download_source, gcs_config, source_location, local_source_archive)

    md5 = yield self.thread_pool.submit(get_md5, local_source_archive)
    if original_md5 and md5 != original_md5:
      raise InvalidSource('Source MD5 does not match')

    raise gen.Return(md5)

  @gen.coroutine
  def register_as_hoster(self, revision_key, md5):
    """ Adds an entry to indicate that the local machine has the archive.

    Args:
      revision_key: A string specifying a revision key.
      md5: A string specifying the source archive's MD5 hex digest.
    """
    new_hoster_node = '/apps/{}/{}'.format(revision_key, options.private_ip)
    try:
      yield self.thread_pool.submit(self.zk_client.create, new_hoster_node,
                                    md5, makepath=True)
    except NodeExistsError:
      logger.debug('{} is already a hoster'.format(options.private_ip))

  @gen.coroutine
  def prepare_source(self, revision_key, location, runtime):
    """ Fetches and extracts the source code for a revision.

    Args:
      revision_key: A string specifying a revision key.
      location: A string specifying the location of the revision's source
        remote archive location.
      runtime: A string specifying the revision's runtime.
    """
    source_extracted = False
    try:
      md5 = yield self.fetch_archive(revision_key, location)
    except AlreadyHoster as already_hoster_err:
      logger.info(already_hoster_err)
      source_extracted = os.path.isdir(os.path.join(UNPACK_ROOT, revision_key))
    else:
      yield self.register_as_hoster(revision_key, md5)

    if not source_extracted:
      yield self.thread_pool.submit(extract_source, revision_key,
                                    source_archive_path_for_revision(revision_key),
                                    runtime)

    project_id = revision_key.split(VERSION_PATH_SEPARATOR)[0]
    if project_id == DASHBOARD_APP_ID:
      self.update_secret(revision_key)

  @gen.coroutine
  def ensure_source(self, revision_key, location, runtime):
    """ Wait until the revision source is ready.

    If this method has been previously called for the same revision, it waits
    for the same future. This prevents the same archive from being fetched
    and extracted multiple times.

    Args:
      revision_key: A string specifying the revision key.
      location: A string specifying the location of the source archive.
      runtime: A string specifying the revision's runtime.
    """
    future = self.source_futures.get(revision_key)
    if future is None or (future.done() and future.exception() is not None):
      future = self.prepare_source(revision_key, location, runtime)
      self.source_futures[revision_key] = future

    yield future

  def clean_old_revisions(self, active_revisions):
    """ Cleans up the source code for old revisions.

    Args:
      active_revisions: A set of strings specifying active revision keys.
    """
    # Remove unneeded source directories.
    for source_directory in os.listdir(UNPACK_ROOT):
      if source_directory not in active_revisions:
        shutil.rmtree(os.path.join(UNPACK_ROOT, source_directory),
                      ignore_errors=True)

    # Remove obsolete archives.
    futures_to_clear = []
    for revision_key, future in self.source_futures.items():
      if not future.done():
        continue

      if revision_key in active_revisions:
        continue

      archive_location = os.path.join(SOURCES_DIRECTORY,
                                      '{}.tar.gz'.format(revision_key))
      try:
        os.remove(archive_location)
      except OSError as error:
        if error.errno != errno.ENOENT:
          raise

        logger.debug(
          '{} did not exist when trying to remove it'.format(archive_location))

      futures_to_clear.append(revision_key)

    for revision_key in futures_to_clear:
      del self.source_futures[revision_key]

  @staticmethod
  def update_secret(revision_key):
    """ Ensures the revision's secret matches the deployment secret. """
    deployment_secret = get_secret()
    revision_base = os.path.join(UNPACK_ROOT, revision_key)
    secret_module = os.path.join(revision_base, 'app', 'lib', 'secret_key.py')
    with open(secret_module) as secret_file:
      revision_secret = secret_file.read().split()[-1][1:-1]
      if revision_secret == deployment_secret:
        return

    with open(secret_module, 'w') as secret_file:
      secret_file.write("GLOBAL_SECRET_KEY = '{}'".format(deployment_secret))

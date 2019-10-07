"""
Postgres connection wrapper with autoreconnect functionality.
"""
import psycopg2
from tornado.ioloop import IOLoop

from appscale.common import retrying
from appscale.taskqueue.utils import logger


class NoDSNSpecified(Exception):
  pass


class PostgresConnectionWrapper(object):
  """ Implements automatic reconnection to Postgresql server. """

  def __init__(self, dsn=None):
    self._dsn = dsn
    self._connection = None

  def set_dsn(self, dsn):
    """ Resets PostgresConnectionWrapper to use new DSN string.

    Args:
      dsn: a str representing Postgres DSN string.
    """
    if self._connection and not self._connection.closed:
      self.close()
      self._connection = None
    self._dsn = dsn

  @retrying.retry(retrying_timeout=60, backoff_multiplier=1)
  def connection(self):
    """ Provides postgres connection. It can either return existing
    working connection or establish new one.

    Returns:
      A context manager for a psycopg2 connection.
    """
    connection = self._connection
    if connection and not connection.closed and connection.info.status:
      # handle underlying connection close
      logger.info('Closing connection on lease ({})'.format(str(connection.info.status)))
      try:
        connection.close()
      except psycopg2.Error as e:
        logger.info('Error closing connection on lease: {}'.format(str(e)))
    if not connection or connection.closed:
      logger.info('Establishing new connection to Postgres server')
      connection = psycopg2.connect(dsn=self._dsn)
      self._connection = connection
    return PostgresConnectionContextManager(connection)

  def close(self):
    """ Closes psycopg2 connection.
    """
    return self._connection.close()


class PostgresConnectionContextManager(object):
  def __init__(self, connection):
    """Connection should have been checked"""
    self._connection = connection

  def _should_close(self, exc_type, exc_value, traceback):
    close = False
    connection = self._connection
    if not connection.closed and connection.info.status: # and exc_value:  TODO:STEVE: should close on (some?) errors?
      logger.info('Closing connection to Postgres server on release ({}): {}'.format(str(connection.info.status), str(exc_value)))
      close = True
    return close

  def __enter__(self):
    logger.info('Leasing connection to Postgres server')
    return self._connection.__enter__()

  def __exit__(self, exc_type, exc_value, traceback):
    logger.info('Releasing connection to Postgres server')
    try:
      return self._connection.__exit__(exc_type, exc_value, traceback)
    except psycopg2.Error as e:
      logger.info('Error exiting connection on release: {}'.format(str(e)))
      # allow original exception to propagate when present, else re-raise
      if exc_value is None:
        raise e
    finally:
      if self._should_close(exc_type, exc_value, traceback):
        try:
          self._connection.close()
        except psycopg2.Error as e:
          logger.info('Error closing connection on release: {}'.format(str(e)))
    return exc_value is None  # False if an exception so it propagates


def start_postgres_dsn_watch(zk_client):
  """ Created zookeeper DataWatch for updating pg_wrapper
  when Postgres DSN string is updated.

  Args:
    zk_client: an instance of zookeeper client.
  """
  zk_client.ensure_path('/appscale/tasks')
  zk_client.DataWatch('/appscale/tasks/postgres_dsn', _update_dsn_watch)


def _update_dsn(new_dsn):
  """ Updates Postgres DSN string to be used
  for establishing connection to Postgresql server.

  Args:
    new_dsn: A bytes array representing new DSN string.
  """
  if not new_dsn:
    raise NoDSNSpecified('No DSN string was found at zookeeper node '
                         '"/appscale/tasks/postgres_dsn"')
  pg_wrapper.set_dsn(new_dsn.decode('utf-8'))


def _update_dsn_watch(new_dsn, _):
  """ Schedules update of Postgres DSN to be executed in tornado IO loop.

  Args:
    new_dsn: A bytes array representing new DSN string.
  """
  main_io_loop = IOLoop.instance()
  main_io_loop.add_callback(_update_dsn, new_dsn)


pg_wrapper = PostgresConnectionWrapper()

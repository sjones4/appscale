"""
Distributed id and lock service for transaction support.
"""

class ZKTimeoutException(Exception):
  """ A special Exception class that should be thrown if a function is
  taking longer than expected by the caller to run
  """
  pass

class ZKTransactionException(Exception):
  """ ZKTransactionException defines a custom exception class that should be
  thrown whenever there was a problem involving a transaction (e.g., the
  transaction failed, we couldn't get a transaction ID).
  """
  pass

class ZKInternalException(Exception):
  """ ZKInternalException defines a custom exception class that should be
  thrown whenever we cannot connect to ZooKeeper for an extended amount of time.
  """
  pass

class ZKBadRequest(ZKTransactionException):
  """ A class thrown when there are too many locks acquired in a XG transaction
  or when XG operations are done on a non XG transaction.
  """
  pass

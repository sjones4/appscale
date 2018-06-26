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

"""
Author: Navraj Chohan <nlake44@gmail.com>
Modifications for AppScale
Implementation of Blobstore stub storage based on the datastore

Contains implementation of blobstore_stub.BlobStorage that writes
blobs into the AppScale backends. Blobs are split into chunks of 
1MB segments. 

"""
from google.appengine.ext.blobstore.blobstore import BlobReader
from google.appengine.api import blobstore
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.api import datastore
from google.appengine.api import datastore_types

__all__ = ['DatastoreBlobStorage']

# The datastore kind used for storing chunks of a blob
_BLOB_CHUNK_KIND_ = "__BlobChunk__"

class DatastoreBlobStorage(blobstore_stub.BlobStorage):
  """Storage mechanism for storing blob data in datastore."""

  def __init__(self, app_id):
    """Constructor.

    Args:
      app_id: App id to store blobs on behalf of.
    """
    self._app_id = app_id

  @classmethod
  def _BlobKey(cls, blob_key):
    """Normalize to instance of BlobKey.
 
    Args:
      blob_key: A blob key of a blob to store.
    Returns:
      A normalized blob key of class BlobKey.
    """
    if not isinstance(blob_key, blobstore.BlobKey):
      return blobstore.BlobKey(unicode(blob_key))
    return blob_key

  def StoreBlob(self, blob_key, blob_stream):
    """Store blob stream to the datastore.

    Args:
      blob_key: Blob key of blob to store.
      blob_stream: Stream or stream-like object that will generate blob content.
    """
    block_count = 0
    blob_key_object = self._BlobKey(blob_key)
    while True:
      block = blob_stream.read(blobstore.MAX_BLOB_FETCH_SIZE)
      if not block:
        break
      entity = datastore.Entity(_BLOB_CHUNK_KIND_, 
                                name=str(blob_key_object) + "__" + str(block_count), 
                                namespace='')
      entity.update({'block': datastore_types.Blob(block)})
      datastore.Put(entity)
      block_count += 1

  def OpenBlob(self, blob_key):
    """Open blob file for streaming.

    Args:
      blob_key: Blob-key of existing blob to open for reading.

    Returns:
      Open file stream for reading blob from the datastore.
    """
    return BlobReader(blob_key, blobstore.MAX_BLOB_FETCH_SIZE, 0)

  @datastore.NonTransactional
  def DeleteBlob(self, blob_key):
    """Delete blob data from the datastore.

    Args:
      blob_key: Blob-key of existing blob to delete.
    Raises:
      ApplicationError: When there is a datastore issue when deleting a blob.
    """
    # Discover all the keys associated with the blob.
    start_key_name = ''.join([str(blob_key), '__'])
    # The chunk key suffixes are all digits, so 'a' is past them.
    end_key_name = ''.join([start_key_name, 'a'])
    start_key = datastore.Key.from_path(_BLOB_CHUNK_KIND_, start_key_name,
                                        namespace='')
    end_key = datastore.Key.from_path(_BLOB_CHUNK_KIND_, end_key_name,
                                      namespace='')
    filters = {'__key__ >': start_key, '__key__ <': end_key}
    query = datastore.Query(filters=filters, keys_only=True)

    keys = list(query.Run())
    datastore.Delete(keys)

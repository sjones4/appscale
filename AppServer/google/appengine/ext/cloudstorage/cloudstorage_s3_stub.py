# Copyright 2019 AppScale Systems, Inc
#
# SPDX-License-Identifier: Apache-2.0
"""Stub for S3 backed AppScale Google Cloud Storage."""

import boto
import calendar
import datetime
import hashlib
import httplib
import StringIO
import boto

from boto.s3.connection import OrdinaryCallingFormat
from google.appengine.api import app_identity
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.ext.cloudstorage import common

_GCS_DEFAULT_CONTENT_TYPE = 'binary/octet-stream'


class _GCSS3FileInfoKey_(object):
  _encoder = blobstore_stub.BlobstoreServiceStub.CreateEncodedGoogleStorageKey

  def name(self):
      return self._encoder('{}/{}'.format(self.bucket_name, self.key_name))


class _GCSS3FileInfo_(object):
  """Store GCS specific info.

  GCS allows user to define arbitrary metadata via header x-goog-meta-foo: bar.
  These headers are returned when user does a GET or HEAD on the object.

  Key name is blobkey.
  """

  def __init__(self):
    self.bucket_name = None
    self.key_name = None
    self.etag = None
    self.creation = datetime.datetime.utcnow()
    self.size = None
    self.finalized = False
    self.next_offset = -1

  def key(self):
    encoded_key = _GCSS3FileInfoKey_()
    encoded_key.bucket_name = self.bucket_name
    encoded_key.key_name = self.key_name
    return encoded_key


class CloudStorageS3Stub(object):
  """S3 backed Google Cloud Storage stub implementation.

  We use S3 to store files and to track upload state.

  Note: this Google Cloud Storage stub is designed to work with
  apphosting.ext.cloudstorage.storage_api.py.
  It only implements the part of GCS storage_api.py uses, and its interface
  maps to GCS XML APIs.
  """
  BLOBKEY_PREFIX = 'GSMUID:'  # TODO use regular blobstore_stub.BlobstoreServiceStub.CreateEncodedGoogleStorageKey encoding

  @classmethod
  def _connect_s3(cls):
    return boto.connect_s3()

  @classmethod
  def _get_upload_id_from_token(cls, token):
    """ Extract the GCS multipart upload identifier from a token

    Args:
      token: The token.

    Returns:
      a multipart upload identifer used for uploading parts, etc.
    """
    if token.startswith(cls.BLOBKEY_PREFIX):
      return token[len(cls.BLOBKEY_PREFIX):]
    else:
      return None

  def post_start_creation(self, filename, options):
    """Start object creation with a POST.

    This implements the resumable upload XML API.

    Args:
      filename: gcs filename of form /bucket/filename.
      options: a dict containing all user specified request headers.
        e.g. {'content-type': 'foo', 'x-goog-meta-bar': 'bar'}.

    Returns:
      a token (blobkey) used for continuing upload.
    """
    common.validate_file_path(filename)
    bucket_name = app_identity.get_default_gcs_bucket_name()
    if not filename.startswith('/{}/'.format(bucket_name)):
      raise ValueError('Invalid filename', httplib.BAD_REQUEST)

    gs = self._connect_s3()
    bucket_name = app_identity.get_default_gcs_bucket_name()
    bucket = gs.get_bucket(bucket_name, validate=False)
    filename = filename[len(bucket_name)+2:]
    upload = bucket.initiate_multipart_upload(filename,
                                              metadata=options,
                                              policy='public-read')
    return self.BLOBKEY_PREFIX + upload.id

  def put_continue_creation(self, token, content, content_range,
                            length=None,
                            _upload_filename=None,
                            _options=None):
    """Continue object upload with PUTs.

    This implements the resumable upload XML API.

    Args:
      token: upload token returned by post_start_creation.
      content: object content. None if no content was provided with this
        PUT request.
      content_range: a (start, end) tuple specifying the content range of this
        chunk. Both are inclusive according to XML API. None if content is
        None.
      length: file length, if this is the last chunk of file content.
      _upload_filename: internal use. Might be removed any time! This is
        used by blobstore to pass in the upload filename from user.

    Returns:
      _GCSS3FileInfo_ for this file if the file is finalized.

    Raises:
      ValueError: if something is invalid. The exception.args is a tuple of
      (msg, http status code).
    """
    gcs_muid = self._get_upload_id_from_token(token)
    if not gcs_muid:
      raise ValueError('Invalid token', httplib.BAD_REQUEST)
    if not content:
      return None

    start, end = content_range
    if (start != 0):
      raise ValueError('Unsupported content range %d-%d' % content_range,
                       httplib.REQUESTED_RANGE_NOT_SATISFIABLE)
    if len(content) != (end - start + 1):
      raise ValueError('Invalid content range %d-%d' % content_range,
                       httplib.REQUESTED_RANGE_NOT_SATISFIABLE)

    bucket_name = app_identity.get_default_gcs_bucket_name()
    gs = self._connect_s3()
    bucket = gs.get_bucket(bucket_name, validate=False)
    uploads = bucket.get_all_multipart_uploads()
    for upload in uploads:
      if upload.id == gcs_muid:
        key_name = upload.key_name
        part_content = StringIO.StringIO(content)
        upload.upload_part_from_file(part_content, 1)
        complete_upload = upload.complete_upload()
        if _options:
          key = bucket.get_key(key_name, validate=False)
          key.copy(bucket_name, key_name, metadata=_options)
          key.set_acl('public-read')
        gcs_file = _GCSS3FileInfo_()
        gcs_file.bucket_name = bucket_name
        gcs_file.key_name = key_name
        gcs_file.etag = complete_upload.etag  # Not content md5 since MPU
        gcs_file.size = len(content)
        gcs_file.finalized = True
        return gcs_file

    raise ValueError('Invalid token', httplib.BAD_REQUEST)

  # def put_copy(self, src, dst):
  #   """Copy file from src to dst.
  #
  #   Metadata is copied.
  #
  #   Args:
  #     src: /bucket/filename. This file must exist.
  #     dst: /bucket/filename
  #   """
  #   common.validate_file_path(src)
  #   common.validate_file_path(dst)
  #
  #
  #   ns = namespace_manager.get_namespace()
  #   try:
  #     namespace_manager.set_namespace('')
  #     src_blobkey = self._filename_to_blobkey(src)
  #     source = _AE_GCSFileInfo_.get_by_key_name(src_blobkey)
  #     token = self._filename_to_blobkey(dst)
  #     new_file = _AE_GCSFileInfo_(key_name=token,
  #                                 filename=dst,
  #                                 finalized=True)
  #     new_file.options = source.options
  #     new_file.etag = source.etag
  #     new_file.size = source.size
  #     new_file.creation = datetime.datetime.utcnow()
  #     new_file.put()
  #   finally:
  #     namespace_manager.set_namespace(ns)
  #
  #
  #   local_file = self.blob_storage.OpenBlob(src_blobkey)
  #   self.blob_storage.StoreBlob(token, local_file)

  def _get_content(self, gcs_file):
    """Aggregate all partial content of the gcs_file.

    Args:
      gcs_file: an instance of _AE_GCSFileInfo_.

    Returns:
      (error_msg, content) tuple. error_msg is set if the file is
      corrupted during upload. Otherwise content is set to the
      aggregation of all partial contents.
    """
    content = ''
    previous_end = 0
    error_msg = ''
    for partial in (_AE_GCSPartialFile_.all(namespace='').ancestor(gcs_file).
                    order('__key__')):
      start = int(partial.key().name())
      if not error_msg:
        if start < previous_end:
          error_msg = 'File is corrupted due to missing chunks.'
        elif start > previous_end:
          error_msg = 'File is corrupted due to overlapping chunks'
        previous_end = partial.end
        content += self.blob_storage.OpenBlob(partial.partial_content).read()
        self.blob_storage.DeleteBlob(partial.partial_content)
      partial.delete()
    if error_msg:
      gcs_file.delete()
      content = ''
    return error_msg, content

  # def get_bucket(self,
  #                bucketpath,
  #                prefix,
  #                marker,
  #                max_keys,
  #                delimiter):
  #   """Get bucket listing with a GET.
  #
  #   How GCS listbucket work in production:
  #   GCS tries to return as many items as possible in a single response. If
  #   there are more items satisfying user's query and the current request
  #   took too long (e.g spent on skipping files in a subdir) or items to return
  #   gets too big (> max_keys), it returns fast and sets IsTruncated
  #   and NextMarker for continuation. They serve redundant purpose: if
  #   NextMarker is set, IsTruncated is True.
  #
  #   Note NextMarker is not where GCS scan left off. It is
  #   only valid for the exact same type of query the marker was generated from.
  #   For example, if a marker is generated from query with delimiter, the marker
  #   is the name of a subdir (instead of the last file within the subdir). Thus
  #   you can't use this marker to issue a query without delimiter.
  #
  #   Args:
  #     bucketpath: gcs bucket path of form '/bucket'
  #     prefix: prefix to limit listing.
  #     marker: a str after which to start listing. Exclusive.
  #     max_keys: max items we scan & return.
  #     delimiter: delimiter for directory.
  #
  #   See https://developers.google.com/storage/docs/reference-methods#getbucket
  #   for details.
  #
  #   Returns:
  #     A tuple of (a list of GCSFileStat for files or directories sorted by
  #     filename, next_marker to use as next marker, is_truncated boolean to
  #     indicate if there are more results satisfying query).
  #   """
  #   common.validate_bucket_path(bucketpath)
  #   q = _AE_GCSFileInfo_.all(namespace='')
  #   fully_qualified_prefix = '/'.join([bucketpath, prefix])
  #   if marker:
  #     q.filter('filename >', '/'.join([bucketpath, marker]))
  #   else:
  #     q.filter('filename >=', fully_qualified_prefix)
  #
  #   result = set()
  #   name = None
  #   first = True
  #   first_dir = None
  #   for info in q.run():
  #
  #     if not info.filename.startswith(fully_qualified_prefix):
  #       break
  #     if len(result) == max_keys:
  #       break
  #
  #
  #     info = db.get(info.key())
  #     if not info:
  #       continue
  #
  #     name = info.filename
  #     if delimiter:
  #
  #       start_index = name.find(delimiter, len(fully_qualified_prefix))
  #       if start_index != -1:
  #         name = name[:start_index + len(delimiter)]
  #
  #
  #         if marker and (first or name == first_dir):
  #           first = False
  #           first_dir = name
  #
  #         else:
  #           result.add(common.GCSFileStat(name, st_size=None,
  #                                         st_ctime=None, etag=None,
  #                                         is_dir=True))
  #         continue
  #
  #     first = False
  #     result.add(common.GCSFileStat(
  #         filename=name,
  #         st_size=info.size,
  #         st_ctime=calendar.timegm(info.creation.utctimetuple()),
  #         etag=info.etag))
  #
  #   def is_truncated():
  #     """Check if there are more results satisfying the query."""
  #     if not result:
  #       return False
  #     q = _AE_GCSFileInfo_.all(namespace='')
  #     q.filter('filename >', name)
  #     info = None
  #
  #     if delimiter and name.endswith(delimiter):
  #
  #       for info in q.run():
  #         if not info.filename.startswith(name):
  #           break
  #       if info.filename.startswith(name):
  #         info = None
  #     else:
  #       info = q.get()
  #     if (info is None or
  #         not info.filename.startswith(fully_qualified_prefix)):
  #       return False
  #     return True
  #
  #   result = list(result)
  #   result.sort()
  #   truncated = is_truncated()
  #   next_marker = name if truncated else None
  #
  #   return result, next_marker, truncated

  # def get_object(self, filename, start=0, end=None):
  #   """Get file content with a GET.
  #
  #   Args:
  #     filename: gcs filename of form '/bucket/filename'.
  #     start: start offset to request. Inclusive.
  #     end: end offset to request. Inclusive.
  #
  #   Returns:
  #     The segment of file content requested.
  #
  #   Raises:
  #     ValueError: if file doesn't exist.
  #   """
  #   common.validate_file_path(filename)
  #   blobkey = self._filename_to_blobkey(filename)
  #   key = blobstore_stub.BlobstoreServiceStub.ToDatastoreBlobKey(blobkey)
  #   gcsfileinfo = db.get(key)
  #   if not gcsfileinfo or not gcsfileinfo.finalized:
  #     raise ValueError('File does not exist.')
  #   local_file = self.blob_storage.OpenBlob(blobkey)
  #   local_file.seek(start)
  #   if end:
  #     return local_file.read(end - start + 1)
  #   else:
  #     return local_file.read()

  # def head_object(self, filename):
  #   """Get file stat with a HEAD.
  #
  #   Args:
  #     filename: gcs filename of form '/bucket/filename'
  #
  #   Returns:
  #     A GCSFileStat object containing file stat. None if file doesn't exist.
  #   """
  #   common.validate_file_path(filename)
  #   blobkey = self._filename_to_blobkey(filename)
  #   key = blobstore_stub.BlobstoreServiceStub.ToDatastoreBlobKey(blobkey)
  #   info = db.get(key)
  #   if info and info.finalized:
  #     metadata = common.get_metadata(info.options)
  #     filestat = common.GCSFileStat(
  #         filename=info.filename,
  #         st_size=info.size,
  #         etag=info.etag,
  #         st_ctime=calendar.timegm(info.creation.utctimetuple()),
  #         content_type=info.content_type,
  #         metadata=metadata)
  #     return filestat
  #   return None

  # def delete_object(self, filename):
  #   """Delete file with a DELETE.
  #
  #   Args:
  #     filename: gcs filename of form '/bucket/filename'
  #
  #   Returns:
  #     True if file is deleted. False if file doesn't exist.
  #   """
  #   common.validate_file_path(filename)
  #   blobkey = self._filename_to_blobkey(filename)
  #   key = blobstore_stub.BlobstoreServiceStub.ToDatastoreBlobKey(blobkey)
  #   gcsfileinfo = db.get(key)
  #   if not gcsfileinfo:
  #     return False
  #
  #   blobstore_stub.BlobstoreServiceStub.DeleteBlob(blobkey,
  #                                                  self.blob_storage)
  #   return True

""" Implements a Cloud Storage stub using an AppScale Cloud Storage server. """
import base64
import datetime
import httplib
import requests
import urlparse

from google.appengine.api import datastore
from google.appengine.api.blobstore import blobstore_stub
from google.appengine.ext.cloudstorage import common
from xml.etree import ElementTree as ETree


class _GCSS3FileInfoKey_(object):
    _encoder = blobstore_stub.BlobstoreServiceStub.CreateEncodedGoogleStorageKey

    def name(self):
        return self._encoder('{}/{}'.format(self.bucket_name, self.key_name))


class _GCSS3FileInfo_(object):
    """Store GCS specific info.

    GCS allows user to define arbitrary metadata via header x-goog-meta-foo: bar
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


class InternalError(Exception):
  """ Indicates that there was a problem fulfilling an API request. """
  pass


class AppScaleCloudStorageStub(object):
  """ A Cloud Storage stub implementation.

  Uses an AppScale Cloud Storage server to store files.

  Note: this Cloud Storage stub is designed to work with
  apphosting.ext.cloudstorage.storage_api.py.
  It only implements the part of GCS storage_api.py uses, and its interface
  maps to GCS XML APIs.
  """

  def __init__(self, acs_location):
    """Initialize.

    Args:
      acs_location: The location of an AppScale Cloud Storage server.
    """
    self.location = acs_location

  def post_start_creation(self, filename, options):
    """Start object creation with a POST.

    This uses the resumable upload XML API.

    Args:
      filename: gs filename of form /bucket/filename.
      options: a dict containing all user specified request headers.
        e.g. {'content-type': 'foo', 'x-goog-meta-bar': 'bar'}.

    Returns:
      a token used for continuing upload. Also used as blobkey to store
    the content.
    """
    common.validate_file_path(filename)

    url = ''.join([self.location, filename])
    options['x-goog-resumable'] = 'start'

    response = requests.post(url, headers=options)

    # The API should response to the initial create call with a 201.
    if response.status_code != httplib.CREATED:
      raise InternalError(response.text)

    location = response.headers.get('Location')
    if location is None:
      raise InternalError('Missing location from response')

    query = urlparse.urlparse(location).query
    try:
      token = urlparse.parse_qs(query)['upload_id'][0]
    except KeyError:
      raise InternalError('Upload ID missing from response')

    # To keep this class stateless, include the filename with the token.
    return base64.b64encode(':'.join([filename, token]))

  def put_continue_creation(self, token, content, content_range,
                            length=None, _upload_filename=None):
    """Continue object upload with PUTs.

    This uses the resumable upload XML API.

    Args:
      token: upload token returned by post_start_creation.
      content: object content.
      content_range: a (start, end) tuple specifying the content range of this
        chunk. Both are inclusive according to XML API.
      length: file length, if this is the last chunk of file content.
      _upload_filename: internal use. Might be removed any time! This is
        used by blobstore to pass in the upload filename from user.

    Raises:
      ValueError: if token is invalid.
    """
    last = length is not None
    filename, token = base64.b64decode(token).split(':', 1)
    common.validate_file_path(filename)

    url = ''.join([self.location, filename, '?upload_id=', token])

    start, end = content_range
    total_size = '*'
    if last:
      total_size = end + 1
    range_header = 'bytes {}-{}/{}'.format(start, end, total_size)
    headers = {'Content-Length': str(len(content)),
               'Content-Range': range_header}

    response = requests.put(url, headers=headers, data=content)

    # The API uses "308 Resume Incomplete" when the client needs to submit
    # more chunks to complete the blob.
    if not last and response.status_code != 308:
      raise InternalError(response.text)

    if last:
      if response.status_code not in (httplib.OK, httplib.CREATED):
        raise InternalError(response.text)

      bucket_name_end = filename.find('/', 1)
      bucket_name = filename[1:bucket_name_end]
      key_name = filename[bucket_name_end + 1:]

      gcs_file = _GCSS3FileInfo_()
      gcs_file.bucket_name = bucket_name
      gcs_file.key_name = key_name
      gcs_file.size = len(content)
      gcs_file.finalized = True

      blob_info = datastore.Entity('__BlobInfo__',
                                   name=gcs_file.key().name(),
                                   namespace='')
      blob_info['creation'] = gcs_file.creation
      blob_info['filename'] = _upload_filename
      blob_info['md5_hash'] = gcs_file.etag
      blob_info['size'] = gcs_file.size
      datastore.Put(blob_info)

      return gcs_file
    else:
      return None

  def get_bucket(self,
                 bucketpath,
                 prefix,
                 marker,
                 max_keys,
                 delimiter):
    """Get bucket listing with a GET.

    Args:
      bucketpath: gs bucket path of form '/bucket'
      prefix: prefix to limit listing
      marker: a str after which to start listing
      max_keys: max size of listing
      delimiter: delimiter for directory

    See https://developers.google.com/storage/docs/reference-methods#getbucket
    for details.

    Returns:
      A tuple of (a list of GCSFileStat for files or directories sorted by
      filename, next_marker to use as next marker, is_truncated boolean to
      indicate if there are more results satisfying query).
    """
    common.validate_bucket_path(bucketpath)

    url = ''.join([self.location, bucketpath])
    params_raw = {
        'prefix': prefix,
        'marker': marker,
        'max-keys': max_keys,
        'delimiter': delimiter
    }
    params = {}
    params.update({key: str(val) for key, val in
                   params_raw.items() if val is not None})
    response = requests.get(url, params=params)

    result = set()
    next_marker = None
    truncated = False
    if response.status_code == httplib.OK:
      response_tr = ETree.fromstring(response.text)
      ns = {'s3': 'http://doc.s3.amazonaws.com/2006-03-01'}
      next_marker = response_tr.findtext('s3:NextMarker', namespaces=ns)
      truncated_text = response_tr.findtext('s3:IsTruncated', namespaces=ns)
      truncated = truncated_text == 'true'
      for content_el in response_tr.findall('s3:Contents', namespaces=ns):
        key_text = content_el.findtext('s3:Key', namespaces=ns)
        last_modified_text = content_el.findtext('s3:LastModified',
                                                 namespaces=ns)
        etag_text = content_el.findtext('s3:ETag', namespaces=ns)
        size_text = content_el.findtext('s3:Size', namespaces=ns)
        result.add(common.GCSFileStat(
            filename=''.join([bucketpath, '/', key_text]),
            st_size=int(size_text),
            st_ctime=common.dt_str_to_posix(last_modified_text),
            etag=etag_text.strip('"')))

    result = list(result)
    result.sort()
    if not truncated:
      next_marker = None

    return result, next_marker, truncated

  def get_object(self, filename, start=0, end=None):
    """Get file content with a GET.

    Args:
      filename: gs filename of form '/bucket/filename'.
      start: start offset to request. Inclusive.
      end: end offset to request. Inclusive.

    Returns:
      The segment of file content requested.

    Raises:
      ValueError: if file doesn't exist.
    """
    common.validate_file_path(filename)

    url = ''.join([self.location, filename])
    headers = {}
    if start != 0 or end is not None:
      headers['Range'] = 'bytes={}-{}'.format(start, end or '')

    response = requests.get(url, headers=headers)
    return response.content

  def head_object(self, filename):
    """Get file stat with a HEAD.

    Args:
      filename: gs filename of form '/bucket/filename'

    Returns:
      A CSFileStat object containing file stat. None if file doesn't exist.
    """
    common.validate_file_path(filename)

    url = ''.join([self.location, filename])
    response = requests.head(url)

    if response.status_code == httplib.OK:
        content_length_text = response.headers.get('Content-Length')
        content_type_text = response.headers.get('Content-Type')
        last_modified_text = response.headers.get('Last-Modified')
        etag_text = response.headers.get('ETag')
        return common.GCSFileStat(
            filename=filename,
            st_size=int(content_length_text),
            etag=etag_text.strip('"'),
            st_ctime=common.http_time_to_posix(last_modified_text),
            content_type=content_type_text)
    return None

  def delete_object(self, filename):
    """Delete file with a DELETE.

    Args:
      filename: gs filename of form '/bucket/filename'

    Returns:
      True if file is deleted. False if file doesn't exist.
    """
    common.validate_file_path(filename)

    url = ''.join([self.location, filename])
    response = requests.delete(url)

    # The API uses a 204 to indicate that the delete was successful.
    if response.status_code == httplib.NO_CONTENT:
      return True

    if response.status_code == httplib.NOT_FOUND:
      return False

    raise InternalError(response.text)

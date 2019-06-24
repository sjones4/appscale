# Copyright 2019 AppScale Systems, Inc
#
# SPDX-License-Identifier: Apache-2.0

import base64
import logging

from google.appengine.api.blobstore import blobstore_stub
from google.appengine.ext import blobstore

# The MIME type from apps to tell Blobstore to select the mime type.
_AUTO_MIME_TYPE = 'application/vnd.google.appengine.auto'

_GS_BLOBKEY_PREFIX = blobstore_stub.BlobstoreServiceStub.GS_BLOBKEY_PREFIX

_GS_REDIRECT_PREFIX = '/_ah/gs_s3/'


def cloudstorage_s3_blobstore_download_rewriter(state):
    """Rewrite a response with blobstore download bodies.

    Checks for the X-AppEngine-BlobKey header in the response.  If found, it
    will discard the body of the request and redirect for download from s3.

    If the application itself provides a content-type header, it will override
    the content-type stored in the action blob.

    If blobstore.BLOB_RANGE_HEADER header is provided, blob will be partially
    served.  If Range is present, and not blobstore.BLOB_RANGE_HEADER, will use
    Range instead.

    Args:
      state: A request_rewriter.RewriterState to modify.
    """
    blob_key = state.headers.get(blobstore.BLOB_KEY_HEADER)
    if not blob_key:
        return

    logging.debug('Checking for cloudstorage/s3 download %s', blob_key)

    if not blob_key.startswith(_GS_BLOBKEY_PREFIX):
        return

    # Configure for nginx serve
    state.body = []
    del state.headers[blobstore.BLOB_KEY_HEADER]

    content_type = state.headers.get('Content-Type')
    if not content_type:
        # ensure a type is set so the default type is not added
        state.headers['Content-Type'] = _AUTO_MIME_TYPE

    gs_path = base64.urlsafe_b64decode(blob_key[len(_GS_BLOBKEY_PREFIX):])
    redirect = _GS_REDIRECT_PREFIX + gs_path

    logging.debug('Redirecting for cloudstorage/s3 download %s', redirect)
    state.headers['X-Accel-Redirect'] = redirect

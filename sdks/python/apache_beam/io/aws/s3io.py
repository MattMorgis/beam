#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""AWS S3 client
"""

from __future__ import absolute_import

import errno
import logging
import io
import re
import time
from builtins import object

from apache_beam.io.filesystemio import Downloader
from apache_beam.io.filesystemio import DownloaderStream
from apache_beam.utils import retry

try:
  # pylint: disable=wrong-import-order, wrong-import-position
  # pylint: disable=ungrouped-imports
  from apache_beam.io.aws.clients.s3 import boto3_client
  from apache_beam.io.aws.clients.s3 import messages
except ImportError:
  raise ImportError('Missing `boto3` requirement')

MAX_BATCH_OPERATION_SIZE = 100


def parse_s3_path(s3_path, object_optional=False):
  """Return the bucket and object names of the given s3:// path."""
  match = re.match('^s3://([^/]+)/(.*)$', s3_path)
  if match is None or (match.group(2) == '' and not object_optional):
    raise ValueError('S3 path must be in the form s3://<bucket>/<object>.')
  return match.group(1), match.group(2)


class S3IOError(IOError, retry.PermanentException):
  """S3 IO error that should not be retried."""
  pass


class S3IO(object):
  """S3 I/O client."""

  def __init__(self, client=None):
    if client is not None:
      self.client = client
    else:
      self.client = boto3_client.Client()

  def open(self,
           filename,
           mode='r',
           read_buffer_size=16*1024*1024,
           mime_type='application/octet-stream'):
    """Open an S3 file path for reading or writing.

    Args:
      filename (str): S3 file path in the form ``s3://<bucket>/<object>``.
      mode (str): ``'r'`` for reading or ``'w'`` for writing.
      read_buffer_size (int): Buffer size to use during read operations.
      mime_type (str): Mime type to set for write operations.

    Returns:
      S3 file object.

    Raises:
      ~exceptions.ValueError: Invalid open file mode.
    """
    if mode == 'r' or mode == 'rb':
      downloader = S3Downloader(self.client, filename,
                                buffer_size=read_buffer_size)
      return io.BufferedReader(DownloaderStream(downloader, mode=mode),
                               buffer_size=read_buffer_size)
    elif mode == 'w' or mode == 'wb':
      uploader = GcsUploader(self.client, filename, mime_type)
      return io.BufferedWriter(UploaderStream(uploader, mode=mode),
                               buffer_size=128 * 1024)
    else:
      raise ValueError('Invalid file open mode: %s.' % mode)

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def list_prefix(self, path):
    """Lists files matching the prefix.

    Args:
      path: S3 file path pattern in the form s3://<bucket>/[name].

    Returns:
      Dictionary of file name -> size.
    """
    bucket, prefix = parse_s3_path(path, object_optional=True)
    request = messages.ListRequest(bucket=bucket, prefix=prefix)

    file_sizes = {}
    counter = 0
    start_time = time.time()

    logging.info("Starting the size estimation of the input")

    while True:
      response = self.client.list(request)
      for item in response.items:
        file_name = 's3://%s/%s' % (bucket, item.key)
        file_sizes[file_name] = item.size
        counter += 1
        if counter % 10000 == 0:
          logging.info("Finished computing size of: %s files", len(file_sizes))
      if response.next_token:
        request.continuation_token = response.next_token
      else:
        break

    logging.info("Finished listing %s files in %s seconds.",
                 counter, time.time() - start_time)

    return file_sizes


class S3Downloader(Downloader):
  def __init__(self, client, path, buffer_size):
    self._client = client
    self._path = path
    self._bucket, self._name = parse_s3_path(path)
    self._buffer_size = buffer_size

    # Get object state.
    self._get_request = (messages.GetRequest(
        bucket=self._bucket,
        object=self._name))

    try:
      metadata = self._get_object_metadata(self._get_request)

    except messages.S3ClientError as e:
      if e.code == 404:
        raise IOError(errno.ENOENT, 'Not found: %s' % self._path)
      else:
        logging.error('HTTP error while requesting file %s: %s', self._path,
                      3)
        raise

    self._size = metadata.size

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def _get_object_metadata(self, get_request):
    return self._client.get_object_metadata(get_request)

  @property
  def size(self):
    return self._size

  def get_range(self, start, end):
    return self._client.get_range(self._get_request, start, end)

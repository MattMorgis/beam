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
import traceback
from builtins import object

from apache_beam.io.filesystemio import Downloader
from apache_beam.io.filesystemio import DownloaderStream
from apache_beam.io.filesystemio import Uploader
from apache_beam.io.filesystemio import UploaderStream
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
      uploader = S3Uploader(self.client, filename, None)
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

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def copy(self, src, dest):
    """Copies the given S3 object from src to dest.

    Args:
      src: S3 file path pattern in the form s3://<bucket>/<name>.
      dest: S3 file path pattern in the form s3://<bucket>/<name>.

    Raises:
      TimeoutError on timeout.
    """
    src_bucket, src_key = parse_s3_path(src)
    dest_bucket, dest_key = parse_s3_path(dest)
    request = messages.CopyRequest(src_bucket, src_key, dest_bucket, dest_key)
    self.client.copy(request)

  def copy_batch(self, src_dest_pairs):
    """Copies the given GCS object from src to dest.

    Args:
      src_dest_pairs: list of (src, dest) tuples of s3://<bucket>/<name> files
                      paths to copy from src to dest
    Returns: List of tuples of (src, dest, exception) in the same order as the
            src_dest_pairs argument, where exception is None if the operation
            succeeded or the relevant exception if the operation failed.
    """
    results = []

    for src_path, dest_path in src_dest_pairs:
      src_bucket, src_key = parse_s3_path(src_path)
      dest_bucket, dest_key = parse_s3_path(dest_path)
      request = messages.CopyRequest(src_bucket, src_key, dest_bucket, dest_key)

      try:
        self.client.copy(request)
        results.append((src_path, dest_path, None))
      except messages.S3ClientError as e:
        results.append((src_path, dest_path, e))

    return results

  @retry.with_exponential_backoff(
      retry_filter=retry.retry_on_server_errors_and_timeout_filter)
  def delete(self, path):
    bucket, object_path = parse_s3_path(path)
    request = messages.DeleteRequest(bucket, object_path)

    try:
      self.client.delete(request)
    except messages.S3ClientError as e:
      if e.code == 404:
        return # Same behavior as GCS - don't surface a 404 error
      else:
        logging.error('HTTP error while deleting file %s: %s', path,
                      3)
        raise e

  def delete_batch(self, paths):
    """Deletes the objects at the given GCS paths.

    Args:
      paths: List of GCS file path patterns in the form s3://<bucket>/<name>,
             not to exceed MAX_BATCH_OPERATION_SIZE in length.

    Returns: List of tuples of (path, exception) in the same order as the paths
             argument, where exception is None if the operation succeeded or
             the relevant exception if the operation failed.
    """
    # Sort paths into bucket: [keys]
    buckets, keys = zip(*[parse_s3_path(path) for path in paths])
    grouped_keys = {bucket: [] for bucket in buckets}
    for bucket, key in zip(buckets, keys): grouped_keys[bucket].append(key)

    results = {}
    for bucket, keys in grouped_keys.items():
      request = messages.DeleteBatchRequest(bucket, keys)
      try: 
        response = self.client.delete_batch(request)

        for key in response.deleted:
          results[(bucket, key)] = None

        for key, error in zip(response.failed, response.errors):
          results[(bucket, key)] = error

      except messages.S3ClientError as e:
        for key in keys:
          results[(bucket, key)] = e

    # Organize final results
    final_results = [(path, results[parse_s3_path(path)]) for path in paths]

    return final_results

  def exists(self, path):
    bucket, object = parse_s3_path(path)
    request = messages.GetRequest(bucket, object)
    try:
      self.client.get_object_metadata(request)
      return True
    except messages.S3ClientError as e:
      if e.code == 404:
        # HTTP 404 indicates that the file did not exist
        return False
      else:
        # We re-raise all other exceptions
        raise


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


class S3Uploader(Uploader):
  def __init__(self, client, path, mime_type):
    self._client = client
    self._path = path
    self._bucket, self._name = parse_s3_path(path)
    #self._mime_type = mime_type

    self.part_number = 1
    self.buffer = b''

    self.last_error = None

    self.upload_id = None

    self.parts = []

    self._start_upload()

  # There is retry logic in the underlying transfer library but we should make
  # it more explicit so we can control the retry parameters.
  @retry.no_retries  # Using no_retries marks this as an integration point.
  def _start_upload(self):
    # The uploader by default transfers data in chunks of 1024 * 1024 bytes at
    # a time, buffering writes until that size is reached.
    try:
      request = messages.UploadRequest(self._bucket, self._name, None)
      response = self._client.create_multipart_upload(request)
      self.upload_id = response.upload_id
    except Exception as e:  # pylint: disable=broad-except
      logging.error('Error in _start_upload while inserting file %s: %s',
                    self._path, traceback.format_exc())
      self.last_error = e
      raise e

  def put(self, data):

    MIN_WRITE_SIZE = 5 * 1024 * 1024
    MAX_WRITE_SIZE = 5 * 1024 * 1024 * 1024

    # TODO: Byte strings might not be the most performant way to handle this
    self.buffer += data

    while len(self.buffer) >= MIN_WRITE_SIZE:
      # Take the first chunk off the buffer and write it to S3
      chunk = self.buffer[:MAX_WRITE_SIZE]
      self._write_to_s3(chunk)
      # Remove the written chunk from the buffer
      self.buffer = self.buffer[MAX_WRITE_SIZE:]

  def _write_to_s3(self, data):

    try:
      request = messages.UploadPartRequest(self._bucket,
                                           self._name,
                                           self.upload_id,
                                           self.part_number,
                                           data)
      response = self._client.upload_part(request)
      self.parts.append({'ETag': response.etag,
                         'PartNumber': response.part_number})
      self.part_number = self.part_number + 1
    except messages.S3ClientError as e:
      self.last_error = e
      if e.code == 404:
        raise IOError(errno.ENOENT, 'Not found: %s' % self._path)
      else:
        logging.error('HTTP error while requesting file %s: %s', self._path,
                      3)
        raise

  def finish(self):

    self._write_to_s3(self.buffer)

    if self.last_error is not None:
      raise self.last_error  # pylint: disable=raising-bad-type

    request = messages.CompleteMultipartUploadRequest(self._bucket,
                                                      self._name,
                                                      self.upload_id,
                                                      self.parts)
    self._client.complete_multipart_upload(request)

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

import logging
import re
import threading
import time
from builtins import object

from apache_beam.utils import retry

try:
  # pylint: disable=wrong-import-order, wrong-import-position
  # pylint: disable=ungrouped-imports
  from apache_beam.io.aws.clients.s3 import boto3_client
  from apache_beam.io.aws.clients.s3 import requests
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
    request = requests.ListRequest(bucket=bucket, prefix=prefix)

    file_sizes = {}
    counter = 0
    start_time = time.time()

    logging.info("Starting the size estimation of the input")

    while True:
      response = self.client.list(request)
      for item in response['Contents']:
        file_name = 's3://%s/%s' % (bucket, item['Key'])
        file_sizes[file_name] = item['Size']
        counter += 1
        if counter % 10000 == 0:
          logging.info("Finished computing size of: %s files", len(file_sizes))
      try:
        # TODO(morgis): Handle pagination
        # kwargs['ContinuationToken'] = response['NextContinuationToken']
        break
      except KeyError:
        break

    logging.info("Finished listing %s files in %s seconds.",
                 counter, time.time() - start_time)

    return file_sizes

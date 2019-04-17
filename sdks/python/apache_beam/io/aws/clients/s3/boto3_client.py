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

from __future__ import absolute_import

try:
  # pylint: disable=wrong-import-order, wrong-import-position
  # pylint: disable=ungrouped-imports
  import boto3
except ImportError:
  raise ImportError('Missing `boto3` requirement')


class Client(object):

  def __init__(self):
    self.client = boto3.client('s3')

  def list(self, request):
    r"""Retrieves a list of objects matching the criteria.

    Args:
      request: (ListRequest) input message
    Returns:
      (Objects) The response message.
    """
    kwargs = {'Bucket': request.bucket,
              'Prefix': request.prefix}

    if request.continuation_token is not None:
      kwargs['ContinuationToken'] = request.continuation_token

    response = self.client.list_objects_v2(**kwargs)
    return response

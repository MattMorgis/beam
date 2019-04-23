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

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.aws.clients.s3 import messages

try:
  # pylint: disable=wrong-import-order, wrong-import-position
  # pylint: disable=ungrouped-imports
  import boto3
  from botocore.exceptions import ClientError, NoCredentialsError

except ImportError:
  raise ImportError('Missing `boto3` requirement')


class Client(object):
  """
  Wrapper for boto3 library
  """

  def __init__(self):
    self.client = boto3.client('s3')

  def get_object_metadata(self, request):
    r"""Retrieves an object's metadata.

    Args:
      request: (GetRequest) input message

    Returns:
      (Object) The response message.
    """
    kwargs = {'Bucket': request.bucket, 'Key': request.object}

    try:
      boto_response = self.client.head_object(**kwargs)
    except ClientError as e:
      s3error = messages.S3ClientError(e.message)
      s3error.code = int(e.response['Error']['Code'])
      s3error.message = e.response['Error']['Message']
      raise s3error
    except NoCredentialsError as e:
      s3error = messages.S3ClientError(e.message)
      s3error.code = 400
      s3error.message = e.message
      raise s3error

    item = messages.Item(boto_response['ETag'],
                         request.object,
                         boto_response['LastModified'],
                         boto_response['ContentLength'])

    return item

  def get_range(self, request, start, end):
    r"""Retrieves an object.

      Args:
        request: (GetRequest) request
        downloader: (Downloader) Download
            data from the request via this stream.
      Returns:
        (Object) The response message.
      """
    try:
      boto_response = self.client.get_object(Bucket=request.bucket,
                                             Key=request.object,
                                             Range='bytes={}-{}'.format(
                                                 start,
                                                 end))
    except ClientError as e:
      s3error = messages.S3ClientError(e.message)
      s3error.code = int(e.response['Error']['Code'])
      s3error.message = e.response['Error']['Message']
      raise s3error
    except NoCredentialsError as e:
      s3error = messages.S3ClientError(e.message)
      s3error.code = 400
      s3error.message = e.message
      raise s3error

    return boto_response['Body'].read()

  def list(self, request):
    r"""Retrieves a list of objects matching the criteria.

    Args:
      request: (ListRequest) input message
    Returns:
      (ListResponse) The response message.
    """
    kwargs = {'Bucket': request.bucket,
              'Prefix': request.prefix}

    if request.continuation_token is not None:
      kwargs['ContinuationToken'] = request.continuation_token

    boto_response = self.client.list_objects_v2(**kwargs)

    items = [messages.Item(etag=content['ETag'],
                           key=content['Key'],
                           last_modified=content['LastModified'],
                           size=content['Size'])
             for content in boto_response['Contents']]

    try:
      next_token = boto_response['NextContinuationToken']
    except KeyError:
      next_token = None

    response = messages.ListResponse(items, next_token)
    return response

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
  from botocore.exceptions import ClientError, NoCredentialsError, ParamValidationError

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
      s3error = messages.S3ClientError(e.response['Error']['Message'])
      s3error.code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
      s3error.message = e.response['Error']['Message']
      raise s3error
    except NoCredentialsError as e:
      s3error = messages.S3ClientError(e.response['Error']['Message'])
      s3error.code = 400
      s3error.message = e.response['Error']['Message']
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
      Returns:
        (bytes) The response message.
      """
    try:
      boto_response = self.client.get_object(Bucket=request.bucket,
                                             Key=request.object,
                                             Range='bytes={}-{}'.format(
                                                 start,
                                                 end - 1))
    except ClientError as e:
      s3error = messages.S3ClientError(e.response['Error']['Message'])
      s3error.code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
      s3error.message = e.response['Error']['Message']
      raise s3error
    except NoCredentialsError as e:
      s3error = messages.S3ClientError(e.response['Error']['Message'])
      s3error.code = 400
      s3error.message = e.response['Error']['Message']
      raise s3error

    return boto_response['Body'].read() # A bytes object

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

  def create_multipart_upload(self, request):
    r"""Initates a multipart upload to S3 for a given object

    Args:
      request: (UploadRequest) input message
    Returns:
      (UploadResponse) The response message.
    """
    try:
      boto_response = self.client.create_multipart_upload(Bucket=request.bucket,
                                                          Key=request.object)
      response = messages.UploadResponse(boto_response['UploadId'])
    except ClientError as e:
      s3error = messages.S3ClientError(e.response['Error']['Message'])
      s3error.code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
      s3error.message = e.response['Error']['Message']
      raise s3error
    except NoCredentialsError as e:
      s3error = messages.S3ClientError(e.response['Error']['Message'])
      s3error.code = 400
      s3error.message = e.response['Error']['Message']
      raise s3error
    return response

  def upload_part(self, request):
    r"""Uploads part of a file to S3 during a multipart upload

    Args:
      request: (UploadPartRequest) input message
    Returns:
      (UploadPartResponse) The response message.
    """
    try:
      boto_response = self.client.upload_part(Body=request.bytes,
                                              Bucket=request.bucket,
                                              Key=request.object,
                                              PartNumber=request.part_number,
                                              UploadId=request.upload_id)
      response = messages.UploadPartResponse(boto_response['ETag'],
                                             request.part_number)
      return response
    except ClientError as e:
      s3error = messages.S3ClientError(e.response['Error']['Message'])
      s3error.code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
      s3error.message = e.response['Error']['Message']
      raise s3error
    except ParamValidationError as e:
      raise messages.S3ClientError(e.kwargs['report'], 400)
    except NoCredentialsError as e:
      s3error = messages.S3ClientError(e.response['Error']['Message'])
      s3error.code = 400
      s3error.message = e.response['Error']['Message']
      raise s3error

  def complete_multipart_upload(self, request):
    r"""Completes a multipart upload to S3

    Args:
      request: (UploadPartRequest) input message
    Returns:
      (Void) The response message.
    """
    parts = {'Parts': request.parts}
    try:
      self.client.complete_multipart_upload(Bucket=request.bucket,
                                            Key=request.object,
                                            UploadId=request.upload_id,
                                            MultipartUpload=parts)
    except ClientError as e:
      s3error = messages.S3ClientError(e.response['Error']['Message'])
      s3error.code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
      s3error.message = e.response['Error']['Message']
      raise s3error

  def delete(self, request):
    r"""Deletes given object from bucket
    Args:
        request: (DeleteRequest) input message
      Returns:
        (void) Void, otherwise will raise if an error occurs
    """
    try:
      self.client.delete_object(Bucket=request.bucket,
                                Key=request.object)

    except ClientError as e:
      s3error = messages.S3ClientError(e.response['Error']['Message'])
      s3error.code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
      s3error.message = e.response['Error']['Message']
      raise s3error

  def delete_batch(self, request):

    aws_request = {
        'Bucket': request.bucket,
        'Delete': {
            'Objects': [{'Key': object} for object in request.objects]
        }
    }

    try:
      aws_response = self.client.delete_objects(**aws_request)
    except Exception as e:
      message = e.response['Error']['Message']
      code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
      raise messages.S3ClientError(message, code)

    deleted = [obj['Key'] for obj in aws_response.get('Deleted', [])]

    failed = [obj['Key'] for obj in aws_response.get('Errors', [])]

    errors = [messages.S3ClientError(obj['Message'], obj['Code']) 
              for obj in aws_response.get('Errors', [])]

    return messages.DeleteBatchResponse(deleted, failed, errors)

  def copy(self, request):
    try:
      copy_src = {
          'Bucket': request.src_bucket,
          'Key': request.src_key
      }
      self.client.copy(copy_src, request.dest_bucket, request.dest_key)
    except Exception as e:
      s3error = messages.S3ClientError(e.response['Error']['Message'])
      s3error.code = int(e.response['ResponseMetadata']['HTTPStatusCode'])
      s3error.message = e.response['Error']['Message']
      raise s3error

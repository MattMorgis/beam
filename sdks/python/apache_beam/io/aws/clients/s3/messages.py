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


class GetRequest(object):
  """
  S3 request object for `Get` command
  """

  def __init__(self, bucket, object):
    self.bucket = bucket
    self.object = object

class UploadResponse(object):
  """
  S3 response object for `StartUpload` command
  """

  def __init__(self, upload_id):
    self.upload_id = upload_id

class UploadRequest(object):
  """
  S3 request object for `StartUpload` command
  """

  def __init__(self, bucket, object, mime_type):
    self.bucket = bucket
    self.object = object
    self.mime_type = mime_type

class UploadPartRequest(object):
  """
  S3 request object for `UploadPart` command
  """

  def __init__(self, bucket, object, upload_id, part_number, bytes):
    self.bucket = bucket
    self.object = object
    self.upload_id = upload_id
    self.part_number = part_number
    self.bytes = bytes
    # self.mime_type = mime_type

class UploadPartResponse(object):
  """
  S3 response object for `UploadPart` command
  """

  def __init__(self, etag, part_number):
    self.etag = etag
    self.part_number = part_number

class CompleteMultipartUploadRequest(object):
  """
  S3 request object for `UploadPart` command
  """

  def __init__(self, bucket, object, upload_id, parts):
    # parts is a list of objects of the form {'ETag': response.etag, 'PartNumber': response.part_number}
    self.bucket = bucket
    self.object = object
    self.upload_id = upload_id
    self.parts = parts
    # self.mime_type = mime_type

class ListRequest(object):
  """
  S3 request object for `List` command
  """

  def __init__(self, bucket, prefix, continuation_token=None):
    self.bucket = bucket
    self.prefix = prefix
    self.continuation_token = continuation_token

class ListResponse(object):
  """
  S3 response object for `List` command
  """

  def __init__(self, items, next_token=None):
    self.items = items
    self.next_token = next_token

class Item(object):
  """
  An item in S3
  """

  def __init__(self, etag, key, last_modified, size):
    self.etag = etag
    self.key = key
    self.last_modified = last_modified
    self.size = size

class DeleteRequest(object):
  """
  S3 request object for `Delete` command
  """

  def __init__(self, bucket, object):
    self.bucket = bucket
    self.object = object

class S3ClientError(Exception):

  def __init__(self, message = None, code = None):
    self.message = message
    self.code = code

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

import os

from apache_beam.io.aws.clients.s3 import messages


class FakeFile(object):

  def __init__(self, bucket, key, contents, last_modified=None):
    self.bucket = bucket
    self.key = key
    self.contents = contents
    self.last_modified = last_modified
    self.etag = 'xxxxxxxx'

  def get_metadata(self):
    last_modified_datetime = None
    if self.last_modified:
      last_modified_datetime = datetime.datetime.utcfromtimestamp(
          self.last_modified)

    return messages.Item(self.etag,
                         self.key,
                         last_modified_datetime,
                         len(self.contents))


class FakeS3Client(object):
  def __init__(self):
    self.files = {}
    self.list_continuation_tokens = {}
    self.multipart_uploads = {}

  def add_file(self, f):
    self.files[(f.bucket, f.key)] = f

  def get_file(self, bucket, obj):
    return self.files.get((bucket, obj), None)

  def delete_file(self, bucket, obj):
    del self.files[(bucket, obj)]

  def get_object_metadata(self, request):
    r"""Retrieves an object's metadata.

    Args:
      request: (GetRequest) input message

    Returns:
      (Item) The response message.
    """
    # TODO: What if there isn't a file for the bucket / key pair?
    # TODO: Do we want to mock out a lack of credentials?
    file_ = self.get_file(request.bucket, request.object)
    return file_.get_metadata()

  def list(self, request):
    bucket = request.bucket
    prefix = request.prefix or ''
    matching_files = []

    for file_bucket, file_name in sorted(iter(self.files)):
      if bucket == file_bucket and file_name.startswith(prefix):
        file_object = self.files[(file_bucket, file_name)].get_metadata()
        matching_files.append(file_object)

    # Handle pagination.
    items_per_page = 5
    if not request.continuation_token:
      range_start = 0
    else:
      if request.continuation_token not in self.list_continuation_tokens:
        raise ValueError('Invalid page token.')
      range_start = self.list_continuation_tokens[request.continuation_token]
      del self.list_continuation_tokens[request.continuation_token]

    result = messages.ListResponse(
        items=matching_files[range_start:range_start + items_per_page])

    if range_start + items_per_page < len(matching_files):
      next_range_start = range_start + items_per_page
      next_continuation_token = '_page_token_%s_%s_%d' % (bucket, prefix,
                                                          next_range_start)
      self.list_continuation_tokens[next_continuation_token] = next_range_start
      result.next_token = next_continuation_token

    return result

  def get_range(self, request, start, end):
    r"""Retrieves an object.

      Args:
        request: (GetRequest) request
      Returns:
        (bytes) The response message.
      """
    # TODO: What if the bucket / key pair doesn't exist?
    # TODO: What if the range is bad? (Start/end too big/small, etc.)

    file_ = self.get_file(request.bucket, request.object)
    return file_.contents[start:end]

  def delete(self, delete_request):
    if self.get_file(delete_request.bucket, delete_request.object):
      self.delete_file(delete_request.bucket, delete_request.object)
    else:
      s3error = messages.S3ClientError()
      s3error.code, s3error.message = 404, 'The specified bucket does not exist'
      raise s3error

  def create_multipart_upload(self, request):
    # Create hash of bucket and key
    # Store upload_id internally
    upload_id = request.bucket + request.object
    self.multipart_uploads[upload_id] = {}
    return messages.UploadResponse(upload_id)

  def upload_part(self, request):
    # Save off bytes passed to internal data store
    # TODO: What if the request sends an ID that we don't have?
    # TODO: Make sure that the part_number is a positive int
    # TODO: Make sure that the size of the part falls within the bounds
    upload_id, part_number = request.upload_id, request.part_number
    self.multipart_uploads[upload_id][part_number] = request.bytes

    etag = 'xxxxx-fake-etag'
    return messages.UploadPartResponse(etag, part_number)

  def complete_multipart_upload(self, request):

    parts_received = self.multipart_uploads[request.upload_id]

    # Check that we got all the parts that they intended to send
    part_numbers_to_confirm = set(part['PartNumber'] for part in request.parts)

    # TODO: What if they didn't send us enough parts / sent us too many parts?
    if part_numbers_to_confirm != set(parts_received.keys()):
      raise # Fill it in with a real message later

    # Sort by part number
    sorted_parts = sorted(parts_received.items(), key=lambda pair: pair[0])
    sorted_bytes = [bytes_ for (part_number, bytes_) in sorted_parts]

    # String together all bytes for the given upload
    final_contents = b''.join(sorted_bytes)

    # Create FakeFile object
    file_ = FakeFile(request.bucket, request.object, final_contents)

    # Store FakeFile in self.files
    self.files[(request.bucket, request.object)] = file_

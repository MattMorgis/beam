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

from apache_beam.io.aws.clients.s3 import messages


class FakeFile(object):
  def __init__(self, bucket, key, size, last_modified=None):
    self.bucket = bucket
    self.key = key
    self.size = size
    self.last_modified = last_modified
    self.etag = 1

  def get_metadata(self):
    last_modified_datetime = None
    if self.last_modified:
      last_modified_datetime = datetime.datetime.utcfromtimestamp(
          self.last_modified)

    return messages.Item(self.etag,
                         self.key,
                         last_modified_datetime,
                         len(self.size))


class FakeS3Client(object):
  def __init__(self):
    self.files = {}
    self.last_generation = {}
    self.list_continuation_tokens = {}

  def add_file(self, f):
    self.files[(f.bucket, f.key)] = f
    self.last_generation[(f.bucket, f.key)] = f.etag

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

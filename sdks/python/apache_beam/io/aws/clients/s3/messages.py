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

  @property
  def bucket(self):
    return self._bucket

  @bucket.setter
  def bucket(self, bucket):
    self._bucket = bucket

  @property
  def object(self):
    return self._object

  @object.setter
  def object(self, object):
    self._object = object


class ListRequest(object):
  """
  S3 request object for `List` command
  """

  def __init__(self, bucket, prefix, continuation_token=None):
    self.bucket = bucket
    self.prefix = prefix
    self.continuation_token = continuation_token

  @property
  def bucket(self):
    return self._bucket

  @bucket.setter
  def bucket(self, bucket):
    self._bucket = bucket

  @property
  def prefix(self):
    return self._prefix

  @prefix.setter
  def prefix(self, prefix):
    self._prefix = prefix

  @property
  def continuation_token(self):
    return self._continuation_token

  @continuation_token.setter
  def continuation_token(self, continuation_token):
    self._continuation_token = continuation_token


class ListResponse(object):
  """
  S3 response object for `List` command
  """

  def __init__(self, items, next_token=None):
    self.items = items
    self.next_token = next_token

  @property
  def items(self):
    return self._items

  @items.setter
  def items(self, items):
    self._items = items

  @property
  def next_token(self):
    return self._next_token

  @next_token.setter
  def next_token(self, next_token):
    self._next_token = next_token


class Item(object):
  """
  An item in S3
  """

  def __init__(self, etag, key, last_modified, size):
    self.etag = etag
    self.key = key
    self.last_modified = last_modified
    self.size = size

  @property
  def etag(self):
    return self._etag

  @etag.setter
  def etag(self, etag):
    self._etag = etag

  @property
  def key(self):
    return self._key

  @key.setter
  def key(self, key):
    self._key = key

  @property
  def last_modified(self):
    return self._last_modified

  @last_modified.setter
  def last_modified(self, last_modified):
    self._last_modified = last_modified

  @property
  def size(self):
    return self._size

  @size.setter
  def size(self, size):
    self._size = size


class S3ClientError(Exception):
  message = None
  code = None

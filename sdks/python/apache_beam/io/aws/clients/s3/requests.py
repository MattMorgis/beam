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


class ListRequest(object):

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

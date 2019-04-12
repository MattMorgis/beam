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
"""Tests for S3 client."""
from __future__ import absolute_import

import logging
import unittest

# Protect against environments where boto3 library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.aws import s3io
except ImportError:
  pass
# pylint: enable=wrong-import-order, wrong-import-position


class TestS3IO(unittest.TestCase):

  def _insert_random_file(self, client, path, size):
    bucket, name = s3io.parse_s3_path(path)
    f = FakeFile(bucket, name, os.urandom(size))
    client.objects.add_file(f)
    return f

  def setUp(self):
    # self.client = FakeBotoClient()
    self.aws = s3io.S3IO()

  def test_list_prefix(self):
    bucket_name = 'random-data-sets'

    # TODO: Setup fake Boto client to return fake files
    # objects = [
    #     ('jerry/pigpen/phil', 5),
    #     ('jerry/pigpen/bobby', 3),
    #     ('jerry/billy/bobby', 4),
    # ]

    # for (object_name, size) in objects:
    #   file_name = 's3://%s/%s' % (bucket_name, object_name)
      # self._insert_random_file(self.client, file_name, size)

    test_cases = [
        ('s3://random-data-sets/j', [
            ('jerry/pigpen/phil', 5),
            ('jerry/pigpen/bobby', 6),
            ('jerry/billy/bobby', 6),
        ]),
        ('s3://random-data-sets/jerry/', [
            ('jerry/pigpen/phil', 5),
            ('jerry/pigpen/bobby', 6),
            ('jerry/billy/bobby', 6),
        ]),
        ('s3://random-data-sets/jerry/pigpen/phil', [
            ('jerry/pigpen/phil', 5),
        ]),
    ]

    for file_pattern, expected_object_names in test_cases:
      expected_file_names = [('s3://%s/%s' % (bucket_name, object_name), size)
                             for (object_name, size) in expected_object_names]
      self.assertEqual(
          set(self.aws.list_prefix(file_pattern).items()),
          set(expected_file_names))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()

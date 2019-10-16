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

import os
import logging
import random
import unittest

from apache_beam.io.aws.clients.s3 import fake_client

# Protect against environments where boto3 library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.aws import s3io
except ImportError:
  pass
# pylint: enable=wrong-import-order, wrong-import-position


class TestS3PathParser(unittest.TestCase):

  BAD_S3_PATHS = [
      's3://',
      's3://bucket',
      's3:///name',
      's3:///',
      's3:/blah/bucket/name',
  ]

  def test_s3_path(self):
    self.assertEqual(
        s3io.parse_s3_path('s3://bucket/name'), ('bucket', 'name'))
    self.assertEqual(
        s3io.parse_s3_path('s3://bucket/name/sub'), ('bucket', 'name/sub'))

  def test_bad_s3_path(self):
    for path in self.BAD_S3_PATHS:
      self.assertRaises(ValueError, s3io.parse_s3_path, path)
    self.assertRaises(ValueError, s3io.parse_s3_path, 's3://bucket/')

  def test_s3_path_object_optional(self):
    self.assertEqual(
        s3io.parse_s3_path('s3://bucket/name', object_optional=True),
        ('bucket', 'name'))
    self.assertEqual(
        s3io.parse_s3_path('s3://bucket/', object_optional=True),
        ('bucket', ''))

  def test_bad_s3_path_object_optional(self):
    for path in self.BAD_S3_PATHS:
      self.assertRaises(ValueError, s3io.parse_s3_path, path, True)


class TestS3IO(unittest.TestCase):

  def _insert_random_file(self, client, path, size):
    bucket, name = s3io.parse_s3_path(path)
    contents = os.urandom(size)
    fakeFile = fake_client.FakeFile(bucket, name, contents)
    f = self.aws.open(path, 'w')
    f.write(contents)
    f.close()
    return fakeFile

  def setUp(self):
    # For pure unit tests, use this Fake S3 Client
    # It will mock all calls to aws and no authentication is needed
    self.client = fake_client.FakeS3Client()
    self.aws = s3io.S3IO(self.client)
    # For integration tests or to test over to the wire
    # Initalize with no client and it will default to using Boto3
    # Uncomment the following line:
    self.aws = s3io.S3IO()

  def test_copy(self):
    src_file_name = 's3://random-data-sets/source'
    dest_file_name = 's3://random-data-sets/dest'
    file_size = 1024
    self._insert_random_file(self.client, src_file_name, file_size)
    self.assertTrue(
        s3io.parse_s3_path(src_file_name) in self.client.objects.files)
    self.assertFalse(
        s3io.parse_s3_path(dest_file_name) in self.client.objects.files)

    self.aws.copy(src_file_name, dest_file_name)

    self.assertTrue(
        s3io.parse_s3_path(src_file_name) in self.client.objects.files)
    self.assertTrue(
        s3io.parse_s3_path(dest_file_name) in self.client.objects.files)

    # Test copy of non-existent files.
    with self.assertRaisesRegex(S3ClientError, r'Not Found'):
      self.aws.copy('s3://random-data-sets/non-existent',
                    's3://random-data-sets/non-existent-destination')

  def test_delete(self):
    file_name = 's3://random-data-sets/_delete_file'
    file_size = 1024

    # Test deletion of non-existent file (shouldn't raise any error)
    self.aws.delete(file_name)

    # Create the file and check that it was created
    self._insert_random_file(self.aws.client, file_name, file_size)
    files = self.aws.list_prefix('s3://random-data-sets/')
    self.assertTrue(file_name in files)

    # Delete the file and check that it was deleted
    self.aws.delete(file_name)
    files = self.aws.list_prefix('s3://random-data-sets/')
    self.assertTrue(file_name not in files)

  def test_delete_batch(self, *unused_args):
    file_name_pattern = 's3://random-data-sets/_delete_batch/%d'
    file_size = 1024
    num_files = 5

    # Test deletion of non-existent files.
    result = self.aws.delete_batch(
        [file_name_pattern % i for i in range(num_files)])
    self.assertTrue(result)
    for i, (file_name, exception) in enumerate(result):
      self.assertEqual(file_name, file_name_pattern % i)
      self.assertEqual(exception, None)
      self.assertFalse(self.aws.exists(file_name_pattern % i))

    # Insert some files.
    for i in range(num_files):
      self._insert_random_file(self.client, file_name_pattern % i, file_size)

    # Check files inserted properly.
    for i in range(num_files):
      self.assertTrue(self.aws.exists(file_name_pattern % i))

    # Execute batch delete.
    self.aws.delete_batch([file_name_pattern % i for i in range(num_files)])

    # Check files deleted properly.
    for i in range(num_files):
      self.assertFalse(self.aws.exists(file_name_pattern % i))

  def test_exists(self):
    file_name = 's3://random-data-sets/_exists'
    file_size = 1024

    self.assertFalse(self.aws.exists(file_name))

    self._insert_random_file(self.aws.client, file_name, file_size)

    self.assertTrue(self.aws.exists(file_name))

    # Clean up
    self.aws.delete(file_name)

    self.assertFalse(self.aws.exists(file_name))

  def test_file_mode(self):
    file_name = 's3://random-data-sets/jerry/pigpen/bobby'
    with self.aws.open(file_name, 'w') as f:
      assert f.mode == 'w'
    with self.aws.open(file_name, 'r') as f:
      assert f.mode == 'r'

  def test_full_file_read(self):
    file_name = 's3://random-data-sets/jerry/pigpen/phil'
    file_size = 1024

    f = self._insert_random_file(self.aws.client, file_name, file_size)
    contents = f.contents

    f = self.aws.open(file_name)
    self.assertEqual(f.mode, 'r')
    f.seek(0, os.SEEK_END)
    self.assertEqual(f.tell(), file_size)
    self.assertEqual(f.read(), b'')
    f.seek(0)
    self.assertEqual(f.read(), contents)

  def test_file_write(self):
    file_name = 's3://random-data-sets/_write_file'
    file_size = 8 * 1024 * 1024 + 2000
    contents = os.urandom(file_size)
    f = self.aws.open(file_name, 'w')
    self.assertEqual(f.mode, 'w')
    f.write(contents[0:1000])
    f.write(contents[1000:1024 * 1024])
    f.write(contents[1024 * 1024:])
    f.close()
    new_f = self.aws.open(file_name, 'r')
    new_f_contents = new_f.read()
    self.assertEqual(
        new_f_contents, contents)

  # This takes a long time to run
  # but helpful if needed to debug the write buffer
  # By default, parts of a multipart upload can't be bigger than 5GB
  #def test_big_file_write(self):
  #  KiB, MiB, GiB = 1024, 1024 * 1024, 1024 * 1024 * 1024
  #  file_name = 's3://random-data-sets/_write_file'
  #  file_size = 7 * GiB
  #  contents = os.urandom(file_size)
  #  f = self.aws.open(file_name, 'w')
  #  self.assertEqual(f.mode, 'w')
  #  f.write(contents[:1 * MiB])
  #  f.write(contents[1 * MiB : 6 * GiB])
  #  f.write(contents[6 * GiB : ])
  #  f.close()
  #  bucket, name = s3io.parse_s3_path(file_name)
  #  new_f = self.aws.open(file_name, 'r')
  #  new_f_contents = new_f.read()
  #  self.assertEqual(
  #      new_f_contents, contents)

  def test_file_random_seek(self):
    file_name = 's3://random-data-sets/_write_seek_file'
    file_size = 5 * 1024 * 1024 - 100
    contents = os.urandom(file_size)
    with self.aws.open(file_name, 'w') as wf:
      wf.write(contents)

    f = self.aws.open(file_name)
    random.seed(0)

    for _ in range(0, 10):
      a = random.randint(0, file_size - 1)
      b = random.randint(0, file_size - 1)
      start, end = min(a, b), max(a, b)
      f.seek(start)

      self.assertEqual(f.tell(), start)

      self.assertEqual(
          f.read(end - start + 1), contents[start:end + 1]
      )
      self.assertEqual(f.tell(), end + 1)

  def test_file_flush(self):
    file_name = 's3://random-data-sets/_flush_file'
    file_size = 5 * 1024 * 1024 + 2000
    contents = os.urandom(file_size)
    f = self.aws.open(file_name, 'w')
    self.assertEqual(f.mode, 'w')
    f.write(contents[0:1000])
    f.flush()
    f.write(contents[1000:1024 * 1024])
    f.flush()
    f.flush()  # Should be a NOOP.
    f.write(contents[1024 * 1024:])
    f.close() # This should al`read`y call the equivalent of flush() in its body
    new_f = self.aws.open(file_name, 'r')
    new_f_contents = new_f.read()
    self.assertEqual(
        new_f_contents, contents)

  def test_file_iterator(self):
    file_name = 's3://random-data-sets/_iterate_file'
    lines = []
    line_count = 10
    for _ in range(line_count):
      line_length = random.randint(100, 500)
      line = os.urandom(line_length).replace(b'\n', b' ') + b'\n'
      lines.append(line)

    contents = b''.join(lines)

    with self.aws.open(file_name, 'w') as wf:
      wf.write(contents)

    f = self.aws.open(file_name)

    read_lines = 0
    for line in f:
      read_lines += 1

    self.assertEqual(read_lines, line_count)

  def test_file_read_line(self):
    file_name = 's3://random-data-sets/_read_line_file'
    lines = []

    # Set a small buffer size to exercise refilling the buffer.
    # First line is carefully crafted so the newline falls as the last character
    # of the buffer to exercise this code path.
    read_buffer_size = 1099
    lines.append(b'x' * 1023 + b'\n')

    for _ in range(1, 1000):
      line_length = random.randint(100, 500)
      line = os.urandom(line_length).replace(b'\n', b' ') + b'\n'
      lines.append(line)
    contents = b''.join(lines)

    file_size = len(contents)

    with self.aws.open(file_name, 'wb') as wf:
      wf.write(contents)

    f = self.aws.open(file_name, 'rb', read_buffer_size=read_buffer_size)

    # Test read of first two lines.
    f.seek(0)
    self.assertEqual(f.readline(), lines[0])
    self.assertEqual(f.tell(), len(lines[0]))
    self.assertEqual(f.readline(), lines[1])

    # Test read at line boundary.
    f.seek(file_size - len(lines[-1]) - 1)
    self.assertEqual(f.readline(), b'\n')

    # Test read at end of file.
    f.seek(file_size)
    self.assertEqual(f.readline(), b'')

    # Test reads at random positions.
    random.seed(0)
    for _ in range(0, 10):
      start = random.randint(0, file_size - 1)
      line_index = 0
      # Find line corresponding to start index.
      chars_left = start
      while True:
        next_line_length = len(lines[line_index])
        if chars_left - next_line_length < 0:
          break
        chars_left -= next_line_length
        line_index += 1
      f.seek(start)
      self.assertEqual(f.readline(), lines[line_index][chars_left:])

  def test_file_close(self):
    file_name = 's3://random-data-sets/_close_file'
    file_size = 5 * 1024 * 1024 + 2000
    contents = os.urandom(file_size)
    f = self.aws.open(file_name, 'w')
    self.assertEqual(f.mode, 'w')
    f.write(contents)
    f.close()
    f.close()  # This should not crash.

    with self.aws.open(file_name, 'r') as f:
      read_contents = f.read()

    self.assertEqual(
        read_contents, contents)

  def test_context_manager(self):
    # Test writing with a context manager.
    file_name = 's3://random-data-sets/_context_manager_file'
    file_size = 1024
    contents = os.urandom(file_size)
    with self.aws.open(file_name, 'w') as f:
      f.write(contents)

    with self.aws.open(file_name, 'r') as f:
      self.assertEqual(f.read(), contents)

  def test_list_prefix(self):
    bucket_name = 'random-data-sets'

    objects = [
        ('jerry/pigpen/phil', 5),
        ('jerry/pigpen/bobby', 3),
        ('jerry/billy/bobby', 4),
    ]

    for (object_name, size) in objects:
      file_name = 's3://%s/%s' % (bucket_name, object_name)
      self._insert_random_file(self.aws.client, file_name, size)

    test_cases = [
        ('s3://random-data-sets/j', [
            ('jerry/pigpen/phil', 5),
            ('jerry/pigpen/bobby', 3),
            ('jerry/billy/bobby', 4),
        ]),
        ('s3://random-data-sets/jerry/', [
            ('jerry/pigpen/phil', 5),
            ('jerry/pigpen/bobby', 3),
            ('jerry/billy/bobby', 4),
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

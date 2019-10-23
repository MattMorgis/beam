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
import time
import unittest

from apache_beam.io.aws.clients.s3 import fake_client
from apache_beam.io.aws.clients.s3 import messages

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

    if self.USE_MOCK:
      self.client.add_file(fakeFile)

    else:
      f = self.aws.open(path, 'w')
      f.write(contents)
      f.close()

    return fakeFile

  def setUp(self):

    # These tests can be run locally against a mock S3 client, or as integration
    # tests against the real S3 client.
    self.USE_MOCK = True

    # If you're running integration tests with S3, set this variable to be an
    # s3 path that you have access to where test data can be written. If you're
    # just running tests against the mock, this can be any s3 path. It should
    # end with a '/'.
    self.TEST_DATA_PATH = 's3://random-data-sets/beam_tests/'

    if self.USE_MOCK:
      self.client = fake_client.FakeS3Client()
      test_data_bucket, _ = s3io.parse_s3_path(self.TEST_DATA_PATH)
      self.client.known_buckets.add(test_data_bucket)
      self.aws = s3io.S3IO(self.client)

    else:
      self.aws = s3io.S3IO()
      self.client = self.aws.client

  def test_size(self):
    file_name = 's3://random-data-sets/dummy_file'
    file_size = 1234

    self._insert_random_file(self.client, file_name, file_size)
    self.assertTrue(self.aws.exists(file_name))
    self.assertEqual(1234, self.aws.size(file_name))

  def test_last_updated(self):
    file_name = 's3://random-data-sets/dummy_file'
    file_size = 1234

    self._insert_random_file(self.client, file_name, file_size)
    self.assertTrue(self.aws.exists(file_name))

    tolerance = 5 * 60 # 5 mins
    low_bound, high_bound = time.time() - tolerance, time.time() + tolerance
    result = self.aws.last_updated(file_name)
    self.assertTrue(low_bound <= result <= high_bound)

  def test_checksum(self):

    file_name = self.TEST_DATA_PATH + 'checksum'
    file_size = 1024
    file_ = self._insert_random_file(self.client, file_name, file_size)

    original_etag = self.aws.checksum(file_name)

    self.aws.delete(file_name)

    with self.aws.open(file_name, 'w') as f:
      f.write(file_.contents)

    rewritten_etag = self.aws.checksum(file_name)

    self.assertEqual(original_etag, rewritten_etag)
    self.assertEqual(len(original_etag), 36)
    self.assertTrue(original_etag.endswith('-1"'))

    # Clean up
    self.aws.delete(file_name)

  def test_copy(self):
    src_file_name = self.TEST_DATA_PATH + 'source'
    dest_file_name = self.TEST_DATA_PATH + 'dest'
    file_size = 1024
    self._insert_random_file(self.client, src_file_name, file_size)

    self.assertTrue(src_file_name in
                    self.aws.list_prefix(self.TEST_DATA_PATH))
    self.assertFalse(dest_file_name in
                     self.aws.list_prefix(self.TEST_DATA_PATH))

    self.aws.copy(src_file_name, dest_file_name)

    self.assertTrue(src_file_name in
                    self.aws.list_prefix(self.TEST_DATA_PATH))
    self.assertTrue(dest_file_name in
                    self.aws.list_prefix(self.TEST_DATA_PATH))

    # Clean up
    self.aws.delete_batch([src_file_name, dest_file_name])

    # Test copy of non-existent files.
    with self.assertRaisesRegex(messages.S3ClientError, r'Not Found'):
      self.aws.copy(self.TEST_DATA_PATH + 'non-existent',
                    self.TEST_DATA_PATH + 'non-existent-destination')

  def test_copy_batch(self):
    from_name_pattern = self.TEST_DATA_PATH + 'copy_me_%d'
    to_name_pattern = self.TEST_DATA_PATH + 'destination_%d'
    file_size = 1024
    num_files = 10

    src_dest_pairs = [(from_name_pattern % i, to_name_pattern % i)
                      for i in range(num_files)]

    result = self.aws.copy_batch(src_dest_pairs)

    self.assertTrue(result)
    for i, (src, dest, exception) in enumerate(result):
      self.assertEqual(src, from_name_pattern % i)
      self.assertEqual(dest, to_name_pattern % i)
      self.assertTrue(isinstance(exception, messages.S3ClientError))
      self.assertEqual(exception.code, 404)
      self.assertFalse(self.aws.exists(from_name_pattern % i))
      self.assertFalse(self.aws.exists(to_name_pattern % i))

    # Insert some files.
    for i in range(num_files):
      self._insert_random_file(self.client, from_name_pattern % i, file_size)

    # Check files inserted properly.
    for i in range(num_files):
      self.assertTrue(self.aws.exists(from_name_pattern % i))

    # Execute batch copy.
    self.aws.copy_batch(src_dest_pairs)

    # Check files copied properly.
    for i in range(num_files):
      self.assertTrue(self.aws.exists(from_name_pattern % i))
      self.assertTrue(self.aws.exists(to_name_pattern % i))

    # Clean up
    all_files = set().union(*[set(pair) for pair in src_dest_pairs])
    self.aws.delete_batch(all_files)

  def test_copytree(self):
    src_dir_name = self.TEST_DATA_PATH + 'source/'
    dest_dir_name = self.TEST_DATA_PATH + 'dest/'
    file_size = 1024
    paths = ['a', 'b/c', 'b/d']
    for path in paths:
      src_file_name = src_dir_name + path
      dest_file_name = dest_dir_name + path
      self._insert_random_file(self.client, src_file_name, file_size)
      self.assertTrue(
          src_file_name in self.aws.list_prefix(self.TEST_DATA_PATH))
      self.assertFalse(
          dest_file_name in self.aws.list_prefix(self.TEST_DATA_PATH))

    self.aws.copytree(src_dir_name, dest_dir_name)

    for path in paths:
      src_file_name = src_dir_name + path
      dest_file_name = dest_dir_name + path
      self.assertTrue(
          src_file_name in self.aws.list_prefix(self.TEST_DATA_PATH))
      self.assertTrue(
          dest_file_name in self.aws.list_prefix(self.TEST_DATA_PATH))

    # Clean up
    for path in paths:
      src_file_name = src_dir_name + path
      dest_file_name = dest_dir_name + path
      self.aws.delete_batch([src_file_name, dest_file_name])

  def test_rename(self):
    src_file_name = self.TEST_DATA_PATH + 'source'
    dest_file_name = self.TEST_DATA_PATH + 'dest'
    file_size = 1024

    self._insert_random_file(self.client, src_file_name, file_size)

    self.assertTrue(
        src_file_name in self.aws.list_prefix(self.TEST_DATA_PATH))
    self.assertFalse(
        dest_file_name in self.aws.list_prefix(self.TEST_DATA_PATH))

    self.aws.rename(src_file_name, dest_file_name)

    self.assertFalse(
        src_file_name in self.aws.list_prefix(self.TEST_DATA_PATH))
    self.assertTrue(
        dest_file_name in self.aws.list_prefix(self.TEST_DATA_PATH))

    # Clean up
    self.aws.delete_batch([src_file_name, dest_file_name])

  def test_rename_batch(self):
    from_name_pattern = self.TEST_DATA_PATH + 'to_rename_%d'
    to_name_pattern = self.TEST_DATA_PATH + 'been_renamed_%d'
    file_size = 1024
    num_files = 10

    src_dest_pairs = [(from_name_pattern % i, to_name_pattern % i)
                      for i in range(num_files)]

    result = self.aws.rename_batch(src_dest_pairs)

    self.assertTrue(result)
    for i, (src, dest, exception) in enumerate(result):
      self.assertEqual(src, from_name_pattern % i)
      self.assertEqual(dest, to_name_pattern % i)
      self.assertTrue(isinstance(exception, messages.S3ClientError))
      self.assertEqual(exception.code, 404)
      self.assertFalse(self.aws.exists(from_name_pattern % i))
      self.assertFalse(self.aws.exists(to_name_pattern % i))

    # Insert some files.
    for i in range(num_files):
      self._insert_random_file(self.client, from_name_pattern % i, file_size)

    # Check files inserted properly.
    for i in range(num_files):
      self.assertTrue(self.aws.exists(from_name_pattern % i))
      self.assertFalse(self.aws.exists(to_name_pattern % i))

    # Execute batch rename.
    self.aws.rename_batch(src_dest_pairs)

    # Check files were renamed properly.
    for i in range(num_files):
      self.assertFalse(self.aws.exists(from_name_pattern % i))
      self.assertTrue(self.aws.exists(to_name_pattern % i))

    # Clean up
    all_files = set().union(*[set(pair) for pair in src_dest_pairs])
    self.aws.delete_batch(all_files)

  def test_rename_batch_with_errors(self):
    real_prefix = self.TEST_DATA_PATH + 'rename_batch_%s'
    fake_prefix = 's3://fake-bucket-68ae4b0ef7b9/rename_batch_%s'
    src_dest_pairs = [(prefix % 'src', prefix % 'dest')
                      for prefix in (real_prefix, fake_prefix)]

    # Create the file in the real bucket
    self._insert_random_file(self.client, real_prefix % 'src', 1024)

    # Execute batch rename
    result = self.aws.rename_batch(src_dest_pairs)

    # First is the file in the real bucket, which shouldn't throw an error
    self.assertEqual(result[0][0], src_dest_pairs[0][0])
    self.assertEqual(result[0][1], src_dest_pairs[0][1])
    self.assertIsNone(result[0][2])

    # Second is the file in the fake bucket, which should throw a 404
    self.assertEqual(result[1][0], src_dest_pairs[1][0])
    self.assertEqual(result[1][1], src_dest_pairs[1][1])
    self.assertEqual(result[1][2].code, 404)

    # Clean up
    self.aws.delete(real_prefix % 'dest')

  def test_delete(self):
    file_name = self.TEST_DATA_PATH + 'delete_file'
    file_size = 1024

    # Test deletion of non-existent file (shouldn't raise any error)
    self.aws.delete(file_name)

    # Create the file and check that it was created
    self._insert_random_file(self.aws.client, file_name, file_size)
    files = self.aws.list_prefix(self.TEST_DATA_PATH)
    self.assertTrue(file_name in files)

    # Delete the file and check that it was deleted
    self.aws.delete(file_name)
    files = self.aws.list_prefix(self.TEST_DATA_PATH)
    self.assertTrue(file_name not in files)

  def test_delete_batch(self, *unused_args):
    file_name_pattern = self.TEST_DATA_PATH + 'delete_batch/%d'
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

  def test_delete_batch_with_errors(self, *unused_args):
    real_file = self.TEST_DATA_PATH + 'delete_batch/file'
    fake_file = 's3://fake-bucket-68ae4b0ef7b9/delete_batch/file'
    filenames = [real_file, fake_file]

    result = self.aws.delete_batch(filenames)

    # First is the file in the real bucket, which shouldn't throw an error
    self.assertEqual(result[0][0], filenames[0])
    self.assertIsNone(result[0][1])

    # Second is the file in the fake bucket, which should throw a 404
    self.assertEqual(result[1][0], filenames[1])
    self.assertEqual(result[1][1].code, 404)

  def test_delete_tree(self):

    root_path = self.TEST_DATA_PATH + 'delete_tree/'
    leaf_paths = ['a', 'b/c', 'b/d', 'b/d/e']
    paths = [root_path + leaf for leaf in leaf_paths]

    # Create file tree
    file_size = 1024
    for path in paths:
      self._insert_random_file(self.client, path, file_size)

    # Check that the files exist
    for path in paths:
      self.assertTrue(self.aws.exists(path))

    # Delete the tree
    self.aws.delete_tree(root_path)

    # Check that the files have been deleted
    for path in paths:
      self.assertFalse(self.aws.exists(path))

  def test_exists(self):
    file_name = self.TEST_DATA_PATH + 'exists'
    file_size = 1024

    self.assertFalse(self.aws.exists(file_name))

    self._insert_random_file(self.aws.client, file_name, file_size)

    self.assertTrue(self.aws.exists(file_name))

    # Clean up
    self.aws.delete(file_name)

    self.assertFalse(self.aws.exists(file_name))

  def test_file_mode(self):
    file_name = self.TEST_DATA_PATH + 'jerry/pigpen/bobby'
    with self.aws.open(file_name, 'w') as f:
      assert f.mode == 'w'
    with self.aws.open(file_name, 'r') as f:
      assert f.mode == 'r'

    # Clean up
    self.aws.delete(file_name)

  def test_full_file_read(self):
    file_name = self.TEST_DATA_PATH + 'jerry/pigpen/phil'
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

    # Clean up
    self.aws.delete(file_name)

  def test_file_write(self):
    file_name = self.TEST_DATA_PATH + 'write_file'
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

    # Clean up
    self.aws.delete(file_name)


  def test_file_random_seek(self):
    file_name = self.TEST_DATA_PATH + 'write_seek_file'
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
    
    # Clean up
    self.aws.delete(file_name)

  def test_file_flush(self):
    file_name = self.TEST_DATA_PATH + 'flush_file'
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

    # Clean up
    self.aws.delete(file_name)

  def test_file_iterator(self):
    file_name = self.TEST_DATA_PATH + 'iterate_file'
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

    # Clean up
    self.aws.delete(file_name)

  def test_file_read_line(self):
    file_name = self.TEST_DATA_PATH + 'read_line_file'
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

    # Clean up
    self.aws.delete(file_name)

  def test_file_close(self):
    file_name = self.TEST_DATA_PATH + 'close_file'
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

    # Clean up
    self.aws.delete(file_name)

  def test_context_manager(self):
    # Test writing with a context manager.
    file_name = self.TEST_DATA_PATH + 'context_manager_file'
    file_size = 1024
    contents = os.urandom(file_size)
    with self.aws.open(file_name, 'w') as f:
      f.write(contents)

    with self.aws.open(file_name, 'r') as f:
      self.assertEqual(f.read(), contents)

    # Clean up
    self.aws.delete(file_name)

  def test_list_prefix(self):

    objects = [
        ('jerry/pigpen/phil', 5),
        ('jerry/pigpen/bobby', 3),
        ('jerry/billy/bobby', 4),
    ]

    for (object_name, size) in objects:
      file_name = self.TEST_DATA_PATH + object_name
      self._insert_random_file(self.aws.client, file_name, size)

    test_cases = [
        (self.TEST_DATA_PATH + 'j', [
            ('jerry/pigpen/phil', 5),
            ('jerry/pigpen/bobby', 3),
            ('jerry/billy/bobby', 4),
        ]),
        (self.TEST_DATA_PATH + 'jerry/', [
            ('jerry/pigpen/phil', 5),
            ('jerry/pigpen/bobby', 3),
            ('jerry/billy/bobby', 4),
        ]),
        (self.TEST_DATA_PATH + 'jerry/pigpen/phil', [
            ('jerry/pigpen/phil', 5),
        ]),
    ]

    for file_pattern, expected_object_names in test_cases:
      expected_file_names = [(self.TEST_DATA_PATH + object_name, size)
                             for (object_name, size) in expected_object_names]
      self.assertEqual(
          set(self.aws.list_prefix(file_pattern).items()),
          set(expected_file_names))

    # Clean up
    for (object_name, size) in objects: 
      self.aws.delete(self.TEST_DATA_PATH + object_name)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()

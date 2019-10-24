# -*- coding: utf-8 -*-
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

"""Unit tests for the S3 File System"""

from __future__ import absolute_import

import logging
import unittest
import mock

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import FileMetadata
from apache_beam.options.pipeline_options import PipelineOptions

# Protect against environments where boto3 library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.aws import s3filesystem
except ImportError:
  s3filesystem = None
# pylint: enable=wrong-import-order, wrong-import-position


@unittest.skipIf(s3filesystem is None, 'AWS dependencies are not installed')
class S3FileSystemTest(unittest.TestCase):

  def setUp(self):
    pipeline_options = PipelineOptions()
    self.fs = s3filesystem.S3FileSystem(pipeline_options=pipeline_options)

  def test_scheme(self):
    self.assertEqual(self.fs.scheme(), 's3')
    self.assertEqual(s3filesystem.S3FileSystem.scheme(), 's3')

  def test_join(self):
    self.assertEqual('s3://bucket/path/to/file',
                     self.fs.join('s3://bucket/path', 'to', 'file'))
    self.assertEqual('s3://bucket/path/to/file',
                     self.fs.join('s3://bucket/path', 'to/file'))
    self.assertEqual('s3://bucket/path/to/file',
                     self.fs.join('s3://bucket/path', '/to/file'))
    self.assertEqual('s3://bucket/path/to/file',
                     self.fs.join('s3://bucket/path/', 'to', 'file'))
    self.assertEqual('s3://bucket/path/to/file',
                     self.fs.join('s3://bucket/path/', 'to/file'))
    self.assertEqual('s3://bucket/path/to/file',
                     self.fs.join('s3://bucket/path/', '/to/file'))
    with self.assertRaises(ValueError):
      self.fs.join('/bucket/path/', '/to/file')

  def test_split(self):
    self.assertEqual(('s3://foo/bar', 'baz'),
                     self.fs.split('s3://foo/bar/baz'))
    self.assertEqual(('s3://foo', ''),
                     self.fs.split('s3://foo/'))
    self.assertEqual(('s3://foo', ''),
                     self.fs.split('s3://foo'))

    with self.assertRaises(ValueError):
      self.fs.split('/no/s3/prefix')

  def test_match_multiples(self):
    # Prepare mocks.
    s3io_mock = mock.MagicMock()
    s3filesystem.s3io.S3IO = lambda: s3io_mock
    s3io_mock.list_prefix.return_value = {
        's3://bucket/file1': 1,
        's3://bucket/file2': 2
    }
    expected_results = set([
        FileMetadata('s3://bucket/file1', 1),
        FileMetadata('s3://bucket/file2', 2)
    ])
    match_result = self.fs.match(['s3://bucket/'])[0]

    self.assertEqual(
        set(match_result.metadata_list),
        expected_results)
    s3io_mock.list_prefix.assert_called_once_with('s3://bucket/')

  def test_match_multiples_limit(self):
    # Prepare mocks.
    s3io_mock = mock.MagicMock()
    limit = 1
    s3filesystem.s3io.S3IO = lambda: s3io_mock
    s3io_mock.list_prefix.return_value = {
        's3://bucket/file1': 1
    }
    expected_results = set([
        FileMetadata('s3://bucket/file1', 1)
    ])
    match_result = self.fs.match(['s3://bucket/'], [limit])[0]
    self.assertEqual(
        set(match_result.metadata_list),
        expected_results)
    self.assertEqual(
        len(match_result.metadata_list),
        limit)
    s3io_mock.list_prefix.assert_called_once_with('s3://bucket/')

  def test_match_multiples_error(self):
    # Prepare mocks.
    s3io_mock = mock.MagicMock()
    s3filesystem.s3io.S3IO = lambda: s3io_mock
    exception = IOError('Failed')
    s3io_mock.list_prefix.side_effect = exception

    with self.assertRaisesRegex(BeamIOError,
                                r'^Match operation failed') as error:
      self.fs.match(['s3://bucket/'])
    self.assertRegex(str(error.exception.exception_details),
                     r's3://bucket/.*%s' % exception)
    s3io_mock.list_prefix.assert_called_once_with('s3://bucket/')

  def test_match_multiple_patterns(self):
    # Prepare mocks.
    s3io_mock = mock.MagicMock()
    s3filesystem.s3io.S3IO = lambda: s3io_mock
    s3io_mock.list_prefix.side_effect = [
        {'s3://bucket/file1': 1},
        {'s3://bucket/file2': 2},
    ]
    expected_results = [
        [FileMetadata('s3://bucket/file1', 1)],
        [FileMetadata('s3://bucket/file2', 2)]
    ]
    result = self.fs.match(['s3://bucket/file1*', 's3://bucket/file2*'])
    self.assertEqual(
        [mr.metadata_list for mr in result],
        expected_results)

  def test_create(self):
    # Prepare mocks.
    s3io_mock = mock.MagicMock()
    s3filesystem.s3io.S3IO = lambda: s3io_mock
    # Issue file copy
    _ = self.fs.create('s3://bucket/from1', 'application/octet-stream')

    s3io_mock.open.assert_called_once_with(
        's3://bucket/from1', 'wb', mime_type='application/octet-stream')

  def test_open(self):
    # Prepare mocks.
    s3io_mock = mock.MagicMock()
    s3filesystem.s3io.S3IO = lambda: s3io_mock
    # Issue file copy
    _ = self.fs.open('s3://bucket/from1', 'application/octet-stream')

    s3io_mock.open.assert_called_once_with(
        's3://bucket/from1', 'rb', mime_type='application/octet-stream')

  def test_delete(self):
    # Prepare mocks.
    s3io_mock = mock.MagicMock()
    s3filesystem.s3io.S3IO = lambda: s3io_mock
    s3io_mock.size.return_value = 0
    files = [
        'gs://bucket/from1',
        'gs://bucket/from2',
        'gs://bucket/from3',
    ]

    # Issue batch delete.
    self.fs.delete(files)
    s3io_mock.delete_batch.assert_called()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()

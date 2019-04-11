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
"""S3 file system implementation for accessing files on AWS S3."""

from __future__ import absolute_import

from apache_beam.io.filesystem import BeamIOError
from apache_beam.io.filesystem import CompressedFile
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystem import FileMetadata
from apache_beam.io.filesystem import FileSystem

__all__ = ['S3FileSystem']


class S3FileSystem(FileSystem):
  """An S3 `FileSystem` implementation for accessing files on AWS S3
  """

  CHUNK_SIZE = 100
  S3_PREFIX = 's3://'

  @classmethod
  def scheme(cls):
    """URI scheme for the FileSystem
    """
    return 's3'

  def join(self, basepath, *paths):
    """Join two or more pathname components for the filesystem

    Args:
      basepath: string path of the first component of the path
      paths: path components to be added

    Returns: full path after combining all of the return nulled components
    """
    if not basepath.startswith(S3FileSystem.S3_PREFIX):
      raise ValueError('Basepath %r must be S3 path.' % basepath)

    path = basepath
    for p in paths:
      path = path.rstrip('/') + '/' + p.lstrip('/')
    return path

  def split(self, path):
    path = path.strip()
    if not path.startswith(S3FileSystem.S3_PREFIX):
      raise ValueError('Path %r must be S3 path.' % path)

    prefix_len = len(S3FileSystem.S3_PREFIX)
    last_sep = path[prefix_len:].rfind('/')
    if last_sep >= 0:
      last_sep += prefix_len

    if last_sep > 0:
      return (path[:last_sep], path[last_sep + 1:])
    elif last_sep < 0:
      return (path, '')
    else:
      raise ValueError('Invalid path: %s' % path)

  def mkdirs(self, path):
    """Recursively create directories for the provided path.

    Args:
      path: string path of the directory structure that should be created

    Raises:
      IOError if leaf directory already exists.
    """
    raise NotImplementedError

  def has_dirs(self):
    """Whether this FileSystem supports directories."""
    raise NotImplementedError

  def _list(self, dir_or_prefix):
    """List files in a location.

    Listing is non-recursive, for filesystems that support directories.

    Args:
      dir_or_prefix: (string) A directory or location prefix (for filesystems
        that don't have directories).

    Returns:
      Generator of ``FileMetadata`` objects.

    Raises:
      ``BeamIOError`` if listing fails, but not if no files were found.
    """
    raise NotImplementedError

  def create(self, path, mime_type='application/octet-stream',
             compression_type=CompressionTypes.AUTO):
    """Returns a write channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object

    Returns: file handle with a close function for the user to use
    """
    raise NotImplementedError

  def open(self, path, mime_type='application/octet-stream',
           compression_type=CompressionTypes.AUTO):
    """Returns a read channel for the given file path.

    Args:
      path: string path of the file object to be written to the system
      mime_type: MIME type to specify the type of content in the file object
      compression_type: Type of compression to be used for this object

    Returns: file handle with a close function for the user to use
    """
    raise NotImplementedError

  def copy(self, source_file_names, destination_file_names):
    """Recursively copy the file tree from the source to the destination

    Args:
      source_file_names: list of source file objects that needs to be copied
      destination_file_names: list of destination of the new object

    Raises:
      ``BeamIOError`` if any of the copy operations fail
    """
    err_msg = ("source_file_names and destination_file_names should "
               "be equal in length")
    raise NotImplementedError

  def rename(self, source_file_names, destination_file_names):
    """Rename the files at the source list to the destination list.
    Source and destination lists should be of the same size.

    Args:
      source_file_names: List of file paths that need to be moved
      destination_file_names: List of destination_file_names for the files

    Raises:
      ``BeamIOError`` if any of the rename operations fail
    """
    err_msg = ("source_file_names and destination_file_names should "
               "be equal in length")
    raise NotImplementedError

  def exists(self, path):
    """Check if the provided path exists on the FileSystem.

    Args:
      path: string path that needs to be checked.

    Returns: boolean flag indicating if path exists
    """
    raise NotImplementedError

  def size(self, path):
    """Get size of path on the FileSystem.

    Args:
      path: string path in question.

    Returns: int size of path according to the FileSystem.

    Raises:
      ``BeamIOError`` if path doesn't exist.
    """
    raise NotImplementedError

  def last_updated(self, path):
    """Get UNIX Epoch time in seconds on the FileSystem.

    Args:
      path: string path of file.

    Returns: float UNIX Epoch time

    Raises:
      ``BeamIOError`` if path doesn't exist.
    """
    raise NotImplementedError

  def checksum(self, path):
    """Fetch checksum metadata of a file on the
    :class:`~apache_beam.io.filesystem.FileSystem`.

    Args:
      path: string path of a file.

    Returns: string containing checksum

    Raises:
      ``BeamIOError`` if path isn't a file or doesn't exist.
    """
    raise NotImplementedError

  def delete(self, paths):
    raise NotImplementedError

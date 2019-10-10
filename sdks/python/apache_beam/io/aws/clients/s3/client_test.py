import unittest

import os
from apache_beam.io.aws.clients.s3 import fake_client, messages

# Protect against environments where boto3 library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apache_beam.io.aws.clients.s3 import boto3_client
  from apache_beam.io.aws import s3io
except ImportError:
  pass
# pylint: enable=wrong-import-order, wrong-import-position

class ClientErrorTest(unittest.TestCase):

  def setUp(self):
    self.client = fake_client.FakeS3Client()
    # self.client = boto3_client.Client()
    self.aws = s3io.S3IO(self.client)
  
  def test_get_object_metadata(self):
    
    # Test nonexistent bucket/object
    bucket, object = 'random-data-sets', '_nonexistent_file_doesnt_exist'
    request = messages.GetRequest(bucket, object)
    self.assertRaises(messages.S3ClientError, 
                      self.client.get_object_metadata, 
                      request)

    try:
      self.client.get_object_metadata(request)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertTrue(e.code == 404)
  
  def test_get_range_nonexistent(self):

    # Test nonexistent bucket/object
    bucket, object = 'random-data-sets', '_nonexistent_file_doesnt_exist'
    request = messages.GetRequest(bucket, object)
    self.assertRaises(messages.S3ClientError, 
                      self.client.get_range, 
                      request, 0, 10)

    try:
      self.client.get_range(request, 0, 10)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertTrue(e.code == 404)

  def test_get_range_bad_start_end(self):

    file_name = 's3://random-data-sets/_get_range'
    contents = os.urandom(1024)

    with self.aws.open(file_name, 'w') as f:
      f.write(contents)
    
    bucket, object = s3io.parse_s3_path(file_name)

    response = self.client.get_range(messages.GetRequest(bucket, object), -10, 20)
    self.assertEqual(response, contents)

    response = self.client.get_range(messages.GetRequest(bucket, object), 20, 10)
    self.assertEqual(response, contents)
    

    
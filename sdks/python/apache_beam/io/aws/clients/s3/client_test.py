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
      self.assertEqual(e.code, 404)
  
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
      self.assertEqual(e.code, 404)

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
    
  def test_upload_part_nonexistent_upload_id(self):

    bucket, object = 'random-data-sets', '_upload_part'
    upload_id = 'not-an-id-12345'
    part_number = 1
    contents = os.urandom(1024)

    request = messages.UploadPartRequest(bucket, 
                                         object, 
                                         upload_id, 
                                         part_number, 
                                         contents)

    self.assertRaises(messages.S3ClientError, 
                      self.client.upload_part, 
                      request)

    try:
      response = self.client.upload_part(request)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertEqual(e.code, 404)

  def test_upload_part_bad_number(self):

    bucket, object = 'random-data-sets', '_upload_part'
    contents = os.urandom(1024)

    request = messages.UploadRequest(bucket, object, None)
    response = self.client.create_multipart_upload(request)
    upload_id = response.upload_id

    part_number = 0.5
    request = messages.UploadPartRequest(bucket, 
                                         object, 
                                         upload_id, 
                                         part_number, 
                                         contents)

    self.assertRaises(messages.S3ClientError, 
                      self.client.upload_part, 
                      request)

    try:
      response = self.client.upload_part(request)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertEqual(e.code, 400)

  def test_complete_multipart_upload_too_small(self):

    bucket, object = 'random-data-sets', '_upload_part'
    request = messages.UploadRequest(bucket, object, None)
    response = self.client.create_multipart_upload(request)
    upload_id = response.upload_id

    part_number = 1
    contents_1 = os.urandom(1024)
    request_1 = messages.UploadPartRequest(bucket, 
                                         object, 
                                         upload_id, 
                                         part_number, 
                                         contents_1)
    response_1 = self.client.upload_part(request_1)


    part_number = 2
    contents_2 = os.urandom(1024)
    request_2 = messages.UploadPartRequest(bucket, 
                                         object, 
                                         upload_id, 
                                         part_number, 
                                         contents_2)
    response_2 = self.client.upload_part(request_2)

    parts = [
      {'PartNumber': 1, 'ETag': response_1.etag},
      {'PartNumber': 2, 'ETag': response_2.etag}
    ]
    complete_request = messages.CompleteMultipartUploadRequest(bucket, object, upload_id, parts)

    try:
      response = self.client.complete_multipart_upload(complete_request)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertEqual(e.code, 400)

    response

  def test_complete_multipart_upload_too_many(self):

    bucket, object = 'random-data-sets', '_upload_part'
    request = messages.UploadRequest(bucket, object, None)
    response = self.client.create_multipart_upload(request)
    upload_id = response.upload_id

    part_number = 1
    contents_1 = os.urandom(5 * 1024)
    request_1 = messages.UploadPartRequest(bucket, 
                                         object, 
                                         upload_id, 
                                         part_number, 
                                         contents_1)
    response_1 = self.client.upload_part(request_1)


    part_number = 2
    contents_2 = os.urandom(1024)
    request_2 = messages.UploadPartRequest(bucket, 
                                         object, 
                                         upload_id, 
                                         part_number, 
                                         contents_2)
    response_2 = self.client.upload_part(request_2)

    parts = [
      {'PartNumber': 1, 'ETag': response_1.etag},
      {'PartNumber': 2, 'ETag': response_2.etag},
      {'PartNumber': 3, 'ETag': 'fake-etag'},
    ]
    complete_request = messages.CompleteMultipartUploadRequest(bucket, object, upload_id, parts)

    try:
      response = self.client.complete_multipart_upload(complete_request)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertEqual(e.code, 400)

    response
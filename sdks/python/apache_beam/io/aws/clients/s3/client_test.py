import unittest

from apache_beam.io.aws.clients.s3 import boto3_client, fake_client, messages

class ClientErrorTest(unittest.TestCase):

  def setUp(self):
    self.client = fake_client.FakeS3Client()
    # self.client = boto3_client.Client()
  
  def test_get_object_metadata(self):
    
    # Test nonexistent bucket/object
    bucket, object = 'random-data-sets', '_nonexistent_file_doesnt_exist'
    request = messages.GetRequest(bucket, object)

    self.assertRaises(messages.S3ClientError, self.client.get_object_metadata, request)

    try:
      self.client.get_object_metadata(request)
    except Exception as e:
      self.assertIsInstance(e, messages.S3ClientError)
      self.assertTrue(e.code == 404)
    
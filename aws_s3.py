
import boto3
import os
from botocore.client import ClientError
import logging


logger = logging.getLogger(__name__)

class S3Handler(boto3.client):
	'''
	S3.Client
	https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#client
	'''
	def __init__(self):
		super().__init__('s3')
		
	def get_bucket(self,bucket,**kwargs):
		'''
		https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_object
		'''
		return self.get_object(Bucket=bucket,**kwargs)
	def get_bucket_if_exist(self, bucket,**kwargs):
		if self.bucket_exists(bucket):
			return self.get_object(Bucket=bucket,**kwargs)
	def parse_client_error(ce,resource,message=''):
		'''
		All client exceptions are raised as ClientError
		'''
		if not isinstance(ce,ClientError):
			if isinstance(ce,Exception):
				raise ce
			return ce
		if ce.response['Error']['Code'].isnumeric():
			error_code = int(ce.response['Error']['Code'])
		else:
			error_code = int(ce.response['ResponseMetadata']['HTTPStatusCode'])
		logger.error(f"ClientError Message: {ce.response['Error']['Message']} Error Code: {error_code}")
		if error_code == 403:
			logger.error(f"Private {resource}. Forbidden Access!{message}")
		elif error_code == 404:
			logger.error(f"{resource} Does Not Exist!{message}")
		ce['Error']['Error Code'] = error_code
		return ce['Error']
	def bucket_exists(self, bucket):
		'''
		head_bucket only returns metadata
		https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_bucket
		'''
		try:
			self.head_bucket(Bucket=bucket)
			return True
		except ClientError as e:
			# If a client error is thrown, then check that it was a 404 error.
			# If it was a 404 error, then the bucket does not exist.
			
			# error_code = int(e.response['Error']['Code'])
			error = self.parse_client_error(e,'Bucket',f' Bucket:{bucket}')
			if error['Error Code'] == 403:
				return True
			return False

	def create_bucket(self,bucket,raise_if_exists=True,**kwargs):
		'''
		https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.create_bucket
		'''
		if raise_if_exists:
			if self.bucket_exists(bucket):
				raise KeyError(f'bucket: {bucket} exists')
		return self.create_bucket(bucket,**kwargs)
	def wait(self,waiter,*args,**kwargs):
		'''
		https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#waiters
		https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.get_waiter
		'''
		waiter = self.get_waiter(waiter)
		waiter.wait(*args,**kwargs)

	def key_exists(self, bucket, key):
		try:
			self.head_object(Bucket=bucket, Key=key)
			return True
		except ClientError as e:
			# If a client error is thrown, then check that it was a 404 error.
			# If it was a 404 error, then the bucket does not exist.
			# error_code = int(e.response['Error']['Code'])
			error = self.parse_client_error(e,'Bucket',f' Bucket:{bucket} Key:{key}')
			if error['Error Code'] == 403:
				return True
			return False
		

	def get_all_keys(self, s3_bucket,paginate=True,**kwargs):
		'''
		if paginate then s3_bucket should be a string otherwise a S3.Bucket
		'''
		if paginate:
			paginator = self.get_paginator('list_objects_v2')
			yield from paginator.paginate(s3_bucket,**kwargs)
		else:
			return s3_bucket.objects.all()

	def download_file(self, bucket, key, file,**kwargs):
		'''
		wraps
		https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_file
		and
		https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.download_fileobj
		'''
		if isinstance(bucket,boto3.S3.Bucket):
			bucket = bucket.name
		try:
			
			if isinstance(file,str):
				self.download_file(bucket, key, file,**kwargs)
			else:
				# dst must be an open file like object in binary mode
				self.download_fileobj(bucket, key, file,**kwargs)
			return True
		except ClientError as e:
			self.parse_client_error(e,'Bucket',f' Bucket:{bucket} Key:{key}, download_file: {file}')	
		return False
	def upload_file(self,file,bucket,key,**kwargs):
		'''
		wraps
		https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_file
		and
		https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_fileobj
		'''
		if isinstance(bucket,boto3.S3.Bucket):
			bucket = bucket.name
		try:
			
			if isinstance(file,str):
				if 'base_name_key' in kwargs:
					base_name = os.path.basename(file)
					key = base_name
				self.upload_file(file,bucket, key, **kwargs)
			else:
				# dst must be an open file like object in binary mode
				self.upload_fileobj(file,bucket, key, **kwargs)
			return True
		except ClientError as e:
			self.parse_client_error(e,'Bucket',f' Bucket:{bucket} Key:{key}, upload_file: {file}')				
		return False

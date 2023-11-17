
import os
from botocore.client import ClientError
import logging
from decimal import Decimal
import logging
import os
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)

class Dynamo(boto3.resource):
	'''
	DynamoDB.Client
	https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
	'''
	def __init__(self):
		super().__init__('dynamodb')
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
	def exists(self, table_name):
		"""
		Determines whether a table exists. As a side effect, stores the table in
		a member variable.

		:param table_name: The name of the table to check.
		:return: True when the table exists; otherwise, False.
		"""
		try:
			table = self.Table(table_name)
			table.load()
			exists = True
		except ClientError as err:
			if err.response['Error']['Code'] == 'ResourceNotFoundException':
				exists = False
			else:
				self.parse_client_error(table_name,err)
		else:
			self.table = table
		return exists

	
	def create_table(self, table_name):
		"""
		Creates an Amazon DynamoDB table that can be used to store movie data.
		The table uses the release year of the movie as the partition key and the
		title as the sort key.

		:param table_name: The name of the table to create.
		:return: The newly created table.
		"""
		try:
			self.table = self.create_table(
				TableName=table_name,
				KeySchema=[
					{'AttributeName': 'year', 'KeyType': 'HASH'},  # Partition key
					{'AttributeName': 'title', 'KeyType': 'RANGE'}  # Sort key
				],
				AttributeDefinitions=[
					{'AttributeName': 'year', 'AttributeType': 'N'},
					{'AttributeName': 'title', 'AttributeType': 'S'}
				],
				ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
			self.table.wait_until_exists()
		except ClientError as err:
			self.parse_client_error(table_name,err)
			raise
		else:
			return self.table

	def list_tables(self):
		"""
		Lists the Amazon DynamoDB tables for the current account.

		:return: The list of tables.
		"""
		try:
			tables = []
			for table in self.tables.all():
				print(table.name)
				tables.append(table)
		except ClientError as err:
			self.parse_client_error('list_tables',err)
			raise
		else:
			return tables

	def write_batch(self, items):
		
		try:
			with self.table.batch_writer() as writer:
				for item in items:
					writer.put_item(Item=item)
		except ClientError as err:
			self.parse_client_error(f'{self.table.name} batch_write',err)
			raise

	def put(self, item):
		
		try:
			self.table.put_item(Item=item)
		except ClientError as err:
			self.parse_client_error(f'{self.table.name} put',err)
			raise

	def get(self, key):
		
		try:
			response = self.table.get_item(Key=key)
		except ClientError as err:
			self.parse_client_error(f'{self.table.name} get',err)
			raise
		else:
			return response['Item']

	def update(self, key,item,**kwargs):
		
		try:
			response = self.table.update_item(
				Key=key,**kwargs)
		except ClientError as err:
			self.parse_client_error(f'{self.table.name} update',err)
			raise
		else:
			return response['Attributes']

	def query(self, key,value):
		
		try:
			response = self.table.query(KeyConditionExpression=Key(key).eq(value))
		except ClientError as err:
			self.parse_client_error(f'{self.table.name} query',err)
			raise
		else:
			return response['Items']

	def scan_movies(self, year_range):
		movies = []
		scan_kwargs = {
			'FilterExpression': Key('year').between(year_range['first'], year_range['second']),
			'ProjectionExpression': "#yr, title, info.rating",
			'ExpressionAttributeNames': {"#yr": "year"}}
		try:
			done = False
			start_key = None
			while not done:
				if start_key:
					scan_kwargs['ExclusiveStartKey'] = start_key
				response = self.table.scan(**scan_kwargs)
				movies.extend(response.get('Items', []))
				start_key = response.get('LastEvaluatedKey', None)
				done = start_key is None
		except ClientError as err:
			self.parse_client_error(f'{self.table.name} scan',err)
			raise

		return movies

	def delete(self, key):
		try:
			self.table.delete_item(Key=key)
		except ClientError as err:
			self.parse_client_error(f'{self.table.name} delete',err)
			raise

	def delete_table(self):
		"""
		Deletes the table.
		"""
		try:
			self.table.delete()
			self.table = None
		except ClientError as err:
			self.parse_client_error(f'{self.table.name} delete_table',err)
			raise


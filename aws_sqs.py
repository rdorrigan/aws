
import boto3
import os

from yaml import safe_load

# logger = logging.getLogger(__name__)
name = os.path.basename(__file__).split(".")[0]
os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'
os.environ['AWS_REGION'] = 'us-west-2'

class SQSQueue():
	'''
	S3.Resource
	https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/queue/index.html
	'''
	def __init__(self,queue_url):
		# super().__init__('sqs')
		self.resource = boto3.resource('sqs')
		if not queue_url:
			creds = load_credentials()
			# if not queue_name:
			# 	queue_name = creds['sqsName']
			# response = boto3.client('sqs').get_queue_url(
			# 	QueueName=queue_name,
			# 	QueueOwnerAWSAccountId=creds['iam.accountId']
			# )
			# queue_url = response['QueueUrl']
			queue_url = creds['sqsReportsURL']
		self.queue = self.resource.Queue(queue_url)
		self.queue.load()
		self.message_count = self.queue.attributes['ApproximateNumberOfMessages']
	def poll(self,**kwargs):
		if kwargs.get('WaitTimeSeconds',0) < 1:
			kwargs['WaitTimeSeconds'] = 20
		if kwargs.get('MaxNumberOfMessages',0) < 1:
			kwargs['MaxNumberOfMessages'] = 10
		return self.queue.receive_messages(**kwargs)
	def load_messages(self,messages=[],load_all=False):
		if not messages:
			messages = self.poll()
		self.queue.load()
		for m in messages:
			yield Message(m)
		if load_all:
			message_count = len(messages)
			while message_count < self.message_count:
				messages = self.poll()
				yield from self.load_messages(messages=messages)
				message_count += len(messages)
				
	
def load_credentials(cred_file = "amazon.properties"):
	
	with open(cred_file, "r") as f:
		props = f.readlines()
	prop_creds = {}
	# print(props)
	for p in props:
		if "=" in p:
			temp_key, val = p.strip().split(" = ")
			prop_creds[temp_key] = val
		elif ":" in p:
			temp_key, val = p.strip().split(": ")
			prop_creds[temp_key] = val
	return prop_creds

class Message():
	def __init__(self,message):
		self.raw_message = message
		if isinstance(message.body,str):
			self.body = safe_load(message.body)
		elif isinstance(message.body,dict):
			self.body = message.body
		if isinstance(message.attributes,str):
			self.attributes = safe_load(message.attributes)
		elif isinstance(message.attributes,dict):
			self.attributes = message.attributes
	def delete(self):
		return self.raw_message.delete()



import boto3
from botocore.exceptions import ClientError

REGION_NAME = "us-east-2"


def new_session(region_name=REGION_NAME):
	session = boto3.session.Session()
	client = session.client(
		service_name="secretsmanager",
		region_name=region_name,
	)
	return client


def get_secret(
	client=None, secret_name="Test-Key-Name", region_name=REGION_NAME, **kwargs
):
	# secret_name = "MySecretName"
	# region_name = "us-west-2"
	if client is None:
		client = new_session(region_name=region_name)

	try:
		get_secret_value_response = client.get_secret_value(
			SecretId=secret_name, **kwargs
		)
	except ClientError as e:
		if e.response["Error"]["Code"] == "ResourceNotFoundException":
			print("The requested secret " + secret_name + " was not found")
		elif e.response["Error"]["Code"] == "InvalidRequestException":
			print("The request was invalid due to:", e)
		elif e.response["Error"]["Code"] == "InvalidParameterException":
			print("The request had invalid params:", e)
		elif e.response["Error"]["Code"] == "DecryptionFailure":
			print(
				"The requested secret can't be decrypted using the provided KMS key:", e
			)
		elif e.response["Error"]["Code"] == "InternalServiceError":
			print("An error occurred on service side:", e)
	else:
		# Secrets Manager decrypts the secret value using the associated KMS CMK
		# Depending on whether the secret was a string or binary, only one of these fields will be populated

		secret_data = (
			get_secret_value_response["SecretString"]
			if "SecretString" in get_secret_value_response
			else get_secret_value_response["SecretBinary"]
		)
		print("Secret Key: {} Secret Value: {}".format(secret_name, secret_data))
		# if 'SecretString' in get_secret_value_response:
		# 	text_secret_data = get_secret_value_response['SecretString']
		# 	print(text_secret_data)
		# else:
		# 	binary_secret_data = get_secret_value_response['SecretBinary']
		# 	print(binary_secret_data)


def list_secrets(client=None, region_name=REGION_NAME, **kwargs):
	if client is None:
		client = new_session(region_name=region_name)
	try:
		secrets = client.list_secrets(**kwargs)
	except ClientError as e:
		if e.response["Error"]["Code"] == "ResourceNotFoundException":
			print("The requested resource was not found")
		elif e.response["Error"]["Code"] == "InvalidRequestException":
			print("The request was invalid due to:", e)
		elif e.response["Error"]["Code"] == "InvalidParameterException":
			print("The request had invalid params:", e)
		elif e.response["Error"]["Code"] == "DecryptionFailure":
			print(
				"The requested secret can't be decrypted using the provided KMS key:", e
			)
		elif e.response["Error"]["Code"] == "InternalServiceError":
			print("An error occurred on service side:", e)
	else:
		for secret in secrets["SecretList"]:
			print("Secret Name: {} Secret Data: {}".format(secret["Name"], secret))


def update_secret(
	client=None, secret_name="Test-Key-Name", region_name=REGION_NAME, **kwargs
):
	# secret_name = "MySecretName"
	# region_name = "us-west-2"
	if client is None:
		client = new_session(region_name=region_name)

		return client.update_secret(SecretId=secret_name, **kwargs)

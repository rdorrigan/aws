import os
import subprocess
import shutil
import argparse
import zipfile
import boto3
from platform import platform
import glob
# from botocore.exceptions import ClientError
# import logging


# class Lambda(boto3.client):
#     def __init__(self) -> None:
#          super().__init__('lambda')

def run_subprocess(args):
	'''
	use subprocess.run to run command line args
	'''
	print(args)
	if not 'Windows' in platform():
		if isinstance(args,str):
			args = [args]
	try:
		cp = subprocess.run(args,shell=False,capture_output=True,text=True)
		cp.check_returncode()
	except subprocess.CalledProcessError as err:
		print(err)
def check_path(path):
	'''
	If path is a file then make a new directory with the name of the file
	'''
	path = unquote_path(path)
	if os.path.exists(path):
		if os.path.isfile(path):
			dir_name = os.path.dirname(path)
			base_name_ = base_name(path)
			package_name = clean_path(dir_name,base_name_)
			os.mkdir(package_name)
			os.rename(path,clean_path(package_name,base_name(path)))
			return package_name
	elif os.path.isfile(path):
		os.makedirs(os.path.dirname(path))
	else:
		os.makedirs(path)
	return path
def create_init_setup(path,name='__init__'):
	'''
	create init and setup files if they don't exists
	'''
	init_path = clean_path(path,f'{name}.py')
	if not path_exists(init_path):
		if name == 'setup':
			with open(init_path,'w') as f:
				f.write(create_setup(base_name(path)))
		else:
			with open(init_path,'w') as f:
				f.flush()
	return init_path

def create_setup(name,**kwargs):
	'''
	Lazy setup.py file
	TODO: add all kwargs
	'''
	return f'''
	from setuptools import setup
	setup(
	name='{name}',
	version={kwargs.get('version','1.0')},
	description={kwargs.get('description','A useful module')},
	author={kwargs.get('author','Some Guy')},
	author_email={kwargs.get('author_email','dontemailme@gmail.com')},
	packages=['{name}'],  #same as name
	install_requires=['wheel', 'bar', 'greek'], #external packages as dependencies
	)'''

def create_requirements_txt(path):
	'''
	create requirements.txt for directory path
	'''
	run_subprocess(f'pipreqs --encoding utf-8 {quote_path(path)}')
	create_init_setup(path,'setup')

def install_requirements(path):
	'''
	install requirements.txt to target package
	'''
	
	if not path_exists(f'{path}/package'):
		os.mkdir(f'{path}/package')
		create_init_setup(f'{path}/package')
	#--target utf-8
	run_subprocess(f'pip install -t {quote_path(clean_path(path,"package"))} -r {quote_path(clean_path(path,"requirements.txt"))}')
def zip_package(path):
	'''
	Zip package for lambda deployment
	'''
	shutil.make_archive(clean_path(path,os.path.basename(path)),format='zip',root_dir=path)

def prepare_package(path):
	'''
	Essentially a main function excluding the deployment step storing everything locally
	'''
	path = check_path(path)
	create_requirements_txt(path)
	install_requirements(path)
	zip_package(path)
def deploy_package(path,aws_cli):
	'''
	Deploy package to AWS Lambda using AWS CLI or boto3
	'''
	function_name = os.path.splitext(os.path.basename(path))[0]
	if aws_cli:
		args = f'aws lambda update-function-code --function-name {function_name} --zip-file fileb://{quote_path(path)}/{function_name}.zip'
		run_subprocess(args)
	else:
		lambda_client = boto3.client('lambda')
		with zipfile.ZipFile(path,'r') as zip_file:
			zip_bytes = zip_file.read()
		lambda_client.update_function_code(FunctionName=function_name,ZipFile=zip_bytes)

def clean_dir(path,exclusions=[],dry_run=True):
	'''
	Remove everything but the python file within a directory
	'''
	for f in os.listdir(path):
		if exclusions:
			if f in exclusions:
				continue
		full_path = clean_path(path,f)
		if os.path.isdir(full_path):
			if dry_run:
				print(f'{full_path} would have been removed')
			else:
				os.remove(full_path)
				print(f'{full_path} removed')
		elif os.path.splitext(f)[1] != '.py':
			if dry_run:
				print(f'{full_path} would have been removed')
			else:
				os.remove(full_path)
				print(f'{full_path} removed')
def base_name(path):
	'''
	File base name
	'''
	return os.path.splitext(os.path.basename(path))[0]
def path_exists(path):
	'''
	Check if path exists
	'''
	return os.path.exists(unquote_path(path))
def unquote_path(path):
	'''
	Remove quotes that are needed for subprocess arg
	'''
	return path.replace('"','')
def clean_path(*paths):
	'''
	Make paths unix like removing windows back slash
	'''
	return '/'.join(map(str, paths)).replace('\\','/').replace('//', '/')
def quote_path(path):
	'''
	Add quotes for windows
	'''
	if 'Windows' in platform():
		return '"' + path + '"'
	return path
def get_py_files(path):
	files = glob.glob(clean_path(path,'*.py'))
	if len(files) > 1:
		for f in files:
			base_name_ = base_name(f)
			base_dir = clean_path(path,base_name_)
			if path_exists(base_dir):
				continue
			check_path(base_dir)
			shutil.copy2(f,clean_path(base_dir,base_name(f)))
			yield base_dir
	else:
		yield path
def main(path,aws_cli,deploy=False,**kwargs):
	'''
	Main: just do it
	'''
	if not path:
		path = input("Path to deploy")
	if not path_exists(path):
		raise FileNotFoundError(unquote_path(path))
	for p in get_py_files(path):
		prepare_package(p)
		if deploy:
			deploy_package(p,aws_cli)
def main_args():
	'''
	ARGS
	'''
	parser = argparse.ArgumentParser()
	parser.add_argument('--aws_cli', action='store_true')
	parser.add_argument('-d','--deploy', action='store_true')
	parser.add_argument('-p', '--path', type=clean_path, default='')
	args = parser.parse_args()
	return vars(args)
if __name__ == "__main__":
	kwargs = main_args()
	print(kwargs)
	main(**kwargs)
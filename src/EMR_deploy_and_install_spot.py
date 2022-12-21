#!/usr/bin/env python3

import boto3 #sudo python3 -m pip install boto3
import time
import sys
# import botocore
import paramiko
import re
import os
import yaml
# from yaml import load, dump
import json
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper
from argparse import ArgumentParser
import logging
import logging.config

IAM_EMR_EC2_DEFAULTROLE = "EMR_EC2_DefaultRole"

iam = boto3.client('iam')

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('simpleExample')

parser = ArgumentParser()
parser.add_argument("-c", "--config", dest="config", required=True,
                    help="Config file", metavar="FILE")

args = parser.parse_args()
PATH = os.path.dirname(os.path.abspath(__file__))

config_file = os.path.join(PATH, args.config)
logging.info(f"config arg is {config_file}")
c = yaml.load(open(config_file), Loader)
config = c['config']

def default_iam_roles_exists():
	paginator = iam.get_paginator('list_roles')
	page_iterator = paginator.paginate()
	has_iam_role = False
	for page in page_iterator:
		role_names = [role['RoleName'] for role in page['Roles']]
		if IAM_EMR_EC2_DEFAULTROLE in role_names:
			has_iam_role = True
			break
	logging.info(f"IAM role {IAM_EMR_EC2_DEFAULTROLE} {'exists' if has_iam_role else 'does not exist'}")
	return has_iam_role

def ensure_default_iam_roles():
	""" Ensure that the EMR default roles exist, creating if necessary
	"""
	# No SDK action for create default roles?  Use CLI.
	if not default_iam_roles_exists():
		cmd = "aws emr create-default-roles"
		logging.info(f"Creating default role {IAM_EMR_EC2_DEFAULTROLE} with CLI command '{cmd}'")
		os.system(cmd)
		# Ensure role now exists
		if not default_iam_roles_exists():
			logging.error("Could not create default EMR role {IAM_EMR_EC2_DEFAULTROLE}: create in console. https://awscli.amazonaws.com/v2/documentation/api/latest/reference/emr/create-default-roles.html")
			exit(1)

ensure_default_iam_roles()

# Spot instances and different CORE/MASTER instances
# command=' \
# 	aws emr create-cluster \
# 	--applications Name=Hadoop Name=Spark \
# 	--tags \'project='+config['PROJECT_TAG']+'\' \'Owner='+config['OWNER_TAG']+'\' \'Name='+config['EC2_NAME_TAG']+'\' \
# 	--ec2-attributes \'{"KeyName":"'+config['KEY_NAME']+'","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"'+config['SUBNET_ID']+'","EmrManagedSlaveSecurityGroup":"'+config['WORKER_SECURITY_GROUP']+'","EmrManagedMasterSecurityGroup":"'+config['MASTER_SECURITY_GROUP']+'"}\' \
# 	--service-role EMR_DefaultRole \
# 	--release-label emr-5.23.0 \
# 	--log-uri \''+config['S3_URI']+'\' \
# 	--name \''+config['EMR_CLUSTER_NAME']+'\' \
# 	--instance-groups \'[{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":'+config['MASTER_HD_SIZE']+',"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"'+config['MASTER_INSTANCE_TYPE']+'","Name":"Master-Instance"},{"InstanceCount":'+config['WORKER_COUNT']+',"BidPrice":"'+config['WORKER_BID_PRICE']+'","EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":'+config['WORKER_HD_SIZE']+',"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"'+config['WORKER_INSTANCE_TYPE']+'","Name":"Core-Group"}]\' \
# 	--configurations \'[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true"}},{"Classification":"yarn-site","Properties":{"yarn.nodemanager.vmem-check-enabled":"false"},"Configurations":[]}]\' \
# 	--auto-scaling-role EMR_AutoScaling_DefaultRole \
# 	--ebs-root-volume-size 32 \
# 	--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
# 	--region '+config['REGION']+' \
# 	--bootstrap-actions Path="s3://hms-dbmi-docs/hail_bootstrap/bootstrap_python36.sh" \
# '

key_name = os.path.splitext(os.path.basename(config['KEY_NAME']))[0]
ec2_attributes = json.dumps(
	{
		"KeyName":  key_name,
		"InstanceProfile": 'EMR_EC2_DefaultRole', 
		"SubnetId": config['SUBNET_ID'],
		"EmrManagedSlaveSecurityGroup": config['WORKER_SECURITY_GROUP'],
		"EmrManagedMasterSecurityGroup": config['MASTER_SECURITY_GROUP']
	}
)

configurations = json.dumps(
	[
		{
			"Classification": "spark",
			"Properties": {
				"maximizeResourceAllocation": "true"
			}
		},
		{
			"Classification": "yarn-site",
			"Properties": {
				"yarn.nodemanager.vmem-check-enabled": "false"
			},
			"Configurations": []
		}
	]
)

instance_groups = json.dumps(
	[
		{
			"InstanceCount": 1,
			"EbsConfiguration": {
				"EbsBlockDeviceConfigs": [
					{
						"VolumeSpecification": {
							"SizeInGB": config['MASTER_HD_SIZE'],
							"VolumeType": "gp2"
						},
						"VolumesPerInstance": 1,
					}
				]
			},
			"InstanceGroupType": "MASTER",
			"InstanceType": config['MASTER_INSTANCE_TYPE'],
			"Name": "Master-Instance",
		},
		{
			"InstanceCount": config['WORKER_COUNT'],
			"BidPrice": config['WORKER_BID_PRICE'],
			"EbsConfiguration": {
				"EbsBlockDeviceConfigs": [
					{
						"VolumeSpecification": {
							"SizeInGB": config['WORKER_HD_SIZE'],
							"VolumeType": "gp2"
						},
						"VolumesPerInstance": 1,
					}
				]
			},
			"InstanceGroupType": "CORE",
			"InstanceType": config['WORKER_INSTANCE_TYPE'],
			"Name": "Core-Group",
		},
	]
)

command = f" \
aws emr create-cluster \
--release-label {config['EMR_RELEASE_LABEL']} \
--instance-groups '{instance_groups}' \
--log-uri {config['S3_URI']} \
--name {config['EMR_CLUSTER_NAME']} \
--configurations '{configurations}' \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--ebs-root-volume-size 32 \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region {config['REGION']} \
--bootstrap-actions Path='{config['BOOTSTRAP_ACTIONS']}' \
--applications Name=Hadoop Name=Spark \
--tags project={config['PROJECT_TAG']} Owner={config['OWNER_TAG']} Name={config['EC2_NAME_TAG']} \
--ec2-attributes '{ec2_attributes}' \
--service-role EMR_DefaultRole \
"

logging.info(f"command:\n{command}")
cluster_id_str = os.popen(command).read()
cluster_id = json.loads(cluster_id_str)['ClusterId']

# Gives EMR cluster information
emr = boto3.client('emr')
waiter = emr.get_waiter('cluster_running')
waiter.wait(
    ClusterId = cluster_id,
    WaiterConfig = {
        'Delay': 30,
        'MaxAttempts': 40,
    }
)

# # Cluster state update
# status_EMR='STARTING'
# tic = time.time()
# # Wait until the cluster is created
# while (status_EMR!='EMPTY'):
# 	print('Creating EMR...')
# 	details_EMR=client_EMR.describe_cluster(ClusterId=cluster_id)
# 	status_EMR=details_EMR.get('Cluster').get('Status').get('State')
# 	print('Cluster status: '+status_EMR)
# 	time.sleep(5)
# 	if (status_EMR=='WAITING'):
# 		print('Cluster successfully created! Starting HAIL installation...')
# 		toc=time.time()-tic
# 		print("\n Total time to provision your cluster: %.2f "%(toc/60)+" minutes")
# 		break
# 	if (status_EMR=='TERMINATED_WITH_ERRORS'):
# 		sys.exit("Cluster un-successfully created. Ending installation...")


# Get public DNS from master node
details_EMR = emr.describe_cluster(ClusterId=cluster_id)
master_dns = details_EMR.get('Cluster').get('MasterPublicDnsName')
master_IP = re.sub("-",".",master_dns.split(".")[0].split("ec2-")[1])
print(f"Master DNS: {master_dns}")
# print('Master IP: '+ master_IP+'\n')
print(f"ClusterId: {cluster_id}")

# Copy the key into the master
# command='scp -o \'StrictHostKeyChecking no\' -i '+config['PATH_TO_KEY']+config['KEY_NAME']+'.pem '+config['PATH_TO_KEY']+config['KEY_NAME']+'.pem hadoop@'+master_dns+':/home/hadoop/.ssh/id_rsa'
command = f" \
scp \
-o 'StrictHostKeyChecking no' \
-i {config['KEY_NAME']} \
{config['KEY_NAME']} \
hadoop@{master_dns}:/home/hadoop/.ssh/id_rsa \
"
os.system(command)

# Copy the installation script into the master
# command='scp -o \'StrictHostKeyChecking no\' -i '+config['PATH_TO_KEY']+config['KEY_NAME']+'.pem '+PATH+'/install_hail_and_python36.sh hadoop@'+master_dns+':/home/hadoop'
print('Copying keys...')
command = f" \
scp -o 'StrictHostKeyChecking no' \
-i {config['KEY_NAME']} \
{PATH}/install_hail_and_python36.sh \
hadoop@{master_dns}:/home/hadoop \
"
os.system(command)

print('Installing software...')
print('Allow 4-8 minutes for full installation')
print('\n This is your Jupyter Lab link: '+ master_IP+':8192\n')
try:
	key = paramiko.RSAKey.from_private_key_file(key_name)
except Exception:
	print(f"Oh no not RSA!")
	pass
	try:
		key = paramiko.ed25519key.Ed25519Key.from_private_key_file(key_name)
	except Exception:
		print(f"Oh no not ed25519!")
		pass

client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(hostname=master_IP, username="hadoop", pkey=key)

# Execute a command(cmd) after connecting/ssh to an instance
HAIL_VERSION = config['HAIL_VERSION']
command = f'./install_hail_and_python36.sh -v {HAIL_VERSION}'
os.system('cd /home/hadoop/')
os.system(command)

# close the client connection
client.close()

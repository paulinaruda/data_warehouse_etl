#!/usr/bin/env python
# coding: utf-8


# imports 
import boto3
import configparser
import matplotlib.pyplot as plt
import pandas as pd
from time import time
from botocore.exceptions import ClientError
import json
import psycopg2
import time
pd.set_option('display.float_format', '{:.2f}'.format)

# Checking credentials
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get('CLUSTER', 'db_cluster_type')
DWH_NODE_TYPE          = config.get('CLUSTER', 'db_node_type')
DWH_NUM_NODES          = config.get('CLUSTER', 'db_num_nodes')

DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER","db_identifier")
DWH_DB                 = config.get("CLUSTER","db_name")
DWH_DB_USER            = config.get("CLUSTER","db_user")
DWH_DB_PASSWORD        = config.get("CLUSTER","db_password")
DWH_PORT               = config.get("CLUSTER","db_port")

DWH_IAM_ROLE_NAME      = config.get("IAM_ROLE", "IAM_ROLE_NAME")


# ## Creating the IAM role for the cluster creation
# Creating an IAM Role that allows Redshift to access S3 bucket

# Initializing client for the IAM service 
iam = boto3.client('iam',aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name='us-west-2'
                  )

# Creating the role for Redshift to access AWS services on my behalf
try:
    print("Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )
    
except Exception as e:
    print(e)

# Attaching policies 
print("Attaching Policy")
# Allowing Redshift for S3 read access
iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']
print("Getting the IAM role ARN")
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

# Allowing for full Redshift access
iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonRedshiftFullAccess"
                      )['ResponseMetadata']['HTTPStatusCode']
print(roleArn)

# writing the correct ARN role into the cfg file
def write_arn_value(role_Arn):
    with open('dwh.cfg', 'r') as file:
        lines = file.readlines()

    with open('dwh.cfg', 'w') as file:
        for line in lines:
            if 'ARN' in line:
                line = line.replace(config.get("IAM_ROLE", "ARN"), role_Arn)
            file.write(line)
write_arn_value(roleArn)


# ## Starting the Redshift cluster
redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

# creating cluster
try:
    response = redshift.create_cluster(        
        #DHW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]
    )
    print("Cluster created")
except Exception as e:
    print("Error creating cluster")
    print(e)

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
attempts = 0

# checking the cluster status
while myClusterProps['ClusterStatus'] != 'available':
    attempts += 1
    print("Cluster is not running yet, next attempt to see cluster availability in 25 seconds, number of attempts:", attempts)
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    if myClusterProps['ClusterStatus'] == 'available':
        break
    else:
        time.sleep(30)  # Sleep for 30 seconds before trying again

if myClusterProps['ClusterStatus'] == 'available':
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
    print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)


# writing the new host value to the file 
def write_host_value(host_value):
    with open('dwh.cfg', 'r') as file:
        lines = file.readlines()

    with open('dwh.cfg', 'w') as file:
        for line in lines:
            if 'HOST' in line:
                line = line.replace(config.get("CLUSTER","HOST"), host_value)
            file.write(line)

# before running this cell check if the HOST value in the dwh.cfg is xx or other value
write_host_value(DWH_ENDPOINT)

ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
    print("TCP port open.")
except Exception as e:
    print(e)

# connecting to the database
conn = psycopg2.connect(
    "host={} dbname={} user={} password={} port={}".format(
        DWH_ENDPOINT,
        config.get('CLUSTER', 'DB_NAME'),
        config.get('CLUSTER', 'DB_USER'),
        config.get('CLUSTER', 'DB_PASSWORD'),
        config.get('CLUSTER', 'DB_PORT')
    ) )

# Create a cursor object to execute SQL queries
cur = conn.cursor()

# Checking if the connection was successful
if conn.status == psycopg2.extensions.STATUS_READY:
    print("Connected successfully!")
else:
    print("Connection failed.")

# running script creating tables
get_ipython().run_line_magic('run', 'create_tables.py')

# running script doing the ETL
get_ipython().run_line_magic('run', 'etl.py')

# deleting the cluster
redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonRedshiftFullAccess")
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)

# Close the cursor and connection
cur.close()
conn.close()
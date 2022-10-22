""" 
Exercise 2: Creating Redshift Cluster using AWS python SDK
 """
import pandas as pd
import boto3
import json
#  STEP0: Make sure you have an AWS secret and access key
    # Create a new IAM user in your AWS account
    # Give it AdministratorAccess, From Attach existing policies directly Tab
    # Give it AdministratorAccess, From Attach existing policies directly Tab
    # Edit the file dwh.cfg in the same folder as this notebook and fill
    """
    [AWS]
    KEY= YOUR_AWS_KEY
    SECRET= YOUR_AWS_SECRET 
    """

# Load DWH Params from a file
import configparser
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })

# Creat clients for EC2, S3, IAM and Redshift
import boto3

ec2 = 

s3 = 

iam = 

redshift =

# Checkout the sample data sources on S3
sampleDbBucket =  s3.Bucket("awssampledbuswest2")
# TODO: Iterate over bucket objects starting with "ssbgz" and print

# STEP1: IAM ROLE

# Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
# TODO: Create the IAM role
try:
    print('1.1 Creating a new IAM Role')
    dwhRole =
except Exception as e:
    print(e)

# TODO: Attach Policy
print('1.2 Attaching Policy')

# TODO: Get and print the IAM role ARN
print('1.3 Get the IAM role ARN')
roleArn = 

print(roleArn)

# STEP 2: REDSHIFT Cluster
# Create a RedShift Cluster
# For complete arguments to create_cluster, see docs
try:
    response = redshift.create_cluster(
        # TODO: add parameters for hardware
        # TODO: add parameters for identifiers & credentials
        # TODO: add parameter for role (to allow s3 access)
    )
except Exception as e:
    print(e)

# Describe the cluster to see its status
# run the below block until the cluster status becomes available
def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)

# Take note of the cluster endpoint and role ARN
# Do not run this unless the cluster status becomes "Available"
DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
print("DWH_ENDPOINT :: ", endpoint)
print("DWH_ROLE_ARN :: ", roleArn)

# STEP 3: Open an incoming TCP port to access the cluster endpoint
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName= ,  # TODO: fill out
        CidrIp='',  # TODO: fill out
        IpProtocol='',  # TODO: fill out
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)
# STEP4: Make sure you can connect to the cluster Connect to the Cluster
%load_ext sql

conn_string="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
print(conn_string)
%sql $conn_string

# STEP5: Clean up your resourse
#### CAREFUL!!
#-- Uncomment & run to delete the created resources
redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
#### CAREFUL!!

myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshiftProps(myClusterProps)

#### CAREFUL!!
#-- Uncomment & run to delete the created resources
iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
#### CAREFUL!!
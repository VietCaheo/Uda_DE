""" Lession Overview
-> Distinguish between setting up  a Spark Cluster using both Local and Standalone Mode
-> Set up Spark Cluster in AWS
-> Use Spark UI
-> Use AWS CLI
-> Creat EMR using AWS CLI
-> Creat EMR Cluster
-> Test port Forwardidng
-> Use Notebooks on your Spark Cluster
-> Write Spark Scripts
-> Store and Retrieve Data on the Cloud
-> Read and Write to Amazon S3
-> Distinct betweeen HDFS and S3
-> Reading and Writing Data to HDFS
 """

# 1. From Local to StandAlone Mode
""" -> Amazon S3 will store the dataset
    -> Rent a cluster of machines, our `Spark Cluster` (located in AWS Datacenter) -> using EC2 (Computing Cloud)
    -> Login from local computer to this Spark Cluster
    -> Upon running Spark Code: the cluster will load the dataset from Amazon S3 into cluster's memory distributed across each machine in the cluster 
    
"""

# 3. USing Spark on AWS
""" 
There are 02 choices:
    1. Use AWS EC2 and install and configure Spark and HDFS (Hadoop Distributed File System) yourself
    2. Use AWS EMR,which is a scalable set of EC2 machines that are already configured to run Spark.

HDFS: Hadoop and Spark are two frameworks provide tools for carry out big-data tasks. Spark is faster, but it lacks a distributed storage system.

MapReduce System (EMR): to store data back on hard drives after complete all the tasks.

EMR Cluster:
    Available region is : us-east-1 ), us-east-2, us-west-1 or us-west-2.
    [!]. We recommend you shut down/delete every AWS resource (e.g., EC2, Sagemaker, Database, EMR, CloudFormation) immediately after the usage or if you are stepping away for a few hours
 """

#  AWS - Install and configured CLI v2:
""" 
For creat or interract with an EMR Cluster using commands.
To do that, finish 3 steps:
    -> Install AWS CLI
    -> Creat an IAM user with Administrator permissions
    -> Configure the AWS CLI.

refer link: 
aws commands: https://docs.aws.amazon.com/cli/latest/reference/

Configure the AWS CLI:
    - Access key: is a combination of an Access Key ID and Secret Access Key => `Access Key`: using by Udacity Gateway, it has alreay provided
    - Default AWS Region: specifies the AWS Region where you want to send.
    - Default output format: how results are formatted (can be: json, yaml, text, or a table)
    - Profile: have the default name is `default`, it can be creat a new profile using the command: 
            aws configure --profile new_name
    - Session Token : if use Udacity Gateway and account: need to add the session token from the AWS Gateway credential pop-up, as well as the Access key and SecretKey.

 """

# For setting local aws-profile 
 
Set default profile credentials:
# Navigate to the home directory
cd ~
# If you do not use the profile-name, a default profile will be created for you.
aws configure --profile <profile-name>
# View the current configuration
aws configure list --profile <profile-name>
# View all existing profile names
aws configure list-profiles
# In case, you want to change the region in a given profile
# aws configure set <parameter> <value>  --profile <profile-name>
aws configure set region us-east-1  --profile <profile-name> 


#  Let the system know that your sensitive information is residing in the .aws folder

export AWS_CONFIG_FILE=~/.aws/config
export AWS_SHARED_CREDENTIALS_FILE=~/.aws/credentials

# Setting the Session Token

# Using Git-Bash for set windows users:
setx AWS_ACCESS_KEY_ID 
setx AWS_SECRET_ACCESS_KEY
setx AWS_DEFAULT_REGION

# (For Udacity AWS Gateway Users)
setx AWS_SESSION_TOKEN


#  Update the specific variable in the configuration:
# Syntax
# aws configure set <varname> <value> [--profile profile-name]
 aws configure set default.region us-east-2

 













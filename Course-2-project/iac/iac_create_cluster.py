import pandas as pd
import boto3
import json
import configparser
import time

#Load config
config_iac = configparser.ConfigParser()
config_iac.read_file(open('iac.cfg'))

KEY                    = config_iac.get('AWS','KEY')
SECRET                 = config_iac.get('AWS','SECRET')
REGION                 = config_iac.get('AWS','REGION')

DWH_CLUSTER_TYPE       = config_iac.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config_iac.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config_iac.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config_iac.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config_iac.get("DWH","DWH_DB")
DWH_DB_USER            = config_iac.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config_iac.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config_iac.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config_iac.get("DWH", "DWH_IAM_ROLE_NAME")

print(f'REGION:{REGION}')
print(f'KEY:{KEY}')
print(f'SECRET:{SECRET}')


#Create IAM client
iam = boto3.client('iam',
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET,
                   region_name='us-west-2'
                  )

#Create the role, 
try:
    print("1.1 Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    str_e = str(e)
    if str_e == 'An error occurred (EntityAlreadyExists) when calling the CreateRole operation: Role with name dwhRole already exists.':
        print(f'This IAM Role {DWH_IAM_ROLE_NAME} already exists')
    else:        
        print(e)
        raise e
    
    

#Attach S3 reading policy
print('1.2 Attaching Policy')
iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonRedshiftQueryEditor"
                      )['ResponseMetadata']['HTTPStatusCode']

iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonRedshiftReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

 

print('1.3 Get the IAM role ARN')
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

print(roleArn)


#Create Redshift client
redshift = boto3.client('redshift',
                       region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

def check_cluster_ready(redshift, cluster_identifier):
    '''
    Check wheter Redshift cluster is ready
    '''
    redshift.describe_clusters()['Clusters'][0]
    redshift_list = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)            
    if len(redshift_list['Clusters'])==0:
        return False
    else:                
        if redshift_list['Clusters'][0]['ClusterStatus']=='available':
            return True
        else:
            return False
    
#Create Redshift instance
print('Creating Redshift instance...')
redshift_cluster_ready = False
try:
    response = redshift.create_cluster(        
        #HW
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

    while not redshift_cluster_ready:
        time.sleep(5)
        redshift_cluster_ready=check_cluster_ready(redshift, DWH_CLUSTER_IDENTIFIER)
    
except Exception as e:
    str_e = str(e)
    if str_e == 'An error occurred (ClusterAlreadyExists) when calling the CreateCluster operation: Cluster already exists':
        print('Cluster already exists.')               
        pass
    else:
        print(f'Unknown error ! - {e}')
        raise e

#Get endpoint        
redshift_cluster_ready=check_cluster_ready(redshift, DWH_CLUSTER_IDENTIFIER)
while not redshift_cluster_ready:
    time.sleep(5)
    redshift_cluster_ready=check_cluster_ready(redshift, DWH_CLUSTER_IDENTIFIER)

redshift_list = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER) 
dict_cluster_info = redshift_list['Clusters'][0]            
print(dict_cluster_info)
endpoint = dict_cluster_info['Endpoint']['Address']
print(f'Redshift Endpoint = {endpoint}')


#Save config into dwh.config
config_dwh = configparser.ConfigParser()
config_dwh.read_file(open('dwh.cfg'))
config_dwh.set("CLUSTER", "host", endpoint)
config_dwh.set("CLUSTER", "db_name", DWH_DB)
config_dwh.set("CLUSTER", "db_user", DWH_DB_USER)
config_dwh.set("CLUSTER", "db_password", DWH_DB_PASSWORD)
config_dwh.set("CLUSTER", "db_port", DWH_PORT)
config_dwh.set("IAM_ROLE", "arn", roleArn)
config_dwh.set("IAM_ROLE", "key", KEY)
config_dwh.set("IAM_ROLE", "secret", SECRET)
config_dwh.set("IAM_ROLE", "region", REGION)
config_dwh.write(open('dwh.cfg', 'w'))
print('dwh.cfg is updated')





#Setup network for incoming TCP
ec2 = boto3.resource('ec2',
                       region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET)

try:
    vpc = ec2.Vpc(id=dict_cluster_info['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    
    defaultSg.authorize_ingress(
        GroupName= defaultSg.group_name,  # TODO: fill out
        CidrIp='0.0.0.0/0',  # TODO: fill out
        IpProtocol='TCP',  # TODO: fill out
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)

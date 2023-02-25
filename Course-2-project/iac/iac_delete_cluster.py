import pandas as pd
import boto3
import json
import configparser

#Load config
config = configparser.ConfigParser()
config.read_file(open('iac.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')
REGION                 = config.get('AWS','REGION')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

print(f'REGION:{REGION}')
print(f'KEY:{KEY}')
print(f'SECRET:{SECRET}')


#Create Redshift client
redshift = boto3.client('redshift',
#                        region_name="us-east-1",
                       region_name=REGION,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

redshift_list = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)

if len(redshift_list['Clusters']) > 0:
    print(f'Found existing redshift cluster {DWH_CLUSTER_IDENTIFIER}')
    print(redshift_list['Clusters'][0])          
    print(f'Deleting {DWH_CLUSTER_IDENTIFIER}')
    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    print(f'{DWH_CLUSTER_IDENTIFIER} has been deleted from {REGION}')
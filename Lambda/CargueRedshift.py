import json
import urllib.parse
import boto3
import botocore.session as bc

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler(event, context):
    print("Entered lambda_handler")

    secret_name='redshift' ## HERE add the secret name created.
    session = boto3.session.Session()
    region = session.region_name
    
    client = session.client(
            service_name='secretsmanager',
            region_name=region
        )
    
    get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    secret_arn=get_secret_value_response['ARN']
    
    secret = get_secret_value_response['SecretString']
    
    secret_json = json.loads(secret)
    
    cluster_id=secret_json['dbClusterIdentifier']
    
    
    bc_session = bc.get_session()
    
    session = boto3.Session(
            botocore_session=bc_session,
            region_name=region,
        )
    
    # Setup the client
    client_redshift = session.client("redshift-data")
    print("Data API client successfully loaded")
    
    truncate =event['truncate']
    consulta_sql = event['consulta_sql']
    sp = event['sp']
                      
    print(consulta_sql)
    
    res = client_redshift.execute_statement(Database= 'prod_datalake', SecretArn= secret_arn, Sql= truncate, ClusterIdentifier= cluster_id)
    res = client_redshift.execute_statement(Database= 'prod_datalake', SecretArn= secret_arn, Sql= consulta_sql, ClusterIdentifier= cluster_id)
    res = client_redshift.execute_statement(Database= 'prod_datalake', SecretArn= secret_arn, Sql= sp, ClusterIdentifier= cluster_id)
    #id=res["Id"]

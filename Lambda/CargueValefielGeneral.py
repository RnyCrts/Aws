import json
import boto3
import pyodbc
import pandas as pd
import pytz
from os import getenv
from subprocess import call
from datetime import datetime, timedelta

def lambda_handler(event, context):
    tz = pytz.timezone('America/Bogota')
    call('rm -rf /tmp/*', shell=True)
    dateZero     = (datetime.now(tz))
    server = getenv("MSSQL_SERVER")
    user = getenv("MSSQL_USERNAME")
    password = getenv("MSSQL_PASSWORD")
    database = getenv("MSSQL_DATABASE")
    consulta_sql = event['consulta_sql'] #'Select top 20 * from Transacciones with(noLock) where year(Fecha)=2020;;'
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+user+';PWD='+ password)
    cursor = conn.cursor()
    bucket_destino = "prod-valemas-datalake-raw"
    tabla = event['tabla']  #'Transacciones_prod'
    filtro = event['filtro'] #'1'
    print(consulta_sql)
    
    consulta_sql = consulta_sql.replace('@fecha@', "'"+str((datetime.now(tz)-timedelta(days=1)).strftime('%Y-%m-%d'))+"'")
    filtro = filtro.replace('@fecha@', str((datetime.now(tz)-timedelta(days=1)).strftime('%Y-%m-%d')))
    df_query = pd.read_sql_query(consulta_sql,conn)
    df_query = df_query.replace(r'(^\s+|\s+$)',' ', regex=True) 
    df_query = df_query.replace(r'\s+',' ', regex=True) 
    df_query = df_query.fillna('')
    

    cargar_archivo_csv(df_query, tabla, tz, bucket_destino, filtro)
    
    df_index = str(len(df_query.index))
    
    if filtro == '':
        insert_data(database, server, tabla, tz, df_index, bucket_destino)
    else:
        insert_data(database, server, tabla+'_'+filtro, tz, df_index, bucket_destino)
    conn.close()



# funcion para cargar archivo csv a S3
def cargar_archivo_csv(df, tabla, tz, bucket_destino, filtro):
    fecha_ayer = str((datetime.now(tz)-timedelta(days=1)).strftime('%Y%m%d'))
    if filtro == '':
        file_name_csv = 'BD_VALEFIEL_'+fecha_ayer+'.csv'
    else:
        file_name_csv = 'BD_VALEFIEL_'+fecha_ayer+'_'+filtro+'.csv'

    tmp_file_csv = '/tmp/'+file_name_csv
    
    final_file_csv = 'valefiel/'+tabla+'/fecha_particion='+fecha_ayer+'/'+file_name_csv
    
    df.to_csv(tmp_file_csv, index = False)
    s3_resource = boto3.resource("s3")
    s3_resource.Bucket(bucket_destino).upload_file(tmp_file_csv, final_file_csv)

    

# funcion de insertar datos de bitacora en dynamodb
def insert_data(database, server, tabla, tz, df_index, bucket_destino):
    record = {}
    record['id'] = {'S':database+'_'+tabla+'_'+str(datetime.now(tz).strftime('%Y-%m-%d'))}
    record['Archivo'] = {'S':database}
    record['Fuente'] = {'S':server}
    record['Bucket_almacenado'] = {'S':bucket_destino}
    record['Registros'] = {'N': df_index}
    record['Tabla'] = {'S':tabla}
    record['Fecha_archivo'] = {'S' : str(datetime.now(tz).strftime('%Y%m%d'))}
    record['Fecha_cargue'] = {'S': str(datetime.now(tz).strftime('%Y-%m-%d'))}
    
    dynamodb = boto3.client('dynamodb')
    dynamodb.put_item(TableName='datalake_cargue_bitacora',
        Item=record
    )

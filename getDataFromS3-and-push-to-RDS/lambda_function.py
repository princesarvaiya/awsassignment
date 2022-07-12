import sys
import logging
import rds_config
import pymysql
import boto3
import json

rds_host  = "assignment.cxinhiyuiyer.us-east-1.rds.amazonaws.com"
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name


logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    connection = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

s3_client = boto3.client('s3')
def lambda_handler(event, context):
    # TODO implement
    bucket = event['Records'][0]['s3']['bucket']['name']
    file = event['Records'][0]['s3']['object']['key']
    file_obj = s3_client.get_object(Bucket=bucket, Key=file)
    jsonFileReader = file_obj['Body'].read()
    jsonDict = json.loads(jsonFileReader)
    
    TABLE_NAME = 'productvisits'
    sqlstatement = ''
   
    print(jsonDict)
    keylist=[]
    valuelist=[]
    for k in jsonDict:
        keylist.append(k)
        valuelist.append(jsonDict[k])
    
    colname = "("
    valname = "("
    
    for every_column in keylist:
        colname = colname + every_column + ', '
    
    colname = colname[:len(colname)-2]
    colname = colname + ')'
    
    for every_val in valuelist:
        valname = valname + '"' + every_val + '", '
        
    valname = valname[:len(valname)-2]
    valname = valname + ")"
    
   
    sqlstatement += 'INSERT INTO ' + TABLE_NAME + ' ' + colname  + ' VALUES ' + valname

    cursor = connection.cursor()
    cursor.execute(sqlstatement)
    connection.commit()

    connection.commit()
    print (cursor.rowcount,'Record inserted successfully into productvisits table')
    

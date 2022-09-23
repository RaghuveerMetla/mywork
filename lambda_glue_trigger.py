import logging
import os
import boto3
import traceback
import urllib.parse
from pathlib import Path 

logger = logging.getLogger()
logger.setLevel(logging.INFO)



def lambda_handler(event, context):
    try:
        print("event: " + str(event))
        
        #Extract relevant metadata from input s3 event 
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        file_name = Path(key).name
        
        #response = s3.get_object(Bucket=bucket, Key=key)
        logger.info("***bucket: " + str(bucket_name))
        logger.info("***key: " + str(key))
        logger.info("***file_name: " + str(file_name))
        
        #define variables
        output_directory = Path(key).parts[-2]
        print("**output_directory : " + str(output_directory))
        output_path = "s3://rio-ops-data/output_directory/"
        print("**output_path : " + str(output_path) + str(output_directory))
        
        #Trigger Logic
        s3_client = boto3.client('s3')
        glue_client = boto3.client('glue')
        
        if file_name == 'RunInfo.xml':
            print("**Invoking XML Parser")
            response = glue_client.start_job_run(JobName = "XML_Parser", Arguments = {"--bucket_name":bucket_name, "--file_name":file_name, "--output_path":output_path})
        elif file_name == 'Sample_sheet.csv':
            print("**Invoking CSV Parser")
            response = glue_client.start_job_run(JobName = "CSV_Parser", Arguments = {"--bucket_name":bucket_name, "--file_name":file_name, "--output_path":output_path})
        elif file_name == 'RTAComplete.txt':
            print("**Invoking TXT Parser")
            response = glue_client.start_job_run(JobName = "TXT_Parser", Arguments = {"--bucket_name":bucket_name, "--file_name":file_name, "--output_path":output_path})
        else:
            print("New Object is out of scope. Please verify!")

    except Exception as err:
        logger.error("Error occurred while handling the event, ExceptionTrace=%s" % err)
        traceback.print_exc()
        
       

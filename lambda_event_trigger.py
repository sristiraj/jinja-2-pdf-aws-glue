# Set up logging
import json
import os
import logging
import boto3
import csv


EVENT_FILE_CSV_DELIMIITER = "|"
EVENT_FILE_CSV_QUOTE_CHAR = '"'
#Initiate loggger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Import Boto 3 for AWS Glue
client = boto3.client('glue')

# Variables for the job: 
glueJobName = "generate_PDF"

def read_s3(s3_file):
    s3 = boto3.client("s3")
    s3_bucket_index = s3_file.replace("s3://","").find("/")
    s3_bucket = s3_file[5:s3_bucket_index+5]
    s3_key = s3_file[s3_bucket_index+6:]
    obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)            
    data = obj["Body"].read().decode('utf-8') 
    return data  

def read_s3_event_file(bucket_name, object_key):
    #Read s3 event file data in string buffer
    data = read_s3(f"s3://{bucket_name}/{object_key}".format())
    #Split lines for multiple lines in csv file
    data_lines = data.splitlines()
    #Read csv data columns separately from string buffer
    csv_data = csv.reader(data_lines, delimiter=EVENT_FILE_CSV_DELIMIITER, quotechar=EVENT_FILE_CSV_QUOTE_CHAR)
    csv_event = list(csv_data)[1]
    if csv_event is not None:
        event_record = csv_event
    else:
        print("No event record available ")
        event_record = []
    return event_record

# Define Lambda function
def lambda_handler(event, context):
    logger.info('## INITIATED BY EVENT: ')
    logger.info("Read Event Source")
    eventSource = event.get("Records",[{"eventSource":"unknown"}])[0].get("eventSource","unknown")
    
    if eventSource=="aws:s3":
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        object_key =  event["Records"][0]["s3"]["object"]["key"]
        event_record = read_s3_event_file(bucket_name, object_key)   
        print(event_record)
        logger.info(f"Event Record: {event_record}".format())
        logger.info(f"Start Glue Job {glueJobName}".format())
        response = client.start_job_run(JobName = glueJobName, Arguments={"--PART_SSN":event_record[1], "--POST_DATE_START":event_record[2], "--POST_DATE_END":event_record[3]})
        logger.info('## STARTED GLUE JOB: ' + glueJobName)
        logger.info('## GLUE JOB RUN ID: ' + response['JobRunId'])
    return "200"

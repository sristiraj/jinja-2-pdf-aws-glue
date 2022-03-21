from jinja2 import Template
import pandas as pd 
import sys
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import *
import pyspark.sql.functions as sf
import json
from xhtml2pdf import pisa
from io import StringIO
import boto3
from datetime import datetime


run_dt = datetime.strftime(datetime.now(),'%d/%m/%y')
run_time = datetime.strftime(datetime.now(),'%H:%M:%S')
run_year = datetime.strftime(datetime.now(),'%Y')
run_month = datetime.strftime(datetime.now(),'%m')
run_day = datetime.strftime(datetime.now(),'%d')
#For glue invocation
AWS_REGION = "us-east-1"
args = getResolvedOptions(sys.argv, ["TEMPLATE_PATH","OUTPUT_PDF_PATH","PART_SSN","GLUE_CONN_NAME","TMP_DIR_PATH","HEADER_TABLE","DETAIL_TABLE","POST_DATE_START","POST_DATE_END","INTERMEDIATE_S3_PATH","TRIGGER_FILE","TRIGGER_ARCHIVE_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

temp_pdf = "Report_{}.pdf".format(args["PART_SSN"].replace(" ","_"))
print(temp_pdf)
temp_html = "Report_{}.html".format(args["PART_SSN"].replace(" ","_"))

def read_template(path):
    '''
    This function is used to read template file using the path provided as parameter
    and pass the template data back
    '''
    if path.startswith("s3://"):
        import boto3
        s3 = boto3.resource("s3")
        s3_bucket_index = path.replace("s3://","").find("/")
        s3_bucket = path[5:s3_bucket_index+5]
        s3_key = path[s3_bucket_index+6:]
        obj = s3.Object(s3_bucket, s3_key)
        template_data = obj.get()['Body'].read().decode('utf-8') 
    elif path.startswith("file://"):
        with open(path.replace("file://","")) as f:
            template_data = f.read()
    return template_data

def get_report_data(glue_conn_name, tmp_dir_path):
    #Get glue connection details
    import boto3
    client = boto3.client('glue', region_name=AWS_REGION)
    response = client.get_connection(Name=glue_conn_name)
    connection_properties = response['Connection']['ConnectionProperties']
    URL = connection_properties['JDBC_CONNECTION_URL']
    url_list = URL.split("/")

    host = "{}".format(url_list[-2][:-5])
    port = url_list[-2][-4:]
    database = "{}".format(url_list[-1])
    user = "{}".format(connection_properties['USERNAME'])
    pwd = "{}".format(connection_properties['PASSWORD'])
    #Set redshift query params
    connection_redshift_options = {"url": f"jdbc:redshift://{host}:{port}/{database}".format(), "user": user, "password": pwd, "redshiftTmpDir":  tmp_dir_path} 
    # df_header = spark.read.format("csv").option("header","true").load(input_data_path+"/header").fillna("")
    # df_detail = spark.read.format("csv").option("header","true").load(input_data_path+"/detail").fillna("")
    connection_redshift_options["query"] = "select * from {} where SSN='{}'".format(args["HEADER_TABLE"],args["PART_SSN"])
    df_header = glueContext.create_dynamic_frame_from_options(connection_type="redshift", connection_options=connection_redshift_options).toDF()

    df_header_cols = [colm.upper() for colm in df_header.columns]
    df_header = df_header.toDF(*df_header_cols).distinct().fillna(" ")
    connection_redshift_options["query"] = "select  part_ssn, post_date, activity, fund, ae, rv, sum(employee) employee, sum(automatic) automatic, sum(matching) matching, sum(row_total) row_total from {} where PART_SSN='{}' and post_date between to_date('{}','YYYY-MM-DD') and to_date('{}','YYYY-MM-DD') group by part_ssn, post_date, activity, fund".format(args["DETAIL_TABLE"],args["PART_SSN"], args["POST_DATE_START"], args["POST_DATE_END"])
    df_detail = glueContext.create_dynamic_frame_from_options(connection_type="redshift", connection_options=connection_redshift_options).toDF()
    df_detail_cols = [colm.upper() for colm in df_detail.columns]
    df_detail = df_detail.toDF(*df_detail_cols).fillna(" ").fillna(0)
    df_detail_summed = df_detail.groupBy("PART_SSN","ACTIVITY","POST_DATE").agg(round(sum("EMPLOYEE"),2).alias("EMPLOYEE"),round(sum("AUTOMATIC"),2).alias("AUTOMATIC"),round(sum("MATCHING"),2).alias("MATCHING"),round(sum("ROW_TOTAL"),2).alias("ROW_TOTAL")).withColumn("FUND",lit(None)).withColumn("AE",lit(".")).withColumn("RV",lit(".")).fillna(" ").fillna(0)
    df_detail1 = df_detail.unionByName(df_detail_summed).orderBy("POST_DATE","ACTIVITY","FUND")
    #Find aggregate value for the ssn passed to show as last row in report
    df_fund_summed = df_detail.groupBy("FUND").agg(round(sum("EMPLOYEE"),2).alias("EMPLOYEE_FUND_SUM"),round(sum("AUTOMATIC"),2).alias("AUTOMATIC_FUND_SUM"),round(sum("MATCHING"),2).alias("MATCHING_FUND_SUM"),round(sum("ROW_TOTAL"),2).alias("ROW_FUND_TOTAL_SUM")).withColumnRenamed("FUND","FUND_SUM").fillna(" ").fillna(0)
    df_summed = df_detail.groupBy("PART_SSN").agg(round(sum("EMPLOYEE"),2).alias("EMPLOYEE_SUM"),round(sum("AUTOMATIC"),2).alias("AUTOMATIC_SUM"),round(sum("MATCHING"),2).alias("MATCHING_SUM"),round(sum("ROW_TOTAL"),2).alias("ROW_TOTAL_SUM")).fillna(" ").fillna(0)
    
    #Convert to dict to be passed to Jinja
    list_data_header = list(map(lambda row: row.asDict(), df_header.collect()))
    list_data_detail = sorted(list(map(lambda row: row.asDict(), df_detail1.collect())),key=lambda x: x['POST_DATE']+"~"+x["ACTIVITY"]+"~"+x["FUND"])
    list_data_fund_summed = list(map(lambda row: row.asDict(), df_fund_summed.collect()))
    list_data_summed = list(map(lambda row: row.asDict(), df_summed.collect()))
    
    print(list_data_detail)
    #Pass three dict generated to Jinja
    list_data = [list_data_header, list_data_detail, list_data_fund_summed, list_data_summed, run_dt, run_time]
    return list_data
    
def convertHtmlToPdf(sourceHtml, outputFilename):
    resultFile = open(outputFilename, "w+b")
    pisaStatus = pisa.CreatePDF(sourceHtml,resultFile)
    resultFile.close()
    return pisaStatus.err
    
def write_pdf_s3(filename, path):
    import boto3
    s3 = boto3.resource("s3")
    s3_bucket_index = path.replace("s3://","").find("/")
    s3_bucket = path[5:s3_bucket_index+5]
    s3_key = path[s3_bucket_index+6:].strip("/")+"/year="+run_year+"/month="+run_month+"/day="+run_day+"/"
    obj = s3.Object(s3_bucket, s3_key+filename)
    s3 = boto3.client('s3')
    with open(temp_pdf, "rb") as f:
        s3.upload_fileobj(f, s3_bucket, s3_key+"/"+filename)

def write_html_s3(filename, path):
    import boto3
    s3 = boto3.resource("s3")
    s3_bucket_index = path.replace("s3://","").find("/")
    s3_bucket = path[5:s3_bucket_index+5]
    s3_key = path[s3_bucket_index+6:].strip("/")+"/year="+run_year+"/month="+run_month+"/day="+run_day+"/"
    obj = s3.Object(s3_bucket, s3_key+filename)
    s3 = boto3.client('s3')
    with open(filename, "rb") as f:

        s3.upload_fileobj(f, s3_bucket, s3_key+"/"+filename)

def move_trigger_file(trigger_file, trigger_arch_path):
    import boto3
    s3 = boto3.resource("s3")
    s3_bucket_index = trigger_file.replace("s3://","").find("/")
    s3_bucket = trigger_file[5:s3_bucket_index+5]
    s3_key = trigger_file[s3_bucket_index+6:].strip("/")
    trigger_file_name = trigger_file[trigger_file.rfind("/")+1:]
    print(trigger_file_name)
    
    s3_bucket_index = trigger_arch_path.replace("s3://","").find("/")
    s3_arch_bucket = trigger_arch_path[5:s3_bucket_index+5]
    s3_arch_key = trigger_arch_path[s3_bucket_index+6:].strip("/")
    
    copy_source = {
    'Bucket': s3_bucket,
    'Key': s3_key
    }
    
    destbucket = s3.Bucket(s3_arch_bucket)
    destbucket.copy(copy_source, trigger_file_name)
    s3.Object(s3_bucket,s3_key).delete()
    
template_data = read_template(args["TEMPLATE_PATH"])
template = Template(template_data)

list_data = get_report_data(args["GLUE_CONN_NAME"],args["TMP_DIR_PATH"])
# print(type(list_data))
template_out = template.render(list_data=list_data, report_run_dt=run_dt, report_run_time=run_time)


with open(temp_html,"w+") as f:
    f.write(template_out)
# pdf=pisa.CreatePDF( StringIO(template_out),open("output.pdf", "wb"))   
convertHtmlToPdf(template_out, temp_pdf)
write_pdf_s3(temp_pdf,args["OUTPUT_PDF_PATH"])
write_html_s3(temp_html,args["OUTPUT_PDF_PATH"])
move_trigger_file(args["TRIGGER_FILE"],args["TRIGGER_ARCHIVE_PATH"])

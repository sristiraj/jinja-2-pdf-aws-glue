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



#For glue invocation
AWS_REGION = "us-gov-west-2"
args = getResolvedOptions(sys.argv, ["TEMPLATE_PATH","OUTPUT_PDF_PATH","PART_SSN","GLUE_CONN_NAME","TMP_DIR_PATH","HEADER_TABLE","DETAIL_TABLE"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

temp_pdf = "Report_{}.pdf".format(args["PART_SSN"].replace(" ","_"))
print(temp_pdf)




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
    connection_redshift_options = {"url": URL, "database":database, "user": user, "password": pwd, "redshiftTmpDir": tmp_dir_path} 
    # df_header = spark.read.format("csv").option("header","true").load(input_data_path+"/header").fillna("")
    # df_detail = spark.read.format("csv").option("header","true").load(input_data_path+"/detail").fillna("")
    connection_redshift_options["dbtable"] = args["HEADER_TABLE"]
    df_header = glueContext.create_dynamic_frame_from_options(connection_type="redshift", connection_options=connection_redshift_options).toDF().filter("PART_SSN='"+args["PART_SSN"]+"'")
    connection_redshift_options["dbtable"] = args["DETAIL_TABLE"]
    df_header = glueContext.create_dynamic_frame_from_options(connection_type="redshift", connection_options=connection_redshift_options).toDF().filter("PART_SSN='"+args["PART_SSN"]+"'")
    df_summed = df_detail.groupBy("PART_SSN").agg(round(sum("EMPLOYEE"),2).alias("EMPLOYEE_SUM"),round(sum("AUTOMATIC"),2).alias("AUTOMATIC_SUM"),round(sum("MATCHING"),2).alias("MATCHING_SUM"),round(sum("ROW_TOTAL"),2).alias("ROW_TOTAL_SUM"))

    list_data_header = list(map(lambda row: row.asDict(), df_header.collect()))
    list_data_detail = list(map(lambda row: row.asDict(), df_detail.collect()))
    list_data_summed = list(map(lambda row: row.asDict(), df_summed.collect()))
    list_data = [list_data_header, list_data_detail, list_data_summed]
    print(list_data)
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
    s3_key = path[s3_bucket_index+6:]
    obj = s3.Object(s3_bucket, s3_key+filename)
    s3 = boto3.client('s3')
    with open(temp_pdf, "rb") as f:
        s3.upload_fileobj(f, s3_bucket, s3_key+"/"+filename)
        
template_data = read_template(args["TEMPLATE_PATH"])
template = Template(template_data)

list_data = get_report_data(args["GLUE_CONN_NAME"],args["TMP_DIR_PATH"])
# print(type(list_data))
template_out = template.render(list_data=list_data)

with open("output.html","w+") as f:
    f.write(template_out)
# pdf=pisa.CreatePDF( StringIO(template_out),open("output.pdf", "wb"))   
convertHtmlToPdf(template_out, temp_pdf)
write_pdf_s3(temp_pdf,args["OUTPUT_PDF_PATH"])

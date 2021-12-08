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


#For local invocation
# args = {"TEMPLATE_PATH":"file://C:\\Users\\RAJSR1\\Downloads\\vscoderepos\\jinjaglue\\resources\\template.html","INPUT_DATA_PATH":"file://C:\\Users\\RAJSR1\\Downloads\\vscoderepos\\jinjaglue\\data\\sample.csv"}

#For glue invocation
args = getResolvedOptions(sys.argv, ["TEMPLATE_PATH","INPUT_DATA_PATH","OUTPUT_PDF_PATH"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

temp_pdf = "output.pdf"

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

def get_report_data(input_data_path):
    import boto3
    df = spark.read.format("csv").option("header","true").load(input_data_path)
    df_agg = df.groupBy("customer").agg(sf.sum("sale").alias("sum_sales"))
    list_data = list(map(lambda row: row.asDict(), df_agg.collect()))
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
    with open("output.pdf", "rb") as f:
        s3.upload_fileobj(f, s3_bucket, s3_key+"/"+filename)
        
template_data = read_template(args["TEMPLATE_PATH"])
template = Template(template_data)

list_data = get_report_data(args["INPUT_DATA_PATH"])
print(type(list_data))
template_out = template.render(list_data=list_data)

with open("output.html","w+") as f:
    f.write(template_out)
# pdf=pisa.CreatePDF( StringIO(template_out),open("output.pdf", "wb"))   
convertHtmlToPdf(template_out, temp_pdf)
write_pdf_s3(temp_pdf,args["OUTPUT_PDF_PATH"])
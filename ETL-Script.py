#Import python modules
import sys
import boto3
from time import time

#Import pyspark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import SparkSession

#Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

 
#Initialize contexts, session and job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

#Reads data from GlueTable in GlueDatabase into a glue dynamic frame.
#Converts the dynamic frame to a spark dataframe.
#if reading fails program is terminated.
def ReadData(GlueDatabase, GlueTable, log_bucket, read_log_object):
    
    s3_client = boto3.resource("s3")
    read_logs = ""
    
    success = False
    try:
        #Read data to Glue dynamic frame
        dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database = GlueDatabase, table_name = GlueTable)
        success = True
        read_logs += "Read Successful\n"
    except Exception as e:
        read_logs += "Read Failed:\n" + str(e) + "\n"
    
    #write read logs
    s3_client.Object(log_bucket, read_log_object).put(Body = read_logs)
    
    #terminate if reading failed
    if success is False:
        sys.exit()
    
    #Convert dynamic frame to data frame to use standard pyspark functions
    return dynamic_frame_read.toDF()
    
#Transforms the schema of the dataframe
def TransformData(data_frame, log_bucket, transform_log_object):
    
    s3_client = boto3.resource('s3')
    transform_logs = ""
    
    #returns schema of a main column in the form of a string
    def GetSchema(column_name, data_frame):
        schema = data_frame.select(column_name).schema.simpleString()
        start_id = len("struct<" + column_name + ":")
        return schema[start_id:-1]
        
    #Changes workflowId schema
    def ChangeWorkflowIdSchema(data_frame):
        data_frame = data_frame.withColumn("workflowId", f.when(f.col("workflowId").isNotNull(), f.struct(f.struct(f.struct(f.col("workflowId.m")).alias("generateInvoiceGraph")).alias("m"))).otherwise(f.lit(None)))
        return data_frame
    
    #concatenate useCaseId and version
    def Concatenate_useCaseId_version(data_frame):
        data_frame = data_frame.withColumn("useCaseId", f.struct(f.concat(f.col("useCaseId.s"), f.lit(":"), f.col("version.n")).alias("s")))
        return data_frame
    
    #change nested field names of a main column
    def ChangeNestedFieldNames(data_frame, column_name, old_to_new_mapping):
        #get column schema in the form of string
        column_schema = GetSchema(column_name, data_frame)
        
        #iterate over the mapping and change the old field names to new field names
        for old_name, new_name in old_to_new_mapping.items():
            column_schema = column_schema.replace(old_name, new_name)
        
        #null cannot be casted to null, so change the null mentions in the schema to string
        column_schema = column_schema.replace('null', 'string')
    
        #cast the old schema to new schema
        return data_frame.withColumn(column_name, f.col(column_name).cast(column_schema))
        
    #change main field names
    def ChangeMainFieldNames(data_frame, old_to_new_mapping):
        #iterate over the mapping and change the old field names to new field names
        for old_name, new_name in old_to_new_mapping.items():
                data_frame = data_frame.withColumnRenamed(old_name, new_name)
        return data_frame


    #Remove storageAttributes
    def Remove_storageAttributes(data_frame):
        
        expression = 'transform(results.l, x -> struct(struct( \
                                                            x.m.storageAttributesList as storageAttributesList, \
                                                            x.m.otherAttributes as otherAttributes, \
                                                            x.m.documentExchangeDetailsDO as documentExchangeDetailsDO, \
                                                            x.m.rawDataStorageDetailsList as rawDataStorageDetailsList, \
                                                            x.m.documentConsumers as documentConsumers, \
                                                            x.m.documentIdentifiers as documentIdentifiers) as m))'
        
        data_frame = data_frame.withColumn("results", f.struct(f.expr(expression).alias("l")))
        return data_frame
        
    #Removed storage attributes
    start_time = time()        
    data_frame = Remove_storageAttributes(data_frame)
    end_time = time()
    transform_logs += "storageAttributes removed! Duration: " + str(end_time - start_time) + "\n"
    
    #change workflowId schema
    start_time = time()        
    data_frame = ChangeWorkflowIdSchema(data_frame)
    end_time = time()
    transform_logs += "Workflow Schema changed! Duration: " + str(end_time - start_time) + "\n"
    
    #concatenate useCaseId and version
    start_time = time()        
    data_frame = Concatenate_useCaseId_version(data_frame)
    end_time = time()        
    transform_logs += "useCaseId and Version concatenated! Duration: " + str(end_time - start_time) + "\n"
    
    #change names of nested fields in results
    column_name = 'results'
    #build the mapping
    old_to_new_mapping = {}
    old_to_new_mapping['documentExchangeDetailsDO'] = 'documentExchangeDetailsList'
    old_to_new_mapping['rawDataStorageDetailsList'] = 'rawDocumentDetailsList'
    old_to_new_mapping['documentConsumers'] = 'documentConsumerList'
    old_to_new_mapping['documentIdentifiers'] = 'documentIdentifierList'
    old_to_new_mapping['storageAttributesList'] = 'generatedDocumentDetailsList'
    old_to_new_mapping['otherAttributes'] = 'documentTags'
    
    start_time = time()        
    data_frame = ChangeNestedFieldNames(data_frame, column_name, old_to_new_mapping)
    end_time = time()
    transform_logs += "Results schema change! Duration: " + str(end_time - start_time) + "\n"
    
    #change main field names
    old_to_new_mapping = {}
    old_to_new_mapping["TenantIdTransactionId"] = "RequestId"
    old_to_new_mapping["version"] = "Version"
    old_to_new_mapping["state"] = "RequestState"
    old_to_new_mapping["workflowId"] = "WorkflowIdentifierMap"
    old_to_new_mapping["lastUpdatedDate"] = "LastUpdatedTime"
    old_to_new_mapping["useCaseId"] = "UsecaseIdAndVersion"
    old_to_new_mapping["results"] = "DocumentMetadataList"
    
    start_time = time()       
    data_frame = ChangeMainFieldNames(data_frame, old_to_new_mapping)
    end_time = time()        
    transform_logs += "Main Field names changed! Duration: " + str(end_time - start_time) + "\n"
    
    #write transformation logs
    s3_client.Object(log_bucket, transform_log_object).put(Body = transform_logs)
    
    #return transformed dataframe
    return data_frame

#Writes the dataframe
def WriteData(data_frame, s3_write_path, log_bucket, write_log_object):
    
    write_logs = ""
    s3_client = boto3.resource("s3")
    
    try:
        start_time = time()
        data_frame.write.mode("append").json(s3_write_path)
        end_time = time()
        write_logs += "Write success!\n"
        write_logs += "Duration: " + str(end_time - start_time) + "\n"
    except Exception as e:
        write_logs += "Write Failed!\n" + str(e) + "\n"
    
    s3_client.Object(log_bucket, write_log_object).put(Body = write_logs)
    
########
#EXTRACT
########

log_bucket = "script-logs-etl"
read_log_object = "read_logs.txt"
glue_database = "transactions-db"
glue_table = "2020_05_28_16_08_00"
data_frame = ReadData(glue_database, glue_table, log_bucket, read_log_object)

##########
#TRANSFORM
##########

log_bucket = "script-logs-etl"
transform_log_object = "transform_logs.txt"
data_frame = TransformData(data_frame, log_bucket, transform_log_object)

#####
#LOAD
#####

s3_write_path = "s3://ip-metadata-bucket-demo/"
log_bucket = "script-logs-etl"
write_log_object = "write_logs.txt"
WriteData(data_frame, s3_write_path, log_bucket, write_log_object)

job.commit()

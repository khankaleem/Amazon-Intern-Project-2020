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
    
    #change workflowId schema
    start_time = time()        
    data_frame = ChangeWorkflowIdSchema(data_frame)
    end_time = time()
    transform_logs += "Workflow Schema changed! duration: " + str(end_time - start_time) + "\n"
    
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
    old_to_new_mapping['storageAttributes'] = 'storageAttributes'
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
    

#Query the dataframe, and write results to S3
def WriteData(data_frame, null_columns, not_null_columns, s3_write_path, log_bucket, write_log_object):
    
    write_logs = ""
    s3_client = boto3.resource('s3')
    
    #number of optional columns
    n_columns = len(null_columns)
    #total schemas possible
    total_subsets = 2**n_columns
    
    #write dataframe to s3
    def WriteDataframe(data_frame):
        nonlocal write_logs
        
        #check for empty Dataframe
        if bool(data_frame.head(1)) is False:
            write_logs += "Write Success\n"
            return
        
        try:
            #convert spark dataframe to glue dynamic frame
            dynamic_frame_write = DynamicFrame.fromDF(data_frame, glue_context, "dynamic_frame_write")
            #write the dynamic frame to s3 path
            datasink = glue_context.write_dynamic_frame.from_options(frame = dynamic_frame_write, connection_type = "s3", connection_options = {"path": s3_write_path}, format = "json", transformation_ctx = "datasink2")
            write_logs += "Write Success\n"
        except Exception as e:
            write_logs += "Write Failed\n: " + str(e) + "\n"
    
    #query the dataframe
    def ProcessQuery(data_frame, query):
        
        nonlocal write_logs
        write_logs += "--------Query--------\n" + query + "\n"
        
        #Create SQL View from dataframe
        data_frame.createOrReplaceTempView("table")
        
        #query the table
        try:
            #get the result of query
            table_df = session.sql(query)
            write_logs += "Query Success\n"
            #write the dataframe
            WriteDataframe(table_df)
        except Exception as e:
            write_logs += "Query Failed\n: " + str(e) + "\n"
        
    #return two lists: queries and queries_compliment.
    #The where clauses of corresponding queries in the lists are compliments of each other.
    def BuildQuery():
        
        #helper function to get the binary notation of a decimal integer
        def ToBinary(num, length):
            ans = []
            for i in range(length):
                if (num&(1<<i)) == 0:
                    ans.append(0)
                else:
                    ans.append(1)
            return ans
        
        condition = [" IS NOT NULL", " IS NULL"]
        queries = []
        queries_compliment = []
        
        #build query for every subset
        for i in range(0, total_subsets):
            
            #initialize the select clause and where clause of the query
            select_clause = ", ".join(not_null_columns)
            where_clause = ""
            subset = ToBinary(i, n_columns)
            
            #build query
            for j in range(0, n_columns-1):
                k = subset[j]
                where_clause += "(" + null_columns[j] + condition[k] + ") AND "
                if k == 0:
                    select_clause += ", " + null_columns[j]
            k = subset[n_columns-1]
            where_clause += "(" + null_columns[n_columns-1] + condition[k] + ")"
            if k == 0:
                select_clause += ", " + null_columns[n_columns-1]
            
            query = "SELECT " + select_clause + " FROM table WHERE " + where_clause
            query_compliment = "SELECT * FROM table WHERE NOT " + "(" + where_clause + ")"
            
            #add query to list
            queries.append(query)
            
            #queries_compliment
            queries_compliment.append(query_compliment)
            
        return queries, queries_compliment
        
    #returns the result of query on dataframe  
    def ReduceDataframe(data_frame, query):
        
        nonlocal write_logs
        write_logs += "--------Query Compliment--------\n" + query + "\n"
        
        #Create SQL View from dataframe
        data_frame.createOrReplaceTempView("table")
        #query the table
        try:
            #get the result of query
            table_df = session.sql(query)
            write_logs += "Query Compliment Success\n"
            return table_df
        except Exception as e:
            write_logs += "Query Compliment Failed\n: " + str(e) + "\n"
        
        return data_frame
    
    
    #get the list of queries
    queries, queries_compliment = BuildQuery()
    
    #query the dataframe and write to S3
    for i in range(total_subsets):
        
        write_logs += "===============================" + str(i) + "========================================\n"
        
        start_time = time()
        #perform query and write to s3
        ProcessQuery(data_frame, queries[i])
        #remove rows used in ProcessQuery
        data_frame = ReduceDataframe(data_frame, queries_compliment[i])
        end_time = time()
        
        write_logs += "Duration: " + str(end_time - start_time) + "\n"
        if bool(data_frame.head(1)) is False:
            break
    
    #log dataframe writes to S3
    s3_client.Object(log_bucket, write_log_object).put(Body = write_logs)
    
    
########
#EXTRACT
########

log_bucket = "script-logs-etl"
read_log_object = "read_logs.txt"
glue_database = "transactions-db"
glue_table = "2020_05_28_16_08_00"
df = ReadData(glue_database, glue_table, log_bucket, read_log_object)

##########
#TRANSFORM
##########

log_bucket = "script-logs-etl"
transform_log_object = "transform_logs.txt"
df = TransformData(df, log_bucket, transform_log_object)

#####
#LOAD
#####

s3_write_path = "s3://ip-metadata-bucket-demo"
log_bucket = "script-logs-etl"
write_log_object = "write_logs.txt"
null_columns = ["index1", "index2", "index3", "index4", "updateMetadataApprovalMap"]
not_null_columns = ["RequestId", "UsecaseIdAndVersion", "Version", "LastUpdatedTime", "RequestState", "DocumentMetadataList", "WorkflowIdentifierMap"]
WriteData(df, null_columns, not_null_columns, s3_write_path, log_bucket, write_log_object)

job.commit()

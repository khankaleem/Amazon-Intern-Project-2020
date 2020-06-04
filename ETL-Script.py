#########################################
### IMPORT LIBRARIES AND SET VARIABLES
#########################################
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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

#Reads data from GlueTable in GlueDatabase into a glue dynamic frame.
#Converts the dynamic frame to a spark dataframe.
#If reading fails program is terminated.
def ReadData(GlueDatabase, GlueTable, log_bucket, read_log_object):
    
    s3_client = boto3.resource('s3')
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
        
        #user defined function for change of schema for workflowId
        def Helper_ChangeWorkflowSchema(row):
            #null check
            if row is None:
                return None
            #change schema
            data = row.asDict(True)
            return {"m":{"generateInvoiceGraph":data}}
        
        #get old schema of workflowId
        workflow_schema_old = GetSchema("workflowId", data_frame)
        #new schema of workflowId
        workflow_schema_new = "struct<m:struct<generateInvoiceGraph:" + workflow_schema_old + ">>"
        
        #change workflowId schema
        ChangeWorkflowSchema = f.udf(Helper_ChangeWorkflowSchema, workflow_schema_new)
        data_frame = data_frame.withColumn("workflowId", ChangeWorkflowSchema("workflowId"))
        
        #return changed dataframe
        return data_frame
    
    #concatenate useCaseId and version
    def Concatenate_useCaseId_version(data_frame):
        
        #user defined function
        def Helper_Concatenate(row1, row2):
            dict1 = row1.asDict(True)
            dict2 = row2.asDict(True)
            return {"s":dict1["s"] + ":" + dict2["n"]}
        
        #concatenate
        Concatenate = f.udf(Helper_Concatenate, 'struct<s:string>')
        data_frame = data_frame.withColumn("useCaseId", Concatenate("useCaseId", "version"))
        
        #return changed dataframe
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
        
        write_success = False
        try:
            #convert spark dataframe to glue dynamic frame
            dynamic_frame_write = DynamicFrame.fromDF(data_frame, glue_context, "dynamic_frame_write")
            #write the dynamic frame to s3 path
            datasink = glue_context.write_dynamic_frame.from_options(frame = dynamic_frame_write, connection_type = "s3", connection_options = {"path": s3_write_path}, format = "json", transformation_ctx = "datasink2")
            write_success = True
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
        query_success = False
        try:
            #get the result of query
            table_df = session.sql(query)
            query_success = True
            write_logs += "Query Success\n"
        except Exception as e:
            write_logs += "Query Failed\n: " + str(e) + "\n"
        #failed query
        if query_success is False:
            return
        #write the dataframe
        WriteDataframe(table_df)
    
    #return a list of queries
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
    
    #query the dataframe and write tO S3
    for i in range(total_subsets):
        write_logs += "===============================" + str(i) + "========================================\n"
        start_time = time()
        ProcessQuery(data_frame, queries[i])
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
#write
WriteData(df, null_columns, not_null_columns, s3_write_path, log_bucket, write_log_object)

job.commit()

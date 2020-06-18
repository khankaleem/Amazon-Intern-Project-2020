'''
The script reads data from S3, transforms the data and writes it back to S3.
Method for reading: readData
Method for tranforming: tranformSchema
Method for writing: writeData
'''

#Import python modules
import sys
import boto3
from time import time

#Import PySpark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import SparkSession

#Import Glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

#Initialize contexts, spark session and job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

'''
Reads transactions data from the glue table in glue database, into a glue dynamic frame.
Converts the glue dynamic frame to a PySpark dataframe. If reading fails program is terminated.
Input: 
    glueDatabase: The resource specifying the logical tables in AWS Glue
    glueTable: The resource specifying the tabular data in the AWS Glue data catalog
    s3LogBucket: The name s3 bucket for logging of read success/failure
    s3Client: The s3 client for logging
Output:
    transactionsDataFrame: a PySpark dataframe containing transactions data
'''
def readData(glueDatabase, glueTable, s3LogBucket, s3Client):
    
    try:
        #Read data into Glue dynamic frame
        glue_dynamic_frame = glue_context.create_dynamic_frame.from_catalog(database = glueDatabase, table_name = glueTable)
        #write success logs in s3LogBucket to file _readSuccess_
        s3Client.Object(s3LogBucket, "_readSuccess_").put(Body = "Read Successful\n")
    except Exception as e:
        #write failure logs in s3LogBucket to file _readFailure_
        s3Client.Object(s3LogBucket, "_readFailure_").put(Body = "Read Failed\n" + str(e))
        #terminate program
        sys.exit()
        
    #Convert glue dynamic frame to spark data frame to use standard pyspark functions
    transactionsDataframe = glue_dynamic_frame.toDF()
    #return the pyspark data frame
    return transactionsDataframe
    

'''
The method transforms the transactions dataframe schema.
Input: 
    transactionsDataframe : PySpark dataframe containing transactions data
    s3LogBucket: The name of s3 bucket for logging
    s3Client: The s3 client for logging
Output:
    ipMetadataDataFrame: Transformed transactions dataframe
'''
def transformSchema(transactionsDataframe, s3LogBucket, s3Client):
        
    '''
    The method returns the schema of a main column in the form of a string.
    Parameters:
        columnName: the name of the main column
        dataFrame: the data frame containing the column
    '''
    def getSchema(columnName, dataframe):
        #get the column schema in the form of string
        schema = dataframe.select(columnName).schema.simpleString()
        startId = len("struct<" + columnName + ":")
        return schema[startId:-1]
        
    '''
    The method changes the workflowId schema.
    Parameters:
        oldDataFrame: The dataframe whose schema needs to be changed
    '''
    def changeWorkflowIdSchema(oldDataframe):
        
        #check if workflowId column is in the schema
        if "workflowId" in oldDataframe.columns:
            #change workflowId schema
            newDataframe = oldDataframe.withColumn("workflowId", f.when(f.col("workflowId").isNotNull(), f.struct(f.struct(f.struct(f.col("workflowId.m")).alias("generateInvoiceGraph")).alias("m"))).otherwise(f.lit(None)))
        
        #return the transformed dataframe
        return newDataframe
    
    '''
    The method concatenates useCaseId and version.
    Parameters:
        oldDataFrame: The dataframe whose schema needs to be changed
    '''
    def concatenateUseCaseIdAndVersion(oldDataframe):
        newDataframe = oldDataframe.withColumn("useCaseId", f.struct(f.concat(f.col("useCaseId.s"), f.lit(":"), f.col("version.n")).alias("s")))
        return newDataframe
    
    '''
    The method changes the nested fields of a main column in the old dataframe.
    Parameters: 
        columnName: the name of the main column
        oldDataframe: the data frame containing the main column
        nestedMapping: The mapping of the nested fields inside the main column
    '''
    def changeNestedFieldNames(oldDataframe, columnName, nestedMapping):
        #check if column exists in the schema
        if columnName not in oldDataframe.columns:
            return oldDataframe
        
        #get column schema in the form of string
        column_schema = getSchema(columnName, oldDataframe)
        
        #iterate over the mapping and change the old field names to new field names
        for old_name, new_name in nestedMapping.items():
            column_schema = column_schema.replace(old_name, new_name)
        
        #null cannot be casted to null, so change the null mentions in the schema to string
        column_schema = column_schema.replace('null', 'string')
        
        #cast the old schema to new schema
        newDataframe = oldDataframe.withColumn(columnName, f.col(columnName).cast(column_schema))
        return newDataframe
    
    '''
    The method changes the main field names in the old dataframe.
    Parameters:
        oldDataFrame: The dataframe whose schema needs to be changed
        mainFieldMapping: contains the mapping of the main field names in oldDataFrame
    '''
    def changeMainFieldNames(oldDataframe, mainColumnMapping):
        #iterate over the mapping and change the old field names to new field names
        newDataframe = oldDataframe
        for old_name, new_name in mainColumnMapping.items():
            #check if old name is in schema
            if old_name in oldDataframe.columns:
                newDataframe = newDataframe.withColumnRenamed(old_name, new_name)
        return newDataframe
    
    '''
    The method removes the nested fields in results from old data frame.
    Parameters:
        oldDataFrame: The dataframe whose schema needs to be changed 
        dropList: The list of nested fields inside results to be dropped
        keepList: The list of nested fields inside results to be kept
    '''
    def dropNestedFieldsInResults(oldDataframe, dropList, keepList):
        #check if results exists or dropList is empty
        if "results" not in oldDataframe.columns or len(dropList) == 0:
            return oldDataframe
        
        #build a transform expression for results    
        expression = "transform(results.l, x -> struct(struct("
        for nested_field in keepList:
            expression += "x.m." + nested_field + " as " + nested_field + ","
        expression = expression[:-1]+") as m))"
        
        #apply the transform to drop the fields
        newDataframe = oldDataframe.withColumn("results", f.struct(f.expr(expression).alias("l")))
        
        #return the transformed data frame
        return newDataframe

    #Initialize logs
    logs_ = ""
    
    #Initialize new ipMetadataDataFrame
    ipMetadataDataframe = transactionsDataframe
    
    #Remove storage attributes
    '''
    dropList contains nested columns inside results that need to be dropped
    keepList contains nested columns inside results that need to be retained
    Change the keepList and dropList as per your use case
    '''
    keepList = ["storageAttributesList", "otherAttributes", "documentExchangeDetailsDO", "rawDataStorageDetailsList", "documentConsumers", "documentIdentifiers"]
    dropList = ["storageAttributes"]
    start_time = time()
    ipMetadataDataframe = dropNestedFieldsInResults(ipMetadataDataframe, dropList, keepList)
    end_time = time()
    logs_ += "storageAttributes removed! Duration: " + str(end_time - start_time) + "\n"
    
    #change workflowId schema
    start_time = time()
    ipMetadataDataframe = changeWorkflowIdSchema(ipMetadataDataframe)
    end_time = time()
    logs_ += "Workflow Schema changed! Duration: " + str(end_time - start_time) + "\n"
    
    #concatenate useCaseId and version
    start_time = time()
    ipMetadataDataframe = concatenateUseCaseIdAndVersion(ipMetadataDataframe)
    end_time = time()
    logs_ += "useCaseId and Version concatenated! Duration: " + str(end_time - start_time) + "\n"
    
    '''
    resultsMapping contains mapping of old schema nested fields to new schema nested fields in results.
    Change the mapping as per your use case.
    '''
    resultsNestedColumnMapping = {}
    resultsNestedColumnMapping['documentExchangeDetailsDO'] = 'documentExchangeDetailsList'
    resultsNestedColumnMapping['rawDataStorageDetailsList'] = 'rawDocumentDetailsList'
    resultsNestedColumnMapping['documentConsumers'] = 'documentConsumerList'
    resultsNestedColumnMapping['documentIdentifiers'] = 'documentIdentifierList'
    resultsNestedColumnMapping['storageAttributesList'] = 'generatedDocumentDetailsList'
    resultsNestedColumnMapping['otherAttributes'] = 'documentTags'
    
    start_time = time()
    ipMetadataDataframe = changeNestedFieldNames(ipMetadataDataframe, "results", resultsNestedColumnMapping)
    end_time = time()
    logs_ += "Results schema change! Duration: " + str(end_time - start_time) + "\n"
    
    '''
    mainFieldMapping contains mapping of old schema main fields to new schema main fields.
    Change the mapping as per your use case.
    '''
    mainColumnMapping = {}
    mainColumnMapping["TenantIdTransactionId"] = "RequestId"
    mainColumnMapping["version"] = "Version"
    mainColumnMapping["state"] = "RequestState"
    mainColumnMapping["workflowId"] = "WorkflowIdentifierMap"
    mainColumnMapping["lastUpdatedDate"] = "LastUpdatedTime"
    mainColumnMapping["useCaseId"] = "UsecaseIdAndVersion"
    mainColumnMapping["results"] = "DocumentMetadataList"
    
    start_time = time()
    ipMetadataDataframe = changeMainFieldNames(ipMetadataDataframe, mainColumnMapping)
    end_time = time()
    logs_ += "Main Field names changed! Duration: " + str(end_time - start_time) + "\n"
    
    #write transformation logs in s3LogBucket to file _TransformationLogs_
    s3Client.Object(s3LogBucket, "_TransformationLogs_").put(Body = logs_)
    
    #return the transformed dataframe
    return ipMetadataDataframe

'''
The method writes Ip-metadata to s3.
Input: 
    ipMetaDataframe: Pyspark dataframe containing tranformed transactions data
    s3WritePath: The path to the s3 bucket where the dataframe is to be written 
    s3LogBucket: The name of the s3 bucket for logging
    s3Client: The s3 client for logging
'''
def writeData(ipMetadataDataframe, s3WritePath, s3LogBucket, s3Client):
    
    #Initialize logs
    logs_ = ""
    
    try:
        #write the dataframe
        start_time = time()
        ipMetadataDataframe.write.mode("append").json(s3WritePath)
        end_time = time()
        logs_ += "Write success!\n" + "Duration: " + str(end_time - start_time) + "\n"
    except Exception as e:
        logs_ += "Write Failed!\n" + str(e) + "\n"
    
    #write logs in s3LogBucket to file _WriteLogs_
    s3Client.Object(s3LogBucket, "_WriteLogs_").put(Body = logs_)
    

#Build s3 client for logging
s3Client = boto3.resource("s3")
#Specify the s3 log bucket for logging of read, transform and write success
s3LogBucket = "script-logs-etl"

'''
EXTRACT DATA:
    Read transactions data from s3.
    The parameters glueDatabase and glueTable need to be specified before executing the job
'''

glueDatabase = "transactions-db"
glueTable = "2020_05_28_16_08_00"
transactionsDataframe = readData(glueDatabase, glueTable, s3LogBucket, s3Client)

'''
TRANSFORM DATA
'''

ipMetadataDataframe = transformSchema(transactionsDataframe, s3LogBucket, s3Client)

'''
#LOAD DATA
    load Ip-metadata to s3.
    The parameter s3WritePath needs to be specified before executing the job
'''

s3WritePath = "s3://ip-metadata-bucket-demo/"
writeData(ipMetadataDataframe, s3WritePath, s3LogBucket, s3Client)

#Commit Job
job.commit()

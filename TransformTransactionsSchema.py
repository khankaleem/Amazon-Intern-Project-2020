'''
The script reads transactions data from the glue data catalog, transforms its schema and writes it back to S3.
Method for reading: readData
Method for tranforming schema: tranformSchema
Method for writing: writeData
'''

#Import Python modules
import sys
import boto3
from time import time

#Import PySpark modules
from pyspark.context import SparkContext
import pyspark.sql.functions as f

#Import Glue modules
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

#Initialize Glue context
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)


'''
The method reads transactions data from the glue table in glue database, into a glue dynamic frame.
Converts the glue dynamic frame to a PySpark dataframe. If reading fails program is terminated.
Input: 
    glueDatabase: The resource specifying the logical tables in AWS Glue
    glueTable: The resource specifying the tabular data in the AWS Glue data catalog
Output:
    transactionsDataframe: PySpark dataframe containing transactions data
'''
def readData(glueDatabase, glueTable):
    
    try:
        #Read data into Glue dynamic frame
        glueDynamicFrame = glue_context.create_dynamic_frame.from_catalog(database = glueDatabase, table_name = glueTable)
        #Convert glue dynamic frame to spark data frame to use standard pyspark functions
        return glueDynamicFrame.toDF()
    except Exception as e:
        #Log read failure to cloudwatch management console. Visibile in logs of AWS glue on console.
        print("=======Read Failed=======\n" + str(e))
        #Terminate program
        sys.exit()

'''
The method transforms the transactions dataframe schema.
Input: 
    transactionsDataframe : PySpark dataframe containing transactions data
Output:
    ipMetadataDataframe: Transformed transactions dataframe
'''
def transformSchema(transactionsDataframe):
        
    '''
    The method returns the schema of a main column in the form of a string.
    Input:
        mainColumnName: the name of the main column
        dataframe: the data frame containing the main column
    Ouput: 
        The main column schema in the form of a string
    '''
    def getSchema(mainColumnName, dataframe):
        #get the column schema in the form of string
        schema = dataframe.select(mainColumnName).schema.simpleString()
        startId = len("struct<" + mainColumnName + ":")
        return schema[startId:-1]
        
    '''
    The method changes the workflowId schema.
    Input:
        transactionsDataframe: The dataframe whose schema needs to be changed
    Output: 
        The dataframe with key generateInvoiceGraph added to workflowId schema
    '''
    def changeWorkflowIdSchema(transactionsDataframe):
        
        #check if workflowId column is in the schema
        if "workflowId" in transactionsDataframe.columns:
            return transactionsDataframe.withColumn("workflowId", f.when(f.col("workflowId").isNotNull(), f.struct(f.struct(f.struct(f.col("workflowId.m")).alias("generateInvoiceGraph")).alias("m"))).otherwise(f.lit(None)))
        else:
            return transactionsDataframe
    
    '''
    The method concatenates useCaseId and version.
    Input:
        transactionsDataframe: The dataframe whose schema needs to be changed
    Output:
        The dataframe with useCaseId and version concatenated by the literal ":"
    '''
    def concatenateUseCaseIdAndVersion(transactionsDataframe):
        return transactionsDataframe.withColumn("useCaseId", f.struct(f.concat(f.col("useCaseId.s"), f.lit(":"), f.col("version.n")).alias("s")))
        
    '''
    The method changes names of the nested columns of a main column in the transactions dataframe.
    Input: 
        columnName: the name of the main column
        transactionsDataframe: the data frame containing the main column
        nestedColumnMapping: The mapping of the names of the nested fields inside the main column
    Output:
        The dataframe with nested columns of the main column renamed
    '''
    def changeNestedColumnNames(transactionsDataframe, columnName, nestedColumnMapping):
        #check if column exists in the schema
        if columnName not in transactionsDataframe.columns:
            return transactionsDataframe
        
        #get column schema in the form of string
        column_schema = getSchema(columnName, transactionsDataframe)
        
        #iterate over the mapping and change the old field names to new field names
        for transactionsNestedColumnName, ipMetadataNestedColumnName in nestedColumnMapping.items():
            if transactionsNestedColumnName in column_schema:
                column_schema = column_schema.replace(transactionsNestedColumnName, ipMetadataNestedColumnName)
        
        #null cannot be casted to null, so change the null mentions in the schema to string
        if "null" in column_schema:
            column_schema = column_schema.replace("null", "string")
        
        #cast the old schema to new schema
        return transactionsDataframe.withColumn(columnName, f.col(columnName).cast(column_schema))
    
    '''
    The method changes the outer column names in the transactions dataframe.
    Input:
        transactionsDataframe: The dataframe whose schema needs to be changed
        outerColumnMapping: contains the mapping of the outer column names
    Output:
        The dataframe with the outer columns renamed
    '''
    def changeOuterColumnNames(transactionsDataframe, outerColumnMapping):
        
        #iterate over the mapping and change the old field names to new field names
        for transactionsOuterColumnName, ipMetadataOuterColumnName in outerColumnMapping.items():
            #check if old name is in schema
            if transactionsOuterColumnName in transactionsDataframe.columns:
                transactionsDataframe = transactionsDataframe.withColumnRenamed(transactionsOuterColumnName, ipMetadataOuterColumnName)

        return transactionsDataframe
    
    '''
    The method removes the nested columns in results specified in dropList, from transactions dataframe.
    Input:
        transactionsDataframe: The dataframe whose schema needs to be changed 
        dropList: The list of nested columns inside results to be dropped
        keepList: The list of nested columns inside results to be kept
    Output:
        The dataframe with the nested columns in results dropped 
    '''
    def dropNestedColumnsInResults(transactionsDataframe, dropList, keepList):
        #check if results exists or dropList is empty
        if "results" not in transactionsDataframe.columns or len(dropList) == 0:
            return transactionsDataframe
        
        #build a transform expression for results
        #expression: "transform(array, func)"" - Transforms elements in array using the function func
        expression = "transform(results.l, x -> struct(struct("
        for nested_field in keepList:
            expression += "x.m." + nested_field + " as " + nested_field + ","
        expression = expression[:-1]+") as m))"
        
        #apply the transform to drop the fields and return the transformed dataframe
        return transactionsDataframe.withColumn("results", f.struct(f.expr(expression).alias("l")))

    #Initialize logs
    logs_ = ""
    
    '''
    Remove storage attributes: The content of storageAttributes is already present in storageAttributesList, hence remove the redundancy
    dropList contains nested columns inside results that need to be dropped
    keepList contains nested columns inside results that need to be retained
    Change the keepList and dropList as per the usecase
    '''
    keepList = ["storageAttributesList", "otherAttributes", "documentExchangeDetailsDO", "rawDataStorageDetailsList", "documentConsumers", "documentIdentifiers"]
    dropList = ["storageAttributes"]
    start_time = time()
    transactionsDataframe = dropNestedColumnsInResults(transactionsDataframe, dropList, keepList)
    end_time = time()
    logs_ += "storageAttributes removed! Duration: " + str(end_time - start_time) + "\n"
    
    '''
    change workflowId schema
    '''
    start_time = time()
    transactionsDataframe = changeWorkflowIdSchema(transactionsDataframe)
    end_time = time()
    logs_ += "Workflow Schema changed! Duration: " + str(end_time - start_time) + "\n"
    
    '''
    concatenate useCaseId and version
    '''
    start_time = time()
    transactionsDataframe = concatenateUseCaseIdAndVersion(transactionsDataframe)
    end_time = time()
    logs_ += "useCaseId and Version concatenated! Duration: " + str(end_time - start_time) + "\n"
    
    '''
    resultsMapping contains mapping of old schema nested fields to new schema nested fields in results.
    Change the mapping as per the usecase.
    '''
    resultsNestedColumnMapping = {}
    resultsNestedColumnMapping['documentExchangeDetailsDO'] = 'documentExchangeDetailsList'
    resultsNestedColumnMapping['rawDataStorageDetailsList'] = 'rawDocumentDetailsList'
    resultsNestedColumnMapping['documentConsumers'] = 'documentConsumerList'
    resultsNestedColumnMapping['documentIdentifiers'] = 'documentIdentifierList'
    resultsNestedColumnMapping['storageAttributesList'] = 'generatedDocumentDetailsList'
    resultsNestedColumnMapping['otherAttributes'] = 'documentTags'
    
    start_time = time()
    transactionsDataframe = changeNestedColumnNames(transactionsDataframe, "results", resultsNestedColumnMapping)
    end_time = time()
    logs_ += "Results schema changed! Duration: " + str(end_time - start_time) + "\n"
    
    '''
    mainFieldMapping contains mapping of old schema main fields to new schema main fields.
    Change the mapping as per the usecase.
    '''
    outerColumnMapping = {}
    outerColumnMapping["TenantIdTransactionId"] = "RequestId"
    outerColumnMapping["version"] = "Version"
    outerColumnMapping["state"] = "RequestState"
    outerColumnMapping["workflowId"] = "WorkflowIdentifierMap"
    outerColumnMapping["lastUpdatedDate"] = "LastUpdatedTime"
    outerColumnMapping["useCaseId"] = "UsecaseIdAndVersion"
    outerColumnMapping["results"] = "DocumentMetadataList"
    
    start_time = time()
    transactionsDataframe = changeOuterColumnNames(transactionsDataframe, outerColumnMapping)
    end_time = time()
    logs_ += "Outer column names changed! Duration: " + str(end_time - start_time) + "\n"
    
    #write transformation 
    print("======Transformation Logs======\n" + logs_)
    print("hello1")
    print(time())
    #return the transformed dataframe
    return transactionsDataframe

'''
The method writes Ip-metadata dataframe to s3.
Input: 
    ipMetaDataframe: Pyspark dataframe containing tranformed transactions data
    s3WritePath: The path to the s3 bucket where the dataframe is to be written 
'''
def writeData(ipMetadataDataframe, s3WritePath):
    
    print("hello")
    print(time())
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
    
    #write logs
    print("=====Write Logs=====\n" + str(logs_))

'''
EXTRACT DATA:
    Read transactions data from s3.
    The parameters glueDatabase and glueTable need to be specified before executing the job
'''

glueDatabase = "internship-project-one-database"
glueTable = "2020_06_23_08_19_12"
transactionsDataframe = readData(glueDatabase, glueTable)

'''
TRANSFORM DATA:
    Transform the transactionsDataframe
'''

ipMetadataDataframe = transformSchema(transactionsDataframe)

'''
#LOAD DATA
    load ipMetadataDataframe to s3.
    The parameter s3WritePath needs to be specified before executing the job
'''

s3WritePath = "s3://internship-project-one/"
writeData(ipMetadataDataframe, s3WritePath)

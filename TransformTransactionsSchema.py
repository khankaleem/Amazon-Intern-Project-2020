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
sparkContext = SparkContext.getOrCreate()
glueContext = GlueContext(sparkContext)

'''
The method reads transactions data from the glue table in glue database, into a glue dynamic frame.
Converts the glue dynamic frame to a PySpark dataframe. If reading fails program is terminated.
Input: 
    glueDatabase: The resource specifying the logical tables in AWS Glue
    glueTable: The resource specifying the tabular data in the AWS Glue data catalog
Output:
    PySpark dataframe containing transactions data
'''
def readData(glueDatabase, glueTable):
    
    try:
        #Read data into Glue dynamic frame
        glueDynamicFrame = glueContext.create_dynamic_frame.from_catalog(database = glueDatabase, table_name = glueTable)
        #Convert glue dynamic frame to spark data frame to use standard pyspark functions
        return glueDynamicFrame.toDF()
    except Exception as e:
        #Log read failure to cloudwatch management console. Visibile in AWS glue console.
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
    The method changes useCaseId schema.
    Input:
        transactionsDataframe: The dataframe whose schema needs to be changed
    Output:
        The dataframe with useCaseId appended by the literal ":1"
    '''
    def changeUseCaseIdSchema(transactionsDataframe):
        return transactionsDataframe.withColumn("useCaseId", f.struct(f.concat(f.col("useCaseId.s"), f.lit(":1")).alias("s")))
    
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
            #check if transactions outer column name is in schema
            if transactionsOuterColumnName in transactionsDataframe.columns:
                transactionsDataframe = transactionsDataframe.withColumnRenamed(transactionsOuterColumnName, ipMetadataOuterColumnName)
        return transactionsDataframe
    
    '''
    The method drops the storageAttributes column in results. 
    Also the schema of storageAttributesList is changed.
    Also the names of nested columns in results are changed.
    Input:
        transactionsDataframe: The dataframe whose schema needs to be changed
    Output:
        The transformed dataframe
    '''
    def changeResultsColumnSchema(transactionsDataframe):
        #check if results exists
        if "results" not in transactionsDataframe.columns:
            return transactionsDataframe
        
        #build a transform expression for results
        #expression: "transform(array, func)" - Transforms each element in array using the function func and returns the transformed array
        expression = 'struct(transform(results.l, x -> struct( \
                                                                struct( \
                                                                        struct(transform(x.m.storageAttributesList.l, \
                                                                                            x -> struct(struct(struct(struct( \
                                                                                                    x.m.retentionPeriodInDays as retentionPeriodInDays, \
                                                                                                    x.m.storageTypeSpecificAttributes.m.DOCUMENT_DOMAIN as DOCUMENT_DOMAIN, \
                                                                                                    x.m.storageTypeSpecificAttributes.m.CUSTOMER_ID as CUSTOMER_ID, \
                                                                                                    x.m.storageTypeSpecificAttributes.m.DOCUMENT_ID as DOCUMENT_ID, \
                                                                                                    x.m.storageTypeSpecificAttributes.m.DOCUMENT_CLASS_ID as DOCUMENT_CLASS_ID, \
                                                                                                    x.m.storageTypeSpecificAttributes.m.LEGAccountId as LEGAccountId, \
                                                                                                    x.m.storageTypeSpecificAttributes.m.PurchasingGroupId as PurchasingGroupId, \
                                                                                                    x.m.storageTypeSpecificAttributes.m.PRINCIPAL_ID as PRINCIPAL_ID, \
                                                                                                    x.m.storageTypeSpecificAttributes.m.DOCUMENT_VERSION_ID as DOCUMENT_VERSION_ID, \
                                                                                                    x.m.storageTypeSpecificAttributes.m.MIME_TYPE as MIME_TYPE, \
                                                                                                    x.m.storageType as storageType, \
                                                                                                    x.m.storageDate as storageDate) as m) as invoiceStoreAttributes) as m)) as l) \
                                                                                            as generatedDocumentDetailsList, \
                                                                        x.m.otherAttributes as documentTags, \
                                                                        x.m.documentExchangeDetailsDO as documentExchangeDetailsList, \
                                                                        x.m.rawDataStorageDetailsList as rawDocumentDetailsList, \
                                                                        x.m.documentConsumers as documentConsumerList, \
                                                                        x.m.documentIdentifiers as documentIdentifierList \
                                                                      ) as m \
                                                             ) \
                                     ) as l \
                            )'
        #return the transformed dataframe
        return transactionsDataframe.withColumn("results", f.expr(expression))
        
    '''
    The method retains only the rows in the transactions dataframe where state is COMPLETE
    Input:
        transactionsDataframe: The dataframe whose schema needs to be changed
    Output:
        The dataframe with only those rows having state as COMPLETE
    '''
    def retainRowsWithStateAsComplete(transactionsDataframe):
        if "state" not in transactionsDataframe.columns:
            return
        #filter the dataframe
        return transactionsDataframe.filter('state IS NOT NULL and state.s == "COMPLETE"')

    '''
    Retain only the rows with state as Complete
    '''
    transactionsDataframe = retainRowsWithStateAsComplete(transactionsDataframe)
    
    '''
    Drop storage attributes
    Change schema of storageAttributesList
    Change Nested column names in results
    '''
    transactionsDataframe = changeResultsColumnSchema(transactionsDataframe)
    
    '''
    change workflowId schema
    '''
    transactionsDataframe = changeWorkflowIdSchema(transactionsDataframe)
    
    '''
    concatenate useCaseId and version
    '''
    transactionsDataframe = changeUseCaseIdSchema(transactionsDataframe)

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
    transactionsDataframe = changeOuterColumnNames(transactionsDataframe, outerColumnMapping)
    
    #return the transformed dataframe
    return transactionsDataframe

'''
The method writes Ip-metadata dataframe to s3.
Input: 
    ipMetaDataframe: Pyspark dataframe containing tranformed transactions data
    s3WritePath: The path to the s3 bucket where the dataframe is to be written 
'''
def writeData(ipMetadataDataframe, s3WritePath):
    
    #Initialize logs
    logs_ = ""
    
    try:
        #write the dataframe to s3 location specified by s3WritePath
        start_time = time()
        ipMetadataDataframe.write.mode("append").json(s3WritePath)
        end_time = time()
        logs_ += "Write success!\n" + "Duration: " + str(end_time - start_time) + "\n"
    except Exception as e:
        logs_ += "Write Failed!\n" + str(e) + "\n"
    
    #write logs to cloudwatch management console. Visibile in AWS glue console.
    print("=====Write Logs=====\n" + str(logs_))

'''
EXTRACT DATA:
    Read transactions data from s3.
    The parameters glueDatabase and glueTable need to be specified before executing the job
'''

glueDatabase = "2020_06_23_08_19_12"
glueTable = "internship-project-one-database"
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

'''
The script reads transactions data from the glue data catalog, transforms its schema and writes it back to S3.
Method for reading: readData
Method for tranforming schema: tranformSchema
Method for writing: writeData
'''
#Import Python modules
import sys
from time import time

#Import PySpark modules
from pyspark.context import SparkContext
from pyspark.sql.functions import struct, col, when, lit, concat, expr
from pyspark.sql.types import StructType, ArrayType

#Import Glue modules
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

#Initialize Glue context
sparkContext = SparkContext.getOrCreate()
glueContext = GlueContext(sparkContext)

'''
The method reads transactions data from the glue table in glue database, into a glue dynamic frame.
Converts the glue dynamic frame to a PySpark dataframe. The rows having state as complete are filtered.
If reading fails program is terminated.
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
        #Convert glue dynamic frame to spark data frame to use standard pyspark functions and retain rows with state as complete
        return glueDynamicFrame.toDF().filter('state.s == "COMPLETE"')
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
            return transactionsDataframe.withColumn("workflowId", when(col("workflowId").isNotNull(), struct(struct(struct("workflowId.m").alias("generateInvoiceGraph")).alias("m"))).otherwise(lit(None)))
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
        return transactionsDataframe.withColumn("useCaseId", struct(concat(col("useCaseId.s"), lit(":1")).alias("s")))
    
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
    def changeResultsColumnSchema(transactionsDataframe, nestedColumnMappingInResults):
        '''
        The functions returns the list of paths to all the nested fields in a schema. 
        All paths are returned by applying Depth First Search on the schema tree.
        For e.g. some of the entries in the paths list could be:
                            ['results.l', 'm.storageAttributesList.l', 'm.retentionPeriodInDays.n']
                            ['results.l', 'm.documentIdentifiers.l', 'm.source.s']
                            ['results.l', 'm.storageAttributesList.l', 'm.storageTypeSpecificAttributes.m.MIME_TYPE.s']
                      A path is broken whenever an array type column is encountered as seen in the above example.
        Input:
            schema: The schema to be traversed
            schemaName: The alias of the schema
        Output:
            The list of paths in the schema
        '''
        def getAllPathsInSchema(schema, schemaName = ""):
            #initailize paths list which will store paths to all the nested fields in schema
            paths = []
            #schema is of structType
            if isinstance(schema, StructType):
                #no StructField inside StructType
                if len(schema.fields) == 0:
                    return [[schemaName]]
                #get paths to nested fields from all StructFields in StructType
                for field in schema.fields:
                    for child in getAllPathsInSchema(field.dataType, field.name):
                        #concatenate the schemName and first entry in each path by '.' and append to paths list
                        paths.append([("" if schemaName == "" else schemaName + ".") + child[0]] + child[1:])
            #schema is of ArrayType
            elif isinstance(schema, ArrayType):
                #get paths to all nested fields in elementType
                for child in getAllPathsInSchema(schema.elementType):
                    #add the schemaName to each path
                    paths.append([schemaName] + child)
            #schema is StringType, etc.
            else:
                return [[schemaName]]
            #return all possible paths
            return paths
    
        #check if results exists
        if "results" not in transactionsDataframe.columns:
            return transactionsDataframe
        
        #get all possible paths to columns in dataframe
        paths = getAllPathsInSchema(transactionsDataframe.select("results").schema)
        #check results list is empty in dataframe
        if len(paths) == 1 and len(paths[0]) == 2 and paths[0][1] == '':
            return transactionsDataframe
        
        #initialize set for storage of transformation logic of nested columns in results
        transformationLogicOfNestedColumnsInResults = set()
        
        #build transformation logic for storageAttributesList
        transformationLogicOfStorageAttributesList = ''
        for path in paths:
            if "m.storageAttributesList.l" in path:
                #check if storageAttributesList schema is empty in the dataframe
                if path[2] == '':
                    transformationLogicOfNestedColumnsInResults.add('x.m.storageAttributesList as generatedDocumentDetailsList')
                    break
                #build the expression for nested columns in storageAttributesList
                else:
                    helperList = path[2].split('.')
                    if helperList[-1] != "m":
                        transformationLogicOfStorageAttributesList += 'x.' + '.'.join(helperList[:-1]) + ' as ' + helperList[-2] + ','
                    
        if transformationLogicOfStorageAttributesList != '':
            transformationLogicOfStorageAttributesList = 'CASE WHEN x.m.storageAttributesList IS NOT NULL THEN \
                                                                        struct(transform(x.m.storageAttributesList.l, \
                                                                                x -> struct(struct(struct(struct(' + \
                                                                                      transformationLogicOfStorageAttributesList[:-1] + \
                                                                                     ') as m) as invoiceStoreAttributes) as m)) as l) ELSE NULL END \
                                                           as generatedDocumentDetailsList'
            transformationLogicOfNestedColumnsInResults.add(transformationLogicOfStorageAttributesList)
        
        #build transform expression for remaining columns
        for path in paths:
            helperList = path[1].split('.')
            if "storageAttributes" not in helperList and "storageAttributesList" not in helperList:
                transformationLogic = 'x.' + helperList[0] + '.' + helperList[1] + ' as ' + nestedColumnMappingInResults[helperList[1]]
                transformationLogicOfNestedColumnsInResults.add(transformationLogic)
        
        #build transform expression for results column
        transformExpression = ''
        for transformationLogic in transformationLogicOfNestedColumnsInResults:
            transformExpression += transformationLogic + ','
        transformExpression = 'struct(transform(results.l, x -> struct(struct(' + transformExpression[:-1] + ') as m )) as l)'

        #return dataframe with transformed results column
        return transactionsDataframe.withColumn("results", expr(transformExpression))
    
    '''
    Drop storage attributes
    Change schema of storageAttributesList
    Change Nested column names in results
    '''

    nestedColumnMappingInResults = {}
    nestedColumnMappingInResults['documentExchangeDetailsDO'] = 'documentExchangeDetailsList'
    nestedColumnMappingInResults['rawDataStorageDetailsList'] = 'rawDocumentDetailsList'
    nestedColumnMappingInResults['documentConsumers'] = 'documentConsumerList'
    nestedColumnMappingInResults['documentIdentifiers'] = 'documentIdentifierList'
    nestedColumnMappingInResults['storageAttributesList'] = 'generatedDocumentDetailsList'
    nestedColumnMappingInResults['otherAttributes'] = 'documentTags'
    transactionsDataframe = changeResultsColumnSchema(transactionsDataframe, nestedColumnMappingInResults)
    
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
        #write the dataframe to s3 location specified by s3WritePath. Change the mode to append/overwrite as per the usecase
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

glueDatabase = "internship-project-two-database"
glueTable = "2020_07_04_17_44_14"
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

s3WritePath = "s3://internship-project-two/ip-metadata/"
writeData(ipMetadataDataframe, s3WritePath)

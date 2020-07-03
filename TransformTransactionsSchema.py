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
from pyspark.sql.types import StructType, ArrayType

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
    def changeResultsColumnSchema(transactionsDataframe, nestedColumnMappingInResults):
        '''
        The functions returns all the possible paths in a schema, in a list format. 
        All paths are returned by applying Depth First Search on the schema tree.
        For e.g. some of the entries in the paths list could be:
                            ['results.l', 'm.storageAttributesList.l', 'm.retentionPeriodInDays.n']
                            ['results.l', 'm.documentIdentifiers.l', 'm.source.s']
                            ['results.l', 'm.storageAttributesList.l', 'm.storageTypeSpecificAttributes.m.MIME_TYPE.s']
                      The path is broken whenever an array type column is encountered as seen in the above example.
        Input:
            schema: The schema to be traversed
            fieldName: The fieldName of the schema
        Output:
            The list of paths in the schema
        '''
        def getAllPathsInSchema(schema, fieldName = ""):
            #initailize paths list which will store all paths in schema
            paths = []
            #schema is of structType
            if isinstance(schema, StructType):
                if len(schema.fields) == 0:
                    return [[fieldName]]
                #get paths from all struct fields in struct type
                for field in schema.fields:
                    for child in getAllPathsInSchema(field.dataType, field.name):
                        paths.append([("" if fieldName == "" else fieldName + ".") + child[0]] + child[1:])
            #schema is of ArrayType
            elif isinstance(schema, ArrayType):
                for child in getAllPathsInSchema(schema.elementType):
                    paths.append([fieldName] + child)
            #schema is string, number etc.
            else:
                return [[fieldName]]
                
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
                #check if list is empty
                if path[2] == '':
                    transformationLogicOfNestedColumnsInResults.add('x.m.storageAttributesList as generatedDocumentDetailsList')
                    break
                #build denest expression for columns in storageAttributesList
                else:
                    helperList = path[2].split('.')
                    transformationLogicOfStorageAttributesList += 'x.' + '.'.join(helperList[:-1]) + ' as ' + helperList[-2] + ','
                    
        if transformationLogicOfStorageAttributesList != '':
            transformationLogicOfStorageAttributesList = 'struct(transform(x.m.storageAttributesList.l, \
                                                                                x -> struct(struct(struct(struct(' + \
                                                                                      transformationLogicOfStorageAttributesList[:-1] + \
                                                                                     ') as m) as invoiceStoreAttributes) as m)) as l) as generatedDocumentDetailsList'
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
        return transactionsDataframe.withColumn("results", f.expr(transformExpression))
        
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

glueDatabase = "internship-project-one-database"
glueTable = "2020_06_23_08_19_12"
transactionsDataframe = readData(glueDatabase, glueTable)
transactionsDataframe.printSchema()
'''
TRANSFORM DATA:
    Transform the transactionsDataframe
'''
ipMetadataDataframe = transformSchema(transactionsDataframe)
ipMetadataDataframe.printSchema()
'''
#LOAD DATA
    load ipMetadataDataframe to s3.
    The parameter s3WritePath needs to be specified before executing the job
'''

s3WritePath = "s3://internship-project-one/"
writeData(ipMetadataDataframe, s3WritePath)

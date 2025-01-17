# Welcome to your new notebook
# Type here in the cell editor to add code!
import numpy as np
from pyspark.sql.functions import col, when, concat, lit, format_string,sum, upper, substring, expr, current_date, current_timestamp,to_timestamp,concat_ws, isnull, date_format, asc, trim, trunc, date_sub, year,coalesce, count, countDistinct, min, max
from pyspark.sql.types import IntegerType, DecimalType, StringType, LongType, TimestampType, StructType, StructField, DoubleType, FloatType
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor
# from tabulate import tabulate

spark = SparkSession.builder\
        .appName("utils")\
        .getOrCreate()
        
class AuditLog:
    
    def __init__(self, WS_ID: str, TABLE_NAME_to_check:str, AUDIT_TABLE_NAME:str, LH_ID_to_check: str, LH_ID_audit: str = None, schema: str = None):
        '''
        if `LH_ID_audit` is not given, it is  LH_ID_to_check automatically, i.e. audit table is in the same lakehouse as that of
        '''
        self.WS_ID = WS_ID
        self.TABLE_NAME_to_check = TABLE_NAME_to_check
        self.AUDIT_TABLE_NAME = AUDIT_TABLE_NAME
        self.LH_ID_to_check = LH_ID_to_check
        self.LH_ID_audit = LH_ID_audit if LH_ID_audit else LH_ID_to_check
        self.schema = schema
        
        self.PATH_TO_AUDIT_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_audit}/Tables/{self.TABLE_NAME_to_check}'
        self.PATH_TO_CHECKED_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_audit}/Tables/{self.TABLE_NAME_to_check}'
        
    def initialDetail(self,  pipelineName: str, pipelineId, TriggerType, TableName, functionName, ):
        self.log = {
            'PIPELINENAME':pipelineName, #
            'PIPELINERUNID':pipelineId, #
            'STARTTIME':datetime.utcnow() + timedelta(hours=7),
            'ENDTIME':None,
            'TRIGGERTYPE':TriggerType, #
            'AUDITKEY':None,
            'TABLE_NAME':TableName,
            'STATUS_ACTIVITY':'Not start',
            'FUNCTION_NAME':functionName,
            'COUNTROWSBEFORE':None,
            'COUNTROWSAFTER':None,
            'ERRORCODE':None,
            'ERRORMESSAGE':None
        }






def fillNaAll(df):
    fillnaDefault = {
        StringType: 'NA',
        IntegerType: 0,
        DoubleType: 0.0,
        TimestampType: '1970-01-01 00:00:00',
        LongType:0,
        DecimalType:0.0,
    }
    fill_values = {}
    for field in df.schema.fields:
        field_type = type(field.dataType)
        if field_type in fillnaDefault:
            fill_values[field.name] = fillnaDefault[field_type]
        else:
            raise TypeError(f'No fill value defined for type {field_type}')
    return df.fillna(fill_values)

def fillNaAllStringType(df):
    fillnaDefault = {
        StringType: 'NA',
    }
    fill_values = {}
    for field in df.schema.fields:
        field_type = type(field.dataType)
        if field_type in fillnaDefault:
            fill_values[field.name] = fillnaDefault[field_type]
    return df.fillna(fill_values)

def copySchemaByName(df,fromDf):
    '''
    schemaField can be get by df.schema.fields
    '''
    dfCol = df.columns
    for column in fromDf.schema.fields:
        if column.name in dfCol: # [] TODO: don't forget to add to other Notebook
            df = df.withColumn(column.name,col(column.name).cast(column.dataType))
    return df

def getSetColumn(df,columnName):
    return set(df.select(columnName).toPandas()[columnName])

def getCountColumn(df,columnName):
    return df.select(columnName).toPandas()[columnName].value_counts()

def trackSizeTable(df,detail=None,schema = False,table=False):
    if detail:
        print(detail,end=': size = ')
    
    numrow = df.count()
    print(f'({numrow}, {len(df.columns)})')
    
    if schema:
        df.printSchema()
    
    if table:
        df.show()

def trackSizeOnLake(tablePath):
    numRow = spark.sql(
    f"""
    SELECT COUNT(*) FROM {tablePath}
    """).collect()[0][0]

    numCol = spark.sql(
    f"""
       DESCRIBE {tablePath}
    """).count()

    print(tablePath,end=': size = ')
    print(f'({numRow}, {numCol})')

def getSizeOnLake(tablePath):
    numRow = spark.sql(
    f"""
    SELECT COUNT(*) FROM {tablePath}
    """).collect()[0][0]

    numCol = spark.sql(
    f"""
       DESCRIBE {tablePath}
    """).count()

    return numRow, numCol

class _AuditLog:
    '''
    Usage:
    (1) At first: create audit object
            >>> audit = utils.AuditLog(MODULE_NAME='...',EXCECUTION_BY='Notebook:...', TABLE_NAME='...',ARTIFACT=...)
            >>> audit = utils.AuditLog(MODULE_NAME='Siebel',EXCECUTION_BY='Notebook:Siebel_DimSiebelMerchandise', TABLE_NAME='SilverLH.dimsiebelmerchandise',ARTIFACT='BronzeLH_Siebel.stagingsiebelmerchandise')
        //At first, pre-log is written at file "auditbefore.csv" to track start process (will not be deleted)
    (2) count row before:
            >>> audit.setRowCountBefore(table.count())
            >>> audit.setRowCountBefore(utils.getSizeOnLake('...')[0])
    (3) After transformation:
            >>> audit.setRowCountAfter(utils.getSizeOnLake('....')[0])
    (4) ending
            >>> audit.endAuditLog()
    '''
    def printAsTable(self):
        table = [(key, value) for key, value in self.log.items()]
        for row in table:
            print(f'{row[0]:17s}:\t{row[1]}')

    def printAsTableStart(self):
        table = [(key, value) for key, value in self.log.items() if key in ['AUDITDATE','MODULE_NAME','EXCECUTION_BY','START_TIME','TABLE_NAME']]
        for row in table:
            print(f'{row[0]:17s}:\t{row[1]}')

    def __init__(self,MODULE_NAME,EXCECUTION_BY,TABLE_NAME,ARTIFACT=None,additional=None):
        utc_now = datetime.utcnow() # Get the current UTC time
        utc_plus_7 = utc_now + timedelta(hours=7) # Convert to UTC+7
        formatted_timestamp = utc_plus_7.strftime('%Y-%m-%d %H:%M:%S') #yyyy-MM-dd HH:mm:ss # Format the timestamp
        self.log = {}
        self.log['AUDITDATE'] = utc_plus_7.strftime('%Y%m%d')
        self.log['MODULE_NAME'] = MODULE_NAME
        self.log['PACKAGE_NAME'] = None #
        self.log['EXCECUTION_BY'] = EXCECUTION_BY
        self.log['START_TIME'] = formatted_timestamp
        self.log['TABLE_NAME'] = TABLE_NAME #
        self.log['ROW_COUNT_BEFORE'] = None
        self.log['END_TIME'] = None #
        self.log['ROW_COUNT_AFTER'] = None # 
        self.log['STATUS'] = 'Fail' #
        self.log['AUDIT_KEY'] = MODULE_NAME + TABLE_NAME + utc_plus_7.strftime('%Y%m%d%H%M%S')
        self.log['ARTIFACT'] = ARTIFACT

        if additional:
            assert isinstance(additional,dict), 'please mapper like dictionary':None,
            for key, val in additional:
                self.log[key]=val
        order = ['AUDITDATE','AUDIT_KEY', 'MODULE_NAME', 'PACKAGE_NAME', 'EXCECUTION_BY', 'START_TIME', 'TABLE_NAME','ARTIFACT']
        line = ','.join([self.log[key] if self.log[key] else '' for key in order])
        with open('/lakehouse/default/Files/logfile/auditbefore.csv', 'a') as f:
            f.write(line+'\n')
        self.printAsTableStart()
        
    def add(self,key,val):
        self.log[key] = val
    
    def get(self, key):
        return self.log.get(key,None)

    def setRowCountBefore(self,count):
        self.log['ROW_COUNT_BEFORE'] = str(count)

    def setRowCountAfter(self,count):
        self.log['ROW_COUNT_AFTER'] = str(count)

    def endAuditLog(self):
        utc_now = datetime.utcnow()
        utc_plus_7 = utc_now + timedelta(hours=7)
        formatted_timestamp = utc_plus_7.strftime('%Y-%m-%d %H:%M:%S') #yyyy-MM-dd HH:mm:ss
        self.log['END_TIME'] = formatted_timestamp
        self._writeSuccess()
        self.printAsTable()

    def getAuditLog(self):
        return self.log

    def _writeFail(self):
        self.log['STATUS'] = 'fail':None,
        order = ['AUDITDATE','AUDIT_KEY', 'MODULE_NAME', 'EXCECUTION_BY', 'START_TIME', 'TABLE_NAME','ARTIFACT', 'ROW_COUNT_BEFORE', 'END_TIME', 'ROW_COUNT_AFTER', 'STATUS']
        line = ','.join([self.log[key] if self.log[key] else '' for key in order])
        with open('/lakehouse/default/Files/logfile/auditlog.csv', 'a') as f:
            f.write(line+'\n')

    def _writeSuccess(self):
        self.log['STATUS'] = 'finish':None,
        order = ['AUDITDATE','AUDIT_KEY', 'MODULE_NAME', 'EXCECUTION_BY', 'START_TIME', 'TABLE_NAME','ARTIFACT', 'ROW_COUNT_BEFORE', 'END_TIME', 'ROW_COUNT_AFTER', 'STATUS']
        line = ','.join([self.log[key] if self.log[key] else '' for key in order])
        with open('/lakehouse/default/Files/logfile/auditlog.csv', 'a') as f:
            f.write(line+'\n')

class AuditLog:
    '''
    Usage:
    (1) At first: create audit object
            >>> audit = utils.AuditLog(MODULE_NAME='...',EXCECUTION_BY='Notebook:...', TABLE_NAME='...',ARTIFACT=...)
            >>> audit = utils.AuditLog(MODULE_NAME='Siebel',EXCECUTION_BY='Notebook:Siebel_DimSiebelMerchandise', TABLE_NAME='SilverLH.dimsiebelmerchandise',ARTIFACT='BronzeLH_Siebel.stagingsiebelmerchandise')
        //At first, pre-log is written at file "auditbefore.csv" to track start process (will not be deleted)
    (2) count row before:
            >>> audit.setRowCountBefore(table.count())
            >>> audit.setRowCountBefore(utils.getSizeOnLake('...')[0])
    (3) After transformation:
            >>> audit.setRowCountAfter(utils.getSizeOnLake('....')[0])
    (4) ending
            >>> audit.endAuditLog()
    '''
    def printAsTable(self):
        table = [(key, value) for key, value in self.log.items()]
        for row in table:
            print(f'{row[0]:17s}:\t{row[1]}')

    def printAsTableStart(self):
        table = [(key, value) for key, value in self.log.items() if key in ['AUDITDATE','MODULE_NAME','EXCECUTION_BY','START_TIME','TABLE_NAME']]
        for row in table:
            print(f'{row[0]:17s}:\t{row[1]}')

    def __init__(self,MODULE_NAME,EXCECUTION_BY,TABLE_NAME,ARTIFACT=None,additional=None):
        utc_now = datetime.utcnow() # Get the current UTC time
        utc_plus_7 = utc_now + timedelta(hours=7) # Convert to UTC+7
        formatted_timestamp = utc_plus_7.strftime('%Y-%m-%d %H:%M:%S') #yyyy-MM-dd HH:mm:ss # Format the timestamp
        self.log = {}
        self.log['AUDITDATE'] = utc_plus_7.strftime('%Y%m%d')
        self.log['MODULE_NAME'] = MODULE_NAME
        self.log['PACKAGE_NAME'] = None #
        self.log['EXCECUTION_BY'] = EXCECUTION_BY
        self.log['START_TIME'] = formatted_timestamp
        self.log['TABLE_NAME'] = TABLE_NAME #
        self.log['ROW_COUNT_BEFORE'] = None
        self.log['END_TIME'] = None #
        self.log['ROW_COUNT_AFTER'] = None # 
        self.log['STATUS'] = 'Fail' #
        self.log['AUDIT_KEY'] = MODULE_NAME + TABLE_NAME + utc_plus_7.strftime('%Y%m%d%H%M%S')
        self.log['ARTIFACT'] = ARTIFACT

        if additional:
            assert isinstance(additional,dict), 'please mapper like dictionary':None,
            for key, val in additional:
                self.log[key]=val
        # order = ['AUDITDATE','AUDIT_KEY', 'MODULE_NAME', 'PACKAGE_NAME', 'EXCECUTION_BY', 'START_TIME', 'TABLE_NAME','ARTIFACT']
        # line = ','.join([self.log[key] if self.log[key] else '' for key in order])
        # with open('/lakehouse/default/Files/logfile/auditbefore.csv', 'a') as f:
        #     f.write(line+'\n')
        self.printAsTableStart()
        
    def add(self,key,val):
        self.log[key] = val
    
    def get(self, key):
        return self.log.get(key,None)

    def setRowCountBefore(self,count):
        self.log['ROW_COUNT_BEFORE'] = str(count)

    def setRowCountAfter(self,count):
        self.log['ROW_COUNT_AFTER'] = str(count)

    def endAuditLog(self):
        utc_now = datetime.utcnow()
        utc_plus_7 = utc_now + timedelta(hours=7)
        formatted_timestamp = utc_plus_7.strftime('%Y-%m-%d %H:%M:%S') #yyyy-MM-dd HH:mm:ss
        self.log['END_TIME'] = formatted_timestamp
        self._writeSuccess()
        self.printAsTable()

    def getAuditLog(self):
        return self.log

    def _writeFail(self):
        self.log['STATUS'] = 'fail':None,
        # order = ['AUDITDATE','AUDIT_KEY', 'MODULE_NAME', 'EXCECUTION_BY', 'START_TIME', 'TABLE_NAME','ARTIFACT', 'ROW_COUNT_BEFORE', 'END_TIME', 'ROW_COUNT_AFTER', 'STATUS']
        # line = ','.join([self.log[key] if self.log[key] else '' for key in order])
        # with open('/lakehouse/default/Files/logfile/auditlog.csv', 'a') as f:
        #     f.write(line+'\n')

    def _writeSuccess(self):
        self.log['STATUS'] = 'finish':None,
        # order = ['AUDITDATE','AUDIT_KEY', 'MODULE_NAME', 'EXCECUTION_BY', 'START_TIME', 'TABLE_NAME','ARTIFACT', 'ROW_COUNT_BEFORE', 'END_TIME', 'ROW_COUNT_AFTER', 'STATUS']
        # line = ','.join([self.log[key] if self.log[key] else '' for key in order])
        # with open('/lakehouse/default/Files/logfile/auditlog.csv', 'a') as f:
        #     f.write(line+'\n')

def savetable(df, sinkPath='UATQueryResult.onFabric'):
    df.select(['index','Table','Column','KeyCheck','KeyGroupby','groupbyValue','valueOnFabric','dateCheck',]).withColumn('valueOnFabric', col('valueOnFabric').cast(FloatType())).write.mode('append').saveAsTable(sinkPath)

def runQuerySpark_byRow(df,idx, Table, Column, KeyCheck, groupbyKey, cTime, additionalSQLFilter = None):
    # Start with the base DataFrame
    if additionalSQLFilter:
        df = df.filter(expr(additionalSQLFilter))
    else:
        additionalSQLFilter = ''

    if KeyCheck == 'countrow':
        result = df.selectExpr("cast(count(*) AS FLOAT) AS valueOnFabric")\
                   .withColumn("index", lit(idx))\
                   .withColumn("Table", lit(Table))\
                   .withColumn("Column", lit(Column))\
                   .withColumn("KeyCheck", lit(KeyCheck))\
                   .withColumn("KeyGroupby", lit(groupbyKey))\
                   .withColumn("groupbyValue", lit(''))\
                   .withColumn('dateCheck', lit(cTime))\
                   .withColumn('filter', lit(additionalSQLFilter))
    elif KeyCheck == 'countby':
        result = df.groupBy(groupbyKey)\
                    .agg(count('*').alias('valueOnFabric'))\
                    .withColumn("index", lit(idx))\
                    .withColumn("Table", lit(Table))\
                    .withColumn("Column", lit(Column))\
                    .withColumn("KeyCheck", lit(KeyCheck))\
                    .withColumn("KeyGroupby", lit(groupbyKey))\
                    .withColumn("groupbyValue", col(groupbyKey).cast("string"))\
                    .withColumn('dateCheck',lit(cTime))\
                    .drop(groupbyKey)\
                    .withColumn('filter', lit(additionalSQLFilter))
    elif KeyCheck == 'countdistinctby':
        result = df.groupBy(groupbyKey)\
                    .agg(countDistinct(Column).alias('valueOnFabric'))\
                    .withColumn("index", lit(idx))\
                    .withColumn("Table", lit(Table))\
                    .withColumn("Column", lit(Column))\
                    .withColumn("KeyCheck", lit(KeyCheck))\
                    .withColumn("KeyGroupby", lit(groupbyKey))\
                    .withColumn("groupbyValue", col(groupbyKey).cast("string"))\
                    .withColumn('dateCheck',lit(cTime))\
                    .drop(groupbyKey)\
                    .withColumn('filter', lit(additionalSQLFilter))
    elif KeyCheck == 'distinct':
        result = df.agg(countDistinct(Column).alias('valueOnFabric'))\
                   .withColumn("index", lit(idx))\
                   .withColumn("Table", lit(Table))\
                   .withColumn("Column", lit(Column))\
                   .withColumn("KeyCheck", lit(KeyCheck))\
                   .withColumn("KeyGroupby", lit(groupbyKey))\
                   .withColumn("groupbyValue", lit(''))\
                   .withColumn('dateCheck', lit(cTime))\
                   .withColumn('filter', lit(additionalSQLFilter))
    elif KeyCheck == 'countnonnull':
        result = df.filter(col(Column).isNotNull()).agg(count('*').alias('valueOnFabric'))\
                   .withColumn("index", lit(idx))\
                   .withColumn("Table", lit(Table))\
                   .withColumn("Column", lit(Column))\
                   .withColumn("KeyCheck", lit(KeyCheck))\
                   .withColumn("KeyGroupby", lit(''))\
                   .withColumn("groupbyValue", lit(''))\
                   .withColumn('dateCheck', lit(cTime))\
                   .withColumn('filter', lit(additionalSQLFilter))
    elif KeyCheck == 'firstdate':
        result = df.filter(col(Column).isNotNull()).agg(min(date_format(Column, 'yyyyMMdd')).alias('valueOnFabric'))\
                   .withColumn("index", lit(idx))\
                   .withColumn("Table", lit(Table))\
                   .withColumn("Column", lit(Column))\
                   .withColumn("KeyCheck", lit(KeyCheck))\
                   .withColumn("KeyGroupby", lit(''))\
                   .withColumn("groupbyValue", lit(''))\
                   .withColumn('dateCheck', lit(cTime))\
                   .withColumn('filter', lit(additionalSQLFilter))
    elif KeyCheck == 'lastdate':
        result = df.filter(col(Column).isNotNull()).agg(max(date_format(Column, 'yyyyMMdd')).alias('valueOnFabric'))\
                   .withColumn("index", lit(idx))\
                   .withColumn("Table", lit(Table))\
                   .withColumn("Column", lit(Column))\
                   .withColumn("KeyCheck", lit(KeyCheck))\
                   .withColumn("KeyGroupby", lit(''))\
                   .withColumn("groupbyValue", lit(''))\
                   .withColumn('dateCheck', lit(cTime))\
                   .withColumn('filter', lit(additionalSQLFilter))
    elif KeyCheck == 'max':
        result = df.filter(col(Column).isNotNull()).agg(max(Column).alias('valueOnFabric'))\
                   .withColumn("index", lit(idx))\
                   .withColumn("Table", lit(Table))\
                   .withColumn("Column", lit(Column))\
                   .withColumn("KeyCheck", lit(KeyCheck))\
                   .withColumn("KeyGroupby", lit(''))\
                   .withColumn("groupbyValue", lit(''))\
                   .withColumn('dateCheck', lit(cTime))\
                   .withColumn('filter', lit(additionalSQLFilter))
    elif KeyCheck == 'min':
        result = df.filter(col(Column).isNotNull()).agg(min(Column).alias('valueOnFabric'))\
                   .withColumn("index", lit(idx))\
                   .withColumn("Table", lit(Table))\
                   .withColumn("Column", lit(Column))\
                   .withColumn("KeyCheck", lit(KeyCheck))\
                   .withColumn("KeyGroupby", lit(''))\
                   .withColumn("groupbyValue", lit(''))\
                   .withColumn('dateCheck', lit(cTime))\
                   .withColumn('filter', lit(additionalSQLFilter))
    elif KeyCheck == 'sum':
        result = df.agg(sum(Column).alias('valueOnFabric'))\
                   .withColumn("index", lit(idx))\
                   .withColumn("Table", lit(Table))\
                   .withColumn("Column", lit(Column))\
                   .withColumn("KeyCheck", lit(KeyCheck))\
                   .withColumn("KeyGroupby", lit(''))\
                   .withColumn("groupbyValue", lit(''))\
                   .withColumn('dateCheck', lit(cTime))\
                   .withColumn('filter', lit(additionalSQLFilter))
    elif KeyCheck == 'sumby':
        result = df.groupBy(groupbyKey)\
                    .agg(sum(Column).alias('valueOnFabric'))\
                    .withColumn("index", lit(idx))\
                    .withColumn("Table", lit(Table))\
                    .withColumn("Column", lit(Column))\
                    .withColumn("KeyCheck", lit(KeyCheck))\
                    .withColumn("KeyGroupby", lit(groupbyKey))\
                    .withColumn("groupbyValue", col(groupbyKey).cast("string"))\
                    .withColumn('dateCheck',lit(cTime))\
                    .drop(groupbyKey)\
                    .withColumn('filter', lit(additionalSQLFilter))

    #special exception
    # print(groupbyKey)
    if groupbyKey.lower() == 'saledate':
        result = result.withColumn('groupbyValue', date_format(col("groupbyValue").cast(TimestampType()), 'yyyyMMdd'))

    return result


def runQuerySpark_TableName(TableName, referenceTable,LakehouseName='SilverLH', sinkPath='UATQueryResult.onFabric', saveResult=True):
    referenceTable_filter = referenceTable[referenceTable['Table'].apply(lambda x: x.lower())==TableName.lower()]
    df = spark.table(LakehouseName+'.'+TableName)

    results = []

    for rowIdx in tqdm(referenceTable_filter.index, desc=TableName,leave=True):
        resultRow = runQuerySpark_byRow(df,*referenceTable_filter.loc[rowIdx])
        results.append(resultRow)
    
    result = results[0]
    for r in results[1:]:
        result = result.unionByName(r)
    
    if saveResult:
        savetable(result,sinkPath)

    return result

def runQuerySpark(referenceTable,LakehouseName='SilverLH', sinkPath='UATQueryResult.onFabric', saveResult=True,max_workers=4):

    allTable = referenceTable['Table'].unique()
    numtable = len(allTable)
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as p:
        results = list(p.map(runQuerySpark_TableName,allTable,[referenceTable]*numtable,[LakehouseName]*numtable,[sinkPath]*numtable,[saveResult]*numtable))
    
    result = results[0]
    for r in results[1:]:
        result = result.unionByName(r)
    return result.withColumn('valueOnFabric',col('valueOnFabric').cast(FloatType()))
    
class TrackSize:
    def __init__(self):
        # self.WS_ID = WS_ID
        # self.LH_ID = LH_ID
        self.dictTrack = {}
        self.step = 1
    
    def log(self,tableName,size):
        tableName = tableName.lower()
        if tableName not in self.dictTrack.keys():
            self.dictTrack[tableName] = {str(self.step):size}
        else:
            self.dictTrack[tableName][str(self.step)]=size
        self.step += 1

    def __str__(self):
        # Create a string representation of the dictionary keys
        output = []
        for key in self.dictTrack:
            output.append(f"{key}: {self.dictTrack[key]}")
        return "{\n"+"\t\n".join(output)+"\n}"




















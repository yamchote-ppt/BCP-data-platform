# Welcome to your new notebook
# Type here in the cell editor to add code!
import numpy as np
from pyspark.sql.functions import col, when, concat, lit, format_string,sum, upper, substring, expr, current_date, current_timestamp,to_timestamp,concat_ws, isnull, date_format, asc, trim, trunc, date_sub, year,coalesce, count, countDistinct, min, max
from pyspark.sql.types import IntegerType, DecimalType, StringType, LongType, TimestampType, StructType, StructField, DoubleType, FloatType
import pandas as pd
from pyspark.sql import Row
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from tqdm.auto import tqdm
from concurrent.futures import ThreadPoolExecutor
from typing import Union, List, Tuple, Any
import notebookutils
# from tabulate import tabulate

spark = SparkSession.builder\
        .appName("utils")\
        .getOrCreate()

class AuditLog_Fusion:

    class logger:
        def __init__(self, **kwargs):
            self._data = kwargs
    
        def __getitem__(self, key):
            return self._data[key]
    
        def __setitem__(self, key, value):
            if key not in self._data:
                raise KeyError(f"Cannot add new key: {key}")
            self._data[key] = value
    
        def __delitem__(self, key):
            raise KeyError(f"Cannot delete key: {key}")
    
        def __iter__(self):
            return iter(self._data)
    
        def __len__(self):
            return len(self._data)
    
        def __repr__(self):
            return repr(self._data)
    
    def __init__(self, columns: Union[List[str], Tuple[str, ...]], WS_ID: str, TABLE_NAME_to_check:str, AUDIT_TABLE_NAME:str, LH_ID_to_check: str, LH_ID_audit: str = None, schema: str = None):
        '''
        - if `LH_ID_audit` is not given, it is  LH_ID_to_check automatically, i.e. audit table is in the same lakehouse as that of
        - if using lakehouse with Schema, please provide `schema` parameter
        '''
        self.WS_ID = WS_ID
        self.TABLE_NAME_to_check = TABLE_NAME_to_check
        self.AUDIT_TABLE_NAME = AUDIT_TABLE_NAME
        self.LH_ID_to_check = LH_ID_to_check
        self.LH_ID_audit = LH_ID_audit if LH_ID_audit else LH_ID_to_check
        self.schema = schema
        self.fixColumns = {'STARTTIME','ENDTIME','AUDITKEY','STATUS_ACTIVITY'}
        self.columns = tuple(set(columns).union(self.fixColumns))
        
        if self.schema:    
            self.PATH_TO_AUDIT_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_audit}/Tables/{self.schema}/{self.AUDIT_TABLE_NAME}'
            self.PATH_TO_CHECKED_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_to_check}/Tables/{self.schema}/{self.TABLE_NAME_to_check}'
        else:
            self.PATH_TO_AUDIT_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_audit}/Tables/{self.AUDIT_TABLE_NAME}'
            self.PATH_TO_CHECKED_TABLE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_to_check}/Tables/{self.TABLE_NAME_to_check}'
        
        if not notebookutils.fs.exists(self.PATH_TO_AUDIT_TABLE):
            raise FileExistsError(f'Create you audit table first at path abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID_to_check}/Tables/...')
        if not notebookutils.fs.exists(self.PATH_TO_CHECKED_TABLE):
            raise FileExistsError(f'your given table does not exists at path abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.TABLE_NAME_to_check}/Tables/...')
    
        self.log = logger(**{column: None for column in self.columns})
        self.log['STATUS_ACTIVITY'] = 'Not start'

    def setKeys(self, initConfig: dict[str, Any]):
        assert set(initConfig.keys()).issubset(set(self.columns).difference()), f'initConfig must have the columns in {self.columns}'
        for column in initConfig:
            self.log[column] = initConfig[column]

    def setKey(self, key: str, value: Any):
        assert key in self.columns, f'key must be in {self.columns}'
        self.log[key] = value
        
    def initialDetail(self, initConfig: dict[str, Any]):
        self.setKeys(initConfig)

    def getKey(self):
        return self.columns
    
    def getLog(self):
        return self.log
        
    def __str__(self):
        out = ''
        for key in self.columns:
            out += f'{key}: {self.log[key]}\n'
        return out
    
    def __repr__(self):
        return str(self.log)
    
    def endSuccess(self):
        self.log['STATUS_ACTIVITY'] = 'Success'
        self._endAuditLog()
        print(self)

        
    def endFail(self, errorCode: str, errorMessage: str):
        self.log['STATUS_ACTIVITY'] = 'Fail'
        self.log['ERRORCODE'] = errorCode
        self.log['ERRORMESSAGE'] = errorMessage
        self._endAuditLog()
        print(self)

    def _endAuditLog(self):
        # write to audit table
        self.log['ENDTIME'] = str(datetime.now() + timedelta(hours=7))
        row = {}
        for key in self.log:
            row[key] = [str(self.log[key])]
        data_tuples = list(zip(*row.values()))
        df = spark.createDataFrame(data_tuples, schema=list(row.keys()))
        df.write.mode('append').save(self.PATH_TO_AUDIT_TABLE)
        return df

    def getAuditLogTable(self):
        return spark.read.load(self.PATH_TO_AUDIT_TABLE)
    
    def countBefore(self):
        if self.log['COUNTROWSBEFORE']:
            raise ValueError('COUNTROWSBEFORE already exist')
        self.log['COUNTROWSBEFORE'] = spark.read.load(self.PATH_TO_CHECKED_TABLE).count()

    def countAfter(self):
        self.log['COUNTROWSAFTER'] = spark.read.load(self.PATH_TO_CHECKED_TABLE).count()

    def getAllPath(self):
        return {'PATH_TO_AUDIT_TABLE':self.PATH_TO_AUDIT_TABLE, 'PATH_TO_CHECKED_TABLE':self.PATH_TO_CHECKED_TABLE}

    def startAudit(self):
        self.log['STARTTIME'] = str(datetime.now() + timedelta(hours=7))
        self.log['STATUS_ACTIVITY'] = 'logging ...'

class AuditLog_SPC(AuditLog_Fusion):
    
    def __init__(self, WS_ID: str, TABLE_NAME_to_check:str, AUDIT_TABLE_NAME:str, LH_ID_to_check: str, LH_ID_audit: str = None, schema: str = None):
        '''
        - if `LH_ID_audit` is not given, it is  LH_ID_to_check automatically, i.e. audit table is in the same lakehouse as that of
        - if using lakehouse with Schema, please provide `schema` parameter
        '''
        super().__init__(['PIPELINENAME', 'PIPELINERUNID', 'TRIGGERTYPE', 'TABLE_NAME', 'FUNCTION_NAME','COUNTROWSBEFORE', 'COUNTROWSAFTER', 'ERRORCODE', 'ERRORMESSAGE'] ,WS_ID, TABLE_NAME_to_check, AUDIT_TABLE_NAME, LH_ID_to_check, LH_ID_audit, schema)

    def initialDetail(self,  pipelineName: str, pipelineId: str, TriggerType: str, TableName: str, functionName: str):
        super().initialDetail({
            'PIPELINENAME': pipelineName, 
            'PIPELINERUNID': pipelineId, 
            'TRIGGERTYPE': TriggerType, 
            'TABLE_NAME': TableName, 
            'FUNCTION_NAME': functionName
        })
        self.startAudit()
        self.log['AUDITKEY'] = self.log['PIPELINENAME'] + '-' + self.log['TABLE_NAME'] + '-' + str(self.log['STARTTIME']).replace(' ','_').replace(':','_')


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




















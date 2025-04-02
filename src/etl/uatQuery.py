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
from notebookutils import mssparkutils
import re
from env.utils import *
# from tabulate import tabulate

spark = SparkSession.builder\
        .appName("uat")\
        .getOrCreate()

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

class UAT:
    def __init__(self, WS_ID, check_LH_ID, saveResult_LH_ID, saveResult_tableName, checklist_LH_ID, checklist_csvName):
        self.WS_ID = WS_ID
        self.check_LH_ID = check_LH_ID
        self.saveResult_LH_ID = saveResult_LH_ID
        self.saveResult_tableName = saveResult_tableName
        self.checklist_LH_ID = checklist_LH_ID
        self.checklist_csvName = checklist_csvName

        self.tablePath = f'abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{checklist_LH_ID}/Files/{checklist_csvName}'
        self.resultPath = f'abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{saveResult_LH_ID}/Tables/{saveResult_tableName}'
        checkList = pd.read_csv(self.tablePath, dtype={'idx':np.int32}).sort_values(by='idx').reset_index(drop=True)
        checkList = checkList[checkList['check']==1]
        checkList[['idx', 'Table', 'Column', 'KeyCheck', 'groupbyKey','additionalSQLFilter']] = checkList[['idx', 'Table', 'Column', 'KeyCheck', 'groupbyKey','additionalSQLFilter']].fillna('')
        self.checkList = checkList[['idx', 'Table', 'Column', 'KeyCheck', 'groupbyKey', 'additionalSQLFilter']]
        base_path = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.check_LH_ID}'
        data_types = ['Tables']
        
        if self.check_LH_ID != '':
            df = pd.concat([
                pd.DataFrame({
                    'tableName': [item.name.lower() for item in mssparkutils.fs.ls(f'{base_path}/{data_type}/')],
                    'type': data_type[:-1].lower() , 
                    'path': [item.path for item in mssparkutils.fs.ls(f'{base_path}/{data_type}/')],
        
                }) for data_type in data_types], ignore_index=True)
            df = df[['tableName', 'path']]
            self.pathToLoad = df.set_index('tableName')['path']
        else:
            self.pathToLoad = None
    
    def getCheckedTablePath(self, tableName):
        return self.pathToLoad.loc[tableName.lower()]

    def getChecklistTable(self):
        return self.checkList

    def get_file_table_list(self):
        return self.pathToLoad
    


class SQLgenerator(UAT):
    def __init__(self, WS_ID, checklist_LH_ID, checklist_csvName, saveQuery_fileName):
        super().__init__(WS_ID=WS_ID, check_LH_ID='', saveResult_LH_ID='', saveResult_tableName='', checklist_LH_ID=checklist_LH_ID, checklist_csvName=checklist_csvName)
        self.checkList = self.checkList[['idx', 'Table', 'Column', 'KeyCheck', 'groupbyKey', 'additionalSQLFilter']]
        self.sql = None

    def countrowQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'' AS [Column], 'countrow' AS [KeyCheck],'' AS [KeyGroupby], '' AS [groupbyValue], CAST(COUNT(*) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))}"
    
    def distinctQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'{Column.lower()}' AS [Column], 'distinct' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(COUNT(DISTINCT({Column})) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))}"
    
    def sumQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'{Column.lower()}' AS [Column], 'sum' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(SUM({Column}) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))}"
    
    def minQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'{Column.lower()}' AS [Column], 'min' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(MIN({Column}) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))}"
    
    def maxQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'{Column.lower()}' AS [Column], 'max' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(MAX({Column}) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))}"
    
    def firstdateQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'{Column.lower()}' AS [Column], 'firstdate' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(FORMAT(CAST(MIN({Column}) AS DATETIME), 'yyyyMMdd') AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table} WHERE {Column} IS NOT NULL{bool(additionalSQLFilter)*(' AND '+str(additionalSQLFilter))}"
    
    def lastdateQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table],'{Column.lower()}' AS [Column], 'lastdate' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(FORMAT(CAST(MAX({Column}) AS DATETIME), 'yyyyMMdd') AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table} WHERE {Column} IS NOT NULL{bool(additionalSQLFilter)*(' AND '+str(additionalSQLFilter))}"
    
    def countnonnullQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table], '{Column.lower()}' AS [Column], 'countnonnull' AS [KeyCheck], '' AS [KeyGroupby], '' AS [groupbyValue], CAST(COUNT({Column}) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table} WHERE {Column} IS NOT NULL{bool(additionalSQLFilter)*(' AND '+str(additionalSQLFilter))}"
    
    def countbyQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table], '' AS [Column], 'countby' AS [KeyCheck], '{KeyGroupby}' AS [KeyGroupby], {KeyGroupby} AS [groupbyValue], CAST(COUNT(*) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))} GROUP BY {KeyGroupby}"

    def countdistinctbyQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table], '{Column.lower()}' AS [Column], 'countdistinctby' AS [KeyCheck], '{KeyGroupby}' AS [KeyGroupby], {KeyGroupby} AS [groupbyValue], CAST(COUNT(DISTINCT({Column})) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))} GROUP BY {KeyGroupby}"
    
    def sumbyQuery(self, idx, Table, Column, KeyGroupby, additionalSQLFilter=None):
        return f"SELECT {idx} AS [index], '{Table.lower()}' AS [Table], '{Column.lower()}' AS [Column], 'sumby' AS [KeyCheck], '{KeyGroupby}' AS [KeyGroupby], {KeyGroupby} AS [groupbyValue], CAST(SUM({Column}) AS FLOAT) AS [ValueOnPrem] FROM dbo.{Table}{bool(additionalSQLFilter)*(' WHERE '+str(additionalSQLFilter))} GROUP BY {KeyGroupby}"

    def getCheckList(self):
        return self.checkList
    
    def getQuery(self, idx, Table, Column, KeyCheck, KeyGroupby, additionalSQLFilter=None):
        mapper = {
        'countrow':self.countrowQuery,
        'distinct':self.distinctQuery,
        'sum': self.sumQuery,
        'min':self.minQuery,
        'max':self.maxQuery,
        'firstdate':self.firstdateQuery,
        'lastdate':self.lastdateQuery,
        'countnonnull':self.countnonnullQuery,
        'countby': self.countbyQuery,
        'countdistinctby': self.countdistinctbyQuery,
        'sumby': self.sumbyQuery
        }
        return mapper[KeyCheck](idx, Table, Column, KeyGroupby, additionalSQLFilter)

    def datetimeShiftSparkToSQL(self, expression):
        if re.match(r".*to_date\(current_timestamp.*",expression):
            result = 'GETDATE()'
            # matchHour = re.match(r".*([+-])[ ]*INTERVAL[ ]+([0-9]+)[ ]+HOURS.*",expression)
            # if matchHour:
            #     intervalHour = (matchHour.group(1) + matchHour.group(2)).replace("+",'')
            #     result = f'DATEADD(HOUR, {intervalHour}, {result})'
            matchDay = re.match(r".*([+-])[ ]*INTERVAL[ ]+([0-9]+)[ ]+DAYS.*",expression)
            if matchDay:
                intervalDay = (matchDay.group(1) + matchDay.group(2)).replace("+",'')
                result = f'DATEADD(DAY, {intervalDay}, {result})'
            return result
        else:
            return None

    def generateSQL(self):
                                                                    # ['idx',          'Table',      'Column',      'KeyCheck',      'groupbyKey',      'additionalSQLFilter']
        self.checkList['sql'] = self.checkList.apply(lambda row: self.getQuery(row['idx'], row['Table'], row['Column'], row['KeyCheck'], row['groupbyKey'], row['additionalSQLFilter']), axis=1)
        self.sql =  ' UNION ALL '.join(self.checkList['sql'])
        return self.sql

    def getSQL(self):
        if not self.sql:
            self.generateSQL()
        return self.sql

    # TODO: implement that load one table at first then query all about that table



class UAT_Fabric(UAT):
    def __init__(self, WS_ID, check_LH_ID, saveResult_LH_ID, saveResult_tableName, checklist_LH_ID, checklist_csvName, saveResult=True, max_workers=4):
        super().__init__(WS_ID, check_LH_ID, saveResult_LH_ID, saveResult_tableName, checklist_LH_ID, checklist_csvName)
        cTime = spark.sql("SELECT current_timestamp() + interval 7 hours").collect()[0][0]
        self.checkList['dateCheck'] = cTime
        self.checkList = self.checkList[['idx', 'Table', 'Column', 'KeyCheck', 'groupbyKey', 'dateCheck', 'additionalSQLFilter']]
        self.saveResult = saveResult
        self.max_workers = max_workers

    def addResultToTable(self, df):
        sinkPath = self.resultPath
        # sinkPath = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.saveResult_LH_ID}.Lakehouse/Tables/{self.saveResult_tableName}'
        df\
            .select(['index','Table','Column','KeyCheck','KeyGroupby','groupbyValue','valueOnFabric','dateCheckFabric'])\
            .withColumn('valueOnFabric', col('valueOnFabric').cast(DecimalType(36,5)))\
            .write.mode('append').save(sinkPath)
    
    def runQuerySpark_byRow(self, df,idx, Table, Column, KeyCheck, groupbyKey, cTime, additionalSQLFilter):
        # already has df in memory by df.cache()
        # Start with the base DataFrame
        if additionalSQLFilter != '':
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
                       .withColumn('dateCheckFabric', lit(cTime))\
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
                        .withColumn('dateCheckFabric',lit(cTime))\
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
                        .withColumn('dateCheckFabric',lit(cTime))\
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
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'countnonnull':
            result = df.filter(col(Column).isNotNull()).agg(count('*').alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(''))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'firstdate':
            result = df.filter(col(Column).isNotNull()).agg(min(date_format(Column, 'yyyyMMdd')).alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(''))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'lastdate':
            result = df.filter(col(Column).isNotNull()).agg(max(date_format(Column, 'yyyyMMdd')).alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(''))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'max':
            result = df.filter(col(Column).isNotNull()).agg(max(Column).alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(''))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'min':
            result = df.filter(col(Column).isNotNull()).agg(min(Column).alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(''))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
                       .withColumn('filter', lit(additionalSQLFilter))
        elif KeyCheck == 'sum':
            result = df.agg(sum(Column).alias('valueOnFabric'))\
                       .withColumn("index", lit(idx))\
                       .withColumn("Table", lit(Table))\
                       .withColumn("Column", lit(Column))\
                       .withColumn("KeyCheck", lit(KeyCheck))\
                       .withColumn("KeyGroupby", lit(''))\
                       .withColumn("groupbyValue", lit(''))\
                       .withColumn('dateCheckFabric', lit(cTime))\
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
                        .withColumn('dateCheckFabric',lit(cTime))\
                        .drop(groupbyKey)\
                        .withColumn('filter', lit(additionalSQLFilter))
    
        #special exception
        if groupbyKey.lower() == 'saledate':
            result = result.withColumn('groupbyValue', date_format(col("groupbyValue").cast(TimestampType()), 'yyyyMMdd'))
    
        return result

    def runQuerySpark_TableName(self, TableName):
        referenceTable = self.checkList
        referenceTable_filter = referenceTable[referenceTable['Table'].apply(lambda x: x.lower())==TableName.lower()]
        checkedTable = spark.read.load(self.getCheckedTablePath(TableName.lower()))
    
        results = []
    
        for rowIdx in tqdm(referenceTable_filter.index, desc=TableName,leave=True):
            resultRow = self.runQuerySpark_byRow(checkedTable,*referenceTable_filter.loc[rowIdx])
            # def runQuerySpark_byRow(self, df,idx, Table, Column, KeyCheck, groupbyKey, cTime, additionalSQLFilter):
            results.append(resultRow)
        
        result = results[0]
        result = result.withColumn('valueOnFabric', col('valueOnFabric').cast(DecimalType(36,5)))
        for r in results[1:]:
            r = copySchemaByName(r,result)
            r = r.withColumn('valueOnFabric', col('valueOnFabric').cast(DecimalType(36,5)))
            result = result.unionByName(r)
        
        if self.saveResult:
            self.addResultToTable(result)
    
        return result

    def runQuerySpark(self):
    
        allTable = self.checkList['Table'].unique()
        numtable = len(allTable)
    
        print('query processing ...')
        with ThreadPoolExecutor(max_workers = self.max_workers) as p:
            results = list(p.map(self.runQuerySpark_TableName,allTable))
        
        print('query successed')

        result = results[0]
        for r in results[1:]:
            r = copySchemaByName(r,result)
            result = result.unionByName(r)
        return result.withColumn('valueOnFabric',col('valueOnFabric').cast(DoubleType()))
import numpy as np
from pyspark.sql import Row
from pyspark.sql.functions import udf, col, when, concat, lit, format_string, upper, substring, substring_index, expr, current_date, current_timestamp,dense_rank, regexp_extract, length,input_file_name, to_timestamp,concat_ws, isnull, date_format, asc, trim, trunc, date_sub, year,coalesce, row_number, sum, lpad, max, regexp_replace, floor, instr, to_date
from pyspark.sql.types import IntegerType, DecimalType, StringType, LongType, TimestampType, StructType, StructField, DoubleType, ShortType
import pandas as pd
import re
import os
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import src.etl.utils as utils
from pyspark.sql.window import Window
import shutil
from decimal import Decimal
# import notebookutils
import hashlib
from tqdm import tqdm
import time

from typing import Callable, List, Dict, Any
# from tabulate import tabulate

spark = SparkSession.builder.config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

current_date_list = [int(x) for x in (datetime.now() + timedelta(hours=7)).strftime('%Y%m%d %H%M%S').split(' ')]

class CashPickUp:

    def __init__(self, config):
        self.config = config
        self.WS_ID = config["WS_ID"]
        self.BronzeLH_ID = config["BronzeLH_ID"]
        self.SilverLH_ID = config["SilverLH_ID"]
        self.STAGING_TABLE_NAME = 'stagingcpu'
        self.FACT_TABLE_NAME    = 'bgn_fact_cashpickup'
        self.LastRowNumber = 0
        self.FILE_PATH_ON_LAKE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Ingest/POS/FIRSTPRO/CPU/'
        self.ProcessedDir = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Processed/POS/FIRSTPRO/CPU/'
        self.STAGING_TABLE_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/{self.STAGING_TABLE_NAME}'
        self.FACT_TABLE_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/{self.FACT_TABLE_NAME}'
        self.Logger = []

    def readFile(self):
        self.current_date_list = [int(x) for x in (datetime.now() + timedelta(hours=7)).strftime('%Y%m%d %H%M%S').split(' ')]
        # return pd.read_csv(self.FILE_PATH_ON_LAKE,encoding='TIS-620',delimiter='|',header=None,names=names)
        df = spark.read.format("csv") \
            .option("encoding", "TIS-620").option("delimiter", "|") \
            .load(self.FILE_PATH_ON_LAKE)
            
        if df.count() != 0:
            df = df \
                .withColumn("FilePath", input_file_name()) \
                .withColumn("FileName", regexp_replace(regexp_extract("filepath", r".*/([^/?]+)", 1), "%23", "#"))\
                .withColumn("ETL_Date", expr("current_timestamp() + INTERVAL 7 HOURS"))
        else:
            df = self.stagingSchema\
                .withColumn("FilePath", input_file_name()) \
                .withColumn("FileName", regexp_replace(regexp_extract("filepath", r".*/([^/?]+)", 1), "%23", "#"))\
                .withColumn("ETL_Date", expr("current_timestamp() + INTERVAL 7 HOURS"))
        
        self.FilePathList = [r.FilePath for r in df.select('FilePath').distinct().collect()]
            
        return df.drop('FilePath')

    def load_staging(self):
        self.Logger.append('\tStarting Load Staging process...')
        # raise NotImplementedError('load_staging is not implemented yet')
        self.stagingTable = self.readFile()
        self.stagingTable.write.mode('overwrite').save(self.STAGING_TABLE_PATH)
        self.stagingTable = spark.read.load(self.STAGING_TABLE_PATH)
    
    def delete_duplicate_file(self):
        self.Logger.append('\tStarting Delete Duplicate File process...')
        # raise NotImplementedError('delete_duplicate_file is not implemented yet')
        factTable = spark.read.load(self.FACT_TABLE_PATH)
        stagingTable = spark.read.load(self.STAGING_TABLE_PATH)
        factTable = factTable.join(stagingTable, on='FileName', how='left_anti')
        factTable.write.mode('overwrite').save(self.FACT_TABLE_PATH)

        self.stagingTable = spark.read.load(self.STAGING_TABLE_PATH)
        self.factTable = spark.read.load(self.FACT_TABLE_PATH)
    
    def get_last_row_number(self):
        self.Logger.append('\tStarting Get Last Row Number process...')
        # raise NotImplementedError('get_last_row_number is not implemented yet')
        '''
        select top 1 RowNumber FROM (
            SELECT RowNumber
            FROM [BCP-DW].[dbo].[BGN_fact_CashPickUp]
            UNION
            SELECT 0 as RowNumber) T1
        ORDER BY 1 DESC
        '''
        factTable = spark.read.load(self.FACT_TABLE_PATH)
        if factTable.count() != 0:
            lastNum = factTable.select('RowNumber').orderBy('RowNumber', ascending=False).limit(1).collect()[0].RowNumber
        else:
            lastNum = 0
        self.lastNum = lastNum

    def get_data_and_time(self):
        self.stagingTable = self.stagingTable\
                .withColumn('Drop_Date', regexp_extract(col('_c5'), r'([0-9]+) [0-9]+',1))\
                .withColumn('Drop_Time', regexp_extract(col('_c5'), r'[0-9]+ ([0-9]+)',1))\
                .withColumn('Tranfer_Date', regexp_extract(col('_c9'), r'([0-9]+) [0-9]+',1))\
                .withColumn('Tranfer_Time', regexp_extract(col('_c9'), r'[0-9]+ ([0-9]+)',1))\
                .withColumn('DropDate_Time', regexp_replace(col('_c5'), " ", ""))\
                .withColumn('DropTime_TypeTime', concat(
                                                        substring(trim(regexp_replace(col('_c5'), r'.* ', '')), 1, 2), lit(':'),
                                                        substring(trim(regexp_replace(col('_c5'), r'.* ', '')), 3, 2), lit(':'),
                                                        substring(trim(regexp_replace(col('_c5'), r'.* ', '')), 5, 2)
                                                    ))
        '''
        SUBSTRING(
            TRIM(RIGHT([Column 5],LEN([Column 5]) - FINDSTRING([Column 5]," ",1))),
            1,
            2) 
        + ":" + 
        SUBSTRING(TRIM(RIGHT([Column 5],LEN([Column 5]) - FINDSTRING([Column 5]," ",1))),3,2) 
        + ":" + 
        SUBSTRING(TRIM(RIGHT([Column 5],LEN([Column 5]) - FINDSTRING([Column 5]," ",1))),5,2)
        '''

    def split_not_null_column2(self):
        self.stagingTable = self.stagingTable.filter(col('_c2').isNotNull())

    def data_conversion(self):
        self.stagingTable = self.stagingTable.withColumn("DropDate", col("Drop_Date").cast(IntegerType())) \
                                            .withColumn("DropTime", col("Drop_Time").cast(IntegerType())) \
                                            .withColumn("TransferDate", col("Tranfer_Date").cast(IntegerType())) \
                                            .withColumn("TransferTime", col("Tranfer_Time").cast(IntegerType())) \
                                            .withColumn("Amount", col("_c8").cast(DecimalType(10,2))) \
                                            .withColumn("Customer_code", col("_c1").cast(IntegerType())) \
                                            .withColumn("POS_Code", col("_c2").cast(IntegerType())) \
                                            .withColumn("PacketNumber", col("_c6").cast(IntegerType())) \
                                            .withColumn("CashierCode", col("_c7").cast(IntegerType())) \
                                            .withColumn("DropTime_TypeTime", col("DropTime_TypeTime").cast(TimestampType()))

    def get_dimstation(self):
        '''
        SELECT [StationKey]
            ,[SOR]
            ,[CustomerCode]
            ,Poscode
        FROM [BCP-DW].[dbo].[dimStation]
        '''
        self.dimstation = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/dimstation')\
                                .select('StationKey','SOR','CustomerCode','Poscode')\
                                .withColumnRenamed('CustomerCode','Customer_code')\
                                .withColumnRenamed('Poscode','POS_Code')\

    def merge_join(self):
        self.stagingTable = self.stagingTable.join(
                                self.dimstation,
                                on = ['Customer_code','POS_Code'],
                                how= 'left'
                            ) #-> to get 'StationKey' and 'SOR'

    def add_row_num(self):
        windowSpec = Window.orderBy('FileName')
        self.stagingTable = self.stagingTable.withColumn('RowNumber', row_number().over(windowSpec) + self.lastNum)

    def save_to_fact(self):
        self.stagingTable = self.stagingTable.drop(*[f'_c{i}' for i  in range(10)],'Drop_Date')\
                                .withColumnsRenamed({
                                    'POS_Code':'StationCode',
                                    'Customer_code':'CustomerCode',
                                    'Tranfer_Date':'TranferDate',
                                    'Tranfer_Time':'TranferTime',
                                    'DropDate_Time':'DropDateTime'})\
                                .select(spark.read.load(self.FACT_TABLE_PATH).columns)
        self.stagingTable = utils.copySchemaByName(self.stagingTable, spark.read.load(self.FACT_TABLE_PATH))
        self.pandas_df = self.stagingTable.toPandas()
        self.stagingTable.write.mode('overwrite').save(self.FACT_TABLE_PATH+'_new')
        spark.read.load(self.FACT_TABLE_PATH+'_new').write.mode('append').save(self.FACT_TABLE_PATH)

    def load_to_fact(self):
        self.Logger.append('\tStarting Load to Fact process...')
        # raise NotImplementedError('load_to_fact is not implemented yet')
        self.get_data_and_time()
        self.split_not_null_column2()
        self.data_conversion()

        self.get_dimstation()
        self.merge_join()
        self.add_row_num()
        self.save_to_fact()

    def move_file(self):
        self.Logger.append('\tStarting Move File')
        # self.FilePathList
        for file in self.FilePathList:
            file = file.replace('%23', '#')
            file = re.match(r'(.*\.CSV).*',file).group(1)
            dest_path = file.replace('/Ingest/', '/Processed/')
            dest_path = re.sub(r'/[^/]+$','',dest_path)
            # print(f'file = {file}')
            # print(f'dest_path = {dest_path}')

            notebookutils.fs.mv(file, dest_path, create_path=True, overwrite=True)

    def run_CPU(self):
        self.Logger.append("Starting Cash Pick Up process...")
        self.load_staging()
        self.delete_duplicate_file()
        self.get_last_row_number()
        self.load_to_fact()
        self.move_file()
        self.Logger.append("Cash Pick Up process completed successfully.")
        return self.pandas_df
    
class CashPickUp_Tranfer_Diff:
    def __init__(self, config):
        self.config = config
        self.WS_ID = config["WS_ID"]
        self.BronzeLH_ID = config["BronzeLH_ID"]
        self.SilverLH_ID = config["SilverLH_ID"]
        self.FACT_CPU_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/bgn_fact_cashpickup'
        self.FACT_TABLE_NAME = 'bgn_fact_cashpickup_tranfer_diff'
        self.FACT_TABLE_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/{self.FACT_TABLE_NAME}'
        self.table = spark.read.load(self.FACT_CPU_PATH)

    def queryTransferDiff(self):
        '''
        SELECT  
            [StationKey]
            ,[StationCode]
            ,[TranferDate]
            ,sum([Amount]) AS Amount_TranferDate
            ,DropTime
        FROM
            [BCP-DW].[dbo].[BGN_fact_CashPickUp]
        where
            TranferDate = DropDate and 
            droptime > '01:00:00'
        group by
            [StationKey]
            ,[StationCode]
            ,[TranferDate]
            ,DropDate
            ,DropTime
        '''
        self.table = self.table\
                        .filter((col('TranferDate')==col('DropDate')) & (col('DropTime') > 10000))\
                        .select('StationKey', 'StationCode', 'TranferDate' ,'DropTime', 'DropDate', 'Amount')\
                        .groupBy('StationKey', 'StationCode', 'TranferDate', 'DropTime', 'DropDate')\
                        .agg(sum('Amount').alias('Amount_TranferDate'))

    def aggregate(self):
        self.table = self.table.groupBy('StationKey', 'StationCode', 'TranferDate')\
                        .agg(sum('Amount_TranferDate').alias('Amount'), max('DropTime').alias('TimeDrop'))

    def save_to_fact(self):
        self.result = self.table.toPandas()
        self.table = utils.copySchemaByName(self.table,spark.read.load(self.FACT_TABLE_PATH))
        self.table.write.mode('append').save(self.FACT_TABLE_PATH)


    def run(self):
        self.queryTransferDiff()
        self.aggregate()
        self.save_to_fact()
        return self.result

class CashPickUp_DropDate:
    def __init__(self, config):
        self.config = config
        self.WS_ID = config["WS_ID"]
        self.BronzeLH_ID = config["BronzeLH_ID"]
        self.SilverLH_ID = config["SilverLH_ID"]
        self.FACT_CPU_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/bgn_fact_cashpickup_new'
        self.FACT_TABLE_NAME = 'bgn_fact_cashpickup_dropdate'
        self.FACT_TABLE_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/{self.FACT_TABLE_NAME}'
        self.table = spark.read.load(self.FACT_CPU_PATH) #.filter(col('ETL_DATE')>=date_sub(current_date(),10))
        self.factTable = spark.read.load(self.FACT_TABLE_PATH) 
        
    def load(self):
        '''
        SELECT 
            [StationKey]
            ,[StationCode]
            , IIF (substring(Cast([DropDateTime] as nvarchar(14)),9,4) < '0100'
                    ,dateadd(day,-1,Cast(concat(SUBSTRING(Cast([DropDate] as nvarchar(8)),1,4) , '-',SUBSTRING(Cast([DropDate] as nvarchar(8)),5,2), '-',SUBSTRING(Cast([DropDate] as nvarchar(8)),7,2)) as Date)) 
                    ,Cast(concat(SUBSTRING(Cast([DropDate] as nvarchar(8)),1,4) , '-',SUBSTRING(Cast([DropDate] as nvarchar(8)),5,2), '-',SUBSTRING(Cast([DropDate] as nvarchar(8)),7,2)) as Date))  as  CheckTime
            ,SUM([Amount] ) SUMAmount
        FROM
            [BCP-DW].[dbo].[BGN_fact_CashPickUp]
        where
            20240701 = 20240701 (?)
        group by
            [StationKey]
            ,[StationCode]
            , IIF (substring(Cast([DropDateTime] as nvarchar(14)),9,4) < '0100'
                    ,dateadd(day,-1,Cast(concat(SUBSTRING(Cast([DropDate] as nvarchar(8)),1,4) , '-',SUBSTRING(Cast([DropDate] as nvarchar(8)),5,2), '-',SUBSTRING(Cast([DropDate] as nvarchar(8)),7,2)) as Date)) 
                    ,Cast(concat(SUBSTRING(Cast([DropDate] as nvarchar(8)),1,4) , '-',SUBSTRING(Cast([DropDate] as nvarchar(8)),5,2), '-',SUBSTRING(Cast([DropDate] as nvarchar(8)),7,2)) as Date))
        '''
        self.table = self.table.withColumn(
                        'CheckTime',
                        when(
                            substring(col('DropDateTime').cast(StringType()), 9, 4) < '0100',
                            date_sub(to_date(concat(
                                substring(col('DropDate').cast(StringType()), 1, 4), lit('-'),
                                substring(col('DropDate').cast(StringType()), 5, 2), lit('-'),
                                substring(col('DropDate').cast(StringType()), 7, 2)
                            )), 1)
                        ).otherwise(
                            to_date(concat(
                                substring(col('DropDate').cast(StringType()), 1, 4), lit('-'),
                                substring(col('DropDate').cast(StringType()), 5, 2), lit('-'),
                                substring(col('DropDate').cast(StringType()), 7, 2)
                            ))
                        )
                    ).withColumn('YearMonth_DropDate', concat(
                                substring(col('DropDate').cast(StringType()), 1, 4),
                                substring(col('DropDate').cast(StringType()), 5, 2)).cast(IntegerType())
                    ).groupBy(
                        'StationKey',
                        'StationCode',
                        'CheckTime',
                        'YearMonth_DropDate'
                    ).agg(
                        sum('Amount').alias('Amount')
                    ).withColumnRenamed('CheckTime','DropDate')

    def save_to_fact(self):
        # insert = self.table.join(self.factTable, on = ['StationKey', 'StationCode', 'DropDate'], how='left_anti')
        self.result = self.table.toPandas()
        # insert = utils.copySchemaByName(insert, self.factTable)
        self.table = utils.copySchemaByName(self.table,spark.read.load(self.FACT_TABLE_PATH))
        self.table.write.mode('append').save(self.FACT_TABLE_PATH)

    def run(self):
        self.load()
        self.save_to_fact()
        return self.result
    
class CashPickUp_SAP:

    def __init__(self, config):
        self.config = config
        self.WS_ID = config["WS_ID"]
        self.BronzeLH_ID = config["BronzeLH_ID"]
        self.SilverLH_ID = config["SilverLH_ID"]
        self.CODE = config["CODE"]
        self.SOURCE_FILE_NAME = config["SOURCE_FILE_NAME"]

        self.STAGING_TABLE_NAME = ('BGN_staging_SAP_'+str(self.CODE)).lower()
        self.FACT_TABLE_NAME    = ('BGN_fact_SAP_'+str(self.CODE)).lower()
        self.FILE_PATH_ON_LAKE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Ingest/BGN_CashPickUp/{self.SOURCE_FILE_NAME}'
        self.ProcessedDir = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Processed/BGN_CashPickUp'
        self.STAGING_TABLE_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/{self.STAGING_TABLE_NAME}'
        self.FACT_TABLE_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/{self.FACT_TABLE_NAME}'
        self.FACT_TABLE = spark.read.load(self.FACT_TABLE_PATH)

        self.current_date_list = [int(x) for x in (datetime.now() + timedelta(hours=7)).strftime('%Y%m%d %H%M%S').split(' ')]
        self.Logger = []

    def readFile(self):

        df = pd.read_excel(self.FILE_PATH_ON_LAKE,skiprows=4).fillna("").astype("str").sum(axis=1).str.split("\t",expand=True)
        df_col = df.iloc[0].str.replace('.','').str.replace('G/L','G_L')
        df = df.iloc[2:]
        df.columns = df_col.str.strip().apply(lambda x: '-' if len(x) == 0 else x)
        df = df.iloc[:-2]
        df = df.drop(columns='-')
        return df

    def load_to_staging(self):
        df = self.readFile()
        df = df[df['Text'].str.contains('CPICK')]
        df['AssignmentDate'] = df['Assignment'].str[-8:]
        df['DocDate'] = pd.to_datetime(df['Doc Date'].str.replace(".",""),format="%m%d%Y").dt.strftime('%Y%m%d')
        df['POSNumber'] = df['Assignment'].str[:5]
        df['Amount in doc curr'] = df['Amount in doc curr'].str.strip()
        df['DocumentNo'] = df['DocumentNo'].str.strip()
        df['AmountCURR'] = df['Amount in doc curr'].str.extract(r'([^\.]+)\..*',expand=False).str.replace(r'[\.,]',"",regex=True)
        df['FileName'] = self.SOURCE_FILE_NAME
        df['ETL_Date'] = datetime.now()
        df['YYYYMM_DocDate'] = pd.to_datetime(df['Doc Date'],format='%d.%m.%Y').dt.strftime('%Y%m')

        df = spark.createDataFrame(df)
        df = df.withColumns(
            {'POSNumber':col('POSNumber').cast(IntegerType())}
        )

        dimstation = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/dimstation').select(col('POSCode').alias('POSNumber'),'StationKey','CustomerCode')
        df = df.join(dimstation, on='POSNumber', how='left')
        df = df.withColumns(
                {
                    'Amount_DOC_Curr': col('AmountCURR').cast(DecimalType(18,2)),
                    'EntryDate': lit(None),
                 }
            ).withColumnsRenamed(
                {
                    'Ty':'Type',
                    'DocDate': 'DOC_Date',
                    'DocumentNo': 'DOC_Number',
                    'AssignmentDate':'AssignmentDateKey',
                    'POSNumber': 'StationCode'
                }
            )
        df = df.select(spark.read.load(self.STAGING_TABLE_PATH).columns)
        df = utils.copySchemaByName(df, spark.read.load(self.STAGING_TABLE_PATH))
        df.write.mode('overwrite').save(self.STAGING_TABLE_PATH)
        self.STAGING_TABLE = spark.read.load(self.STAGING_TABLE_PATH).cache()

    def remove_previous_data(self):
        self.YYYYMM_DocDate = self.STAGING_TABLE.select(col('YYYYMM_DocDate').alias('YearMonth_DocDate')).distinct()

        '''
        delete FROM [BCP-DW].[dbo].[BGN_fact_SAP_1106070]
        where YearMonth_DocDate = ?
        '''
        self.table = self.FACT_TABLE.join(self.YYYYMM_DocDate, on='YearMonth_DocDate', how='left_anti')

    def save_to_fact(self):
        self.STAGING_TABLE = utils.copySchemaByName(self.STAGING_TABLE, self.FACT_TABLE)
        print(utils.trackSizeTable(self.table,schema=True))
        print(utils.trackSizeTable(self.STAGING_TABLE,schema=True))
        self.table = self.table.unionByName(
            self.STAGING_TABLE.withColumnRenamed('YYYYMM_DocDate','YearMonth_DocDate')
        )
        self.table.write.mode('overwrite').save(self.FACT_TABLE_PATH)

    def runETL(self):
        self.readFile()
        self.load_to_staging()
        self.remove_previous_data()
        self.save_to_fact()


    # def load_staging(self):
    #     # self.Logger.append('\tStarting Load Staging process...')
    #     # # raise NotImplementedError('load_staging is not implemented yet')
    #     # self.stagingTable = self.readFile()
    #     # self.stagingTable.write.mode('overwrite').save(self.STAGING_TABLE_PATH)
    #     # self.stagingTable = spark.read.load(self.STAGING_TABLE_PATH)
    
    # def delete_duplicate_file(self):
    #     # self.Logger.append('\tStarting Delete Duplicate File process...')
    #     # # raise NotImplementedError('delete_duplicate_file is not implemented yet')
    #     # factTable = spark.read.load(self.FACT_TABLE_PATH)
    #     # stagingTable = spark.read.load(self.STAGING_TABLE_PATH)
    #     # factTable = factTable.join(stagingTable, on='FileName', how='left_anti')
    #     # factTable.write.mode('overwrite').save(self.FACT_TABLE_PATH)

    #     # self.stagingTable = spark.read.load(self.STAGING_TABLE_PATH)
    #     # self.factTable = spark.read.load(self.FACT_TABLE_PATH)
    
    # def get_last_row_number(self):
    #     # self.Logger.append('\tStarting Get Last Row Number process...')
    #     # # raise NotImplementedError('get_last_row_number is not implemented yet')
    #     # '''
    #     # select top 1 RowNumber FROM (
    #     #     SELECT RowNumber
    #     #     FROM [BCP-DW].[dbo].[BGN_fact_CashPickUp]
    #     #     UNION
    #     #     SELECT 0 as RowNumber) T1
    #     # ORDER BY 1 DESC
    #     # '''
    #     # factTable = spark.read.load(self.FACT_TABLE_PATH)
    #     # if factTable.count() != 0:
    #     #     lastNum = factTable.select('RowNumber').orderBy('RowNumber', ascending=False).limit(1).collect()[0].RowNumber
    #     # else:
    #     #     lastNum = 0
    #     # self.lastNum = lastNum

    # def get_data_and_time(self):
    #     # self.stagingTable = self.stagingTable\
    #     #         .withColumn('Drop_Date', regexp_extract(col('_c5'), r'([0-9]+) [0-9]+',1))\
    #     #         .withColumn('Drop_Time', regexp_extract(col('_c5'), r'[0-9]+ ([0-9]+)',1))\
    #     #         .withColumn('Tranfer_Date', regexp_extract(col('_c9'), r'([0-9]+) [0-9]+',1))\
    #     #         .withColumn('Tranfer_Time', regexp_extract(col('_c9'), r'[0-9]+ ([0-9]+)',1))\
    #     #         .withColumn('DropDate_Time', regexp_replace(col('_c5'), " ", ""))\
    #     #         .withColumn('DropTime_TypeTime', concat(
    #     #                                                 substring(trim(regexp_replace(col('_c5'), r'.* ', '')), 1, 2), lit(':'),
    #     #                                                 substring(trim(regexp_replace(col('_c5'), r'.* ', '')), 3, 2), lit(':'),
    #     #                                                 substring(trim(regexp_replace(col('_c5'), r'.* ', '')), 5, 2)
    #     #                                             ))
    #     # '''
    #     # SUBSTRING(
    #     #     TRIM(RIGHT([Column 5],LEN([Column 5]) - FINDSTRING([Column 5]," ",1))),
    #     #     1,
    #     #     2) 
    #     # + ":" + 
    #     # SUBSTRING(TRIM(RIGHT([Column 5],LEN([Column 5]) - FINDSTRING([Column 5]," ",1))),3,2) 
    #     # + ":" + 
    #     # SUBSTRING(TRIM(RIGHT([Column 5],LEN([Column 5]) - FINDSTRING([Column 5]," ",1))),5,2)
    #     # '''

    # def split_not_null_column2(self):
    #     # self.stagingTable = self.stagingTable.filter(col('_c2').isNotNull())

    # def data_conversion(self):
    #     # self.stagingTable = self.stagingTable.withColumn("DropDate", col("Drop_Date").cast(IntegerType())) \
    #     #                                     .withColumn("DropTime", col("Drop_Time").cast(IntegerType())) \
    #     #                                     .withColumn("TransferDate", col("Tranfer_Date").cast(IntegerType())) \
    #     #                                     .withColumn("TransferTime", col("Tranfer_Time").cast(IntegerType())) \
    #     #                                     .withColumn("Amount", col("_c8").cast(DecimalType(10,2))) \
    #     #                                     .withColumn("Customer_code", col("_c1").cast(IntegerType())) \
    #     #                                     .withColumn("POS_Code", col("_c2").cast(IntegerType())) \
    #     #                                     .withColumn("PacketNumber", col("_c6").cast(IntegerType())) \
    #     #                                     .withColumn("CashierCode", col("_c7").cast(IntegerType())) \
    #     #                                     .withColumn("DropTime_TypeTime", col("DropTime_TypeTime").cast(TimestampType()))

    # def get_dimstation(self):
    #     # '''
    #     # SELECT [StationKey]
    #     #     ,[SOR]
    #     #     ,[CustomerCode]
    #     #     ,Poscode
    #     # FROM [BCP-DW].[dbo].[dimStation]
    #     # '''
    #     # self.dimstation = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/dimstation')\
    #     #                         .select('StationKey','SOR','CustomerCode','Poscode')\
    #     #                         .withColumnRenamed('CustomerCode','Customer_code')\
    #     #                         .withColumnRenamed('Poscode','POS_Code')\

    # def merge_join(self):
    #     # self.stagingTable = self.stagingTable.join(
    #     #                         self.dimstation,
    #     #                         on = ['Customer_code','POS_Code'],
    #     #                         how= 'left'
    #     #                     ) #-> to get 'StationKey' and 'SOR'

    # def add_row_num(self):
    #     # windowSpec = Window.orderBy('FileName')
    #     # self.stagingTable = self.stagingTable.withColumn('RowNumber', row_number().over(windowSpec) + self.lastNum)

    # def save_to_fact(self):
    #     # self.stagingTable = self.stagingTable.drop(*[f'_c{i}' for i  in range(10)],'Drop_Date')\
    #     #                         .withColumnsRenamed({
    #     #                             'POS_Code':'StationCode',
    #     #                             'Customer_code':'CustomerCode',
    #     #                             'Tranfer_Date':'TranferDate',
    #     #                             'Tranfer_Time':'TranferTime',
    #     #                             'DropDate_Time':'DropDateTime'})\
    #     #                         .select(spark.read.load(self.FACT_TABLE_PATH).columns)
    #     # self.stagingTable = utils.copySchemaByName(self.stagingTable, spark.read.load(self.FACT_TABLE_PATH))
    #     # self.pandas_df = self.stagingTable.toPandas()
    #     # self.stagingTable.write.mode('overwrite').save(self.FACT_TABLE_PATH+'_new')
    #     # spark.read.load(self.FACT_TABLE_PATH+'_new').write.mode('append').save(self.FACT_TABLE_PATH)

    # def load_to_fact(self):
    #     # self.Logger.append('\tStarting Load to Fact process...')
    #     # # raise NotImplementedError('load_to_fact is not implemented yet')
    #     # self.get_data_and_time()
    #     # self.split_not_null_column2()
    #     # self.data_conversion()

    #     # self.get_dimstation()
    #     # self.merge_join()
    #     # self.add_row_num()
    #     # self.save_to_fact()

    # def move_file(self):
    #     # self.Logger.append('\tStarting Move File')
    #     # # self.FilePathList
    #     # for file in self.FilePathList:
    #     #     file = file.replace('%23', '#')
    #     #     file = re.match(r'(.*\.CSV).*',file).group(1)
    #     #     dest_path = file.replace('/Ingest/', '/Processed/')
    #     #     dest_path = re.sub(r'/[^/]+$','',dest_path)
    #     #     # print(f'file = {file}')
    #     #     # print(f'dest_path = {dest_path}')

    #     #     notebookutils.fs.mv(file, dest_path, create_path=True, overwrite=True)

    # def run_CPU(self):
    #     # self.Logger.append("Starting Cash Pick Up process...")
    #     # self.load_staging()
    #     # self.delete_duplicate_file()
    #     # self.get_last_row_number()
    #     # self.load_to_fact()
    #     # self.move_file()
    #     # self.Logger.append("Cash Pick Up process completed successfully.")
    #     # return self.pandas_df
import numpy as np
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import re
import os
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import env.utils as utils
from pyspark.sql.window import Window
import shutil
from decimal import Decimal
import notebookutils
import hashlib
from tqdm import tqdm
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List, Dict, Any


spark = SparkSession.builder.config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

current_date_list = [int(x) for x in (datetime.now() + timedelta(hours=7)).strftime('%Y%m%d %H%M%S').split(' ')]

def get_all_files(directory):
    file_paths = []
    for root, dirs, files in os.walk(directory):
        if ('Archive' not in root):
            for file in files:
                if (re.match(r'.*\.[a-zA-Z]+$',file)):
                    matchObj = re.match(r'^.*/POS/(FIRSTPRO|FLOWCO)/([A-Z_]*)$', root)
                    # file_paths.append(os.path.join(root, file))
                    file_paths.append((matchObj.group(1),matchObj.group(2), file))
    return file_paths

class POS:
    '''
    This will be call at first to allocate the filekey of all nonblank raw file by adopt the code of reserved fileKey
    '''
    def __init__(self, WS_ID, BronzeLH_ID=None, SilverLH_ID=None):
        self.WS_ID = WS_ID
        self.BronzeLH_ID = BronzeLH_ID
        self.SilverLH_ID = SilverLH_ID
        self.BronzeLH_path = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}'
        self.SilverLH_path = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}'

        self.staging_suffix_mapper  = {'TRN':'Trn','PMT':'Payment','LUB':'Lube','AR_TRANSACTIONS':'AR','DSC':'Discount','EOD_METERS':'Meter','POINTS':'Points','REFUND':'Refund','EOD_TANKS':'Tank', 'FREE':'Free'}
        self.fact_suffix_mapper     = {'TRN':'fuelsales','PMT':'payment','LUB':'lube','AR_TRANSACTIONS':'ar','DSC':'discount','EOD_METERS':'meter','POINTS':'points','REFUND':'refund','EOD_TANKS':'tank', 'FREE':'free'}
        self.mismatch_suffix_mapper = {'TRN':'trn',  'PMT':'payment','LUB':'lube','AR_TRANSACTIONS':'ar','DSC':'discount','EOD_METERS':'meter','POINTS':'points','REFUND':'refund','EOD_TANKS':'tank', 'FREE':'free'}
    
        self.staging_columns = {
            ('FIRSTPRO','TRN'): ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","Dispenser","Hose","Grade","GradeTitle","UnitPrice","Volume","Amount","AttendeeNumber","ChiefNumber"],
            ('FLOWCO','TRN'):   ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","Dispenser","Hose","Grade","GradeTitle","UnitPrice","Volume","Amount","AttendeeNumber","ChiefNumber"],
            ('FIRSTPRO','PMT'): ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","PMCode","ReceiveAmount","ARNumber","LPNumber","DocCode","PAY_Date","PAY_Time","End_Date","End_Time"],
            ('FLOWCO','PMT'):   ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","PMCode","ReceiveAmount","ARNumber","LPNumber","DocCode"],
            ('FIRSTPRO','LUB'):["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","LubeCode","LubeName","UnitPrice","Quantity","Amount"],
            ('FLOWCO','LUB'):  ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","LubeCode","LubeName","UnitPrice","Quantity","Amount"],
            ('FIRSTPRO','AR_TRANSACTIONS'): ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","Dispenser","Hose","GradeNumber","GradeTitle","GradePrice","Volume","Amount","LubeCode","UnitPrice","Quantity","LBAmount","PMCode","ARNumber","ARName","LicensePlate","DocCode","ODOMeter","AttendantNumber","DONumber"],
            ('FLOWCO','AR_TRANSACTIONS'):   ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","Dispenser","Hose","GradeNumber","GradeTitle","GradePrice","Volume","Amount","LubeCode","UnitPrice","Quantity","LBAmount","PMCode","ARNumber","ARName","LicensePlate","DocCode","ODOMeter","AttendantNumber","DONumber"],
            ('FIRSTPRO','DSC'): ["KeyId","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","DCCode","DCAmount"],
            ('FLOWCO','DSC'): ["KeyId","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","DCCode","DCAmount"],
            ('FIRSTPRO','EOD_METERS'): ["KeyId","CustomerCode","POSCode","StationName","CloseDateKey","TankNumber","DispenserNumber","HoseNumber","ShiftNumber","GradeNumber","GradeTitle","GradePrice","StartVolume","EndVolume","StartAmount","EndAmount","TestVolume","UsName"],
            ('FLOWCO','EOD_METERS'):   ["KeyId","CustomerCode","POSCode","StationName","CloseDateKey","ShiftNumber","DispenserNumber","HoseNumber","TankNumber","GradeNumber","GradeTitle","GradePrice","StartVolume","EndVolume","StartAmount","EndAmount","TestVolume","UsName"],
            ('FIRSTPRO','POINTS'): ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","PMCode","Terminal_ID","Merchant_Name","Batch_Number","Card_Trace_Number","Card_Number","Available_Balance","Collect_Point","Redeem_Point","Collect_TimeStemp"],
            ('FIRSTPRO','REFUND'): ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","Volume","Amount","Vat"],
            ('FIRSTPRO','EOD_TANKS'):  ["KeyID","CustomerCode","POSCode","StationName","StartDateKey","TankNumber","GradeNumber","GradeTitle","OpenVolume","CloseVolume","DeliveryVolume","EndDateKey"],
            ('FLOWCO','EOD_TANKS'):    ["KeyID","CustomerCode","POSCode","StationName","StartDateKey","TankNumber","GradeNumber","GradeTitle","OpenVolume","CloseVolume","DeliveryVolume","EndDateKey"],
            ('FIRSTPRO','FREE'):   ['KeyID','CustomerCode','POSCode','StationName','EOD_Date','ReceiptNumber','TransDateKey','TransTime','TransNumber','FreeItemNo','FreeItemName','FreeQTY','FreeTopupQTY','FreeMemberQTY','FreeRemainQTY','ReceiptItemQTY','MemberCard','LoyaltyRedeemQTY','Rate','AccrualPoint','LoyaltyProductCode','LoyaltyProductName','Promotion'],
            ('FLOWCO','FREE'):     ['KeyID','CustomerCode','POSCode','StationName','EOD_Date','ReceiptNumber','TransDateKey','TransTime','TransNumber','FreeItemNo','FreeItemName','FreeQTY','FreeTopupQTY','FreeMemberQTY','FreeRemainQTY','ReceiptItemQTY','MemberCard','LoyaltyRedeemQTY','Rate','AccrualPoint','LoyaltyProductCode','LoyaltyProductName','Promotion','DeliveryId']
        }

        self.RENAME_MAPPING = {'PMT':'PAYMENT',
                    'LUB':'LUBE',
                    'AR_TRANSACTIONS':'AR TRANSACTIONS',
                    'DSC':'DISCOUNT',
                    'EOD_METERS':'METERS',
                    'EOD_TANKS':'TANKS'}
        
        self.lookupstation_key = {
            ('FIRSTPRO','TRN'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FLOWCO','TRN'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FIRSTPRO','PMT'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FLOWCO','PMT'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FIRSTPRO','LUB'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FLOWCO','LUB'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FIRSTPRO','AR_TRANSACTIONS'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FLOWCO','AR_TRANSACTIONS'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FIRSTPRO','DSC'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FLOWCO','DSC'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FIRSTPRO','EOD_METERS'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FLOWCO','EOD_METERS'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FIRSTPRO','POINTS'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FIRSTPRO','REFUND'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FIRSTPRO','EOD_TANKS'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FLOWCO','EOD_TANKS'): {'POSName_key': None, 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FIRSTPRO','FREE'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
            ('FLOWCO','FREE'): {'POSName_key':'ReStationName', 'POSCode_key':'POSCode', 'CustomerCode_key':'CustomerCode'},
        }

        self.lookupproduct_key = { # SourceKey_key: Any, SourceTitle_key: Any, StationKey_key: Any
            ('FIRSTPRO','TRN'): {'SourceKey_key':'Grade', 'SourceTitle_key':'ReGradeTitle'},
            ('FLOWCO','TRN'): {'SourceKey_key':'Grade', 'SourceTitle_key':'ReGradeTitle'},
            ('FIRSTPRO','AR_TRANSACTIONS'): {'SourceKey_key':'GradeNumber', 'SourceTitle_key':'ReGradeTitle'},
            ('FLOWCO','AR_TRANSACTIONS'): {'SourceKey_key':'GradeNumber', 'SourceTitle_key':'ReGradeTitle'},
            ('FIRSTPRO','EOD_METERS'): {'SourceKey_key':'GradeNumber', 'SourceTitle_key':'GradeTitle'},
            ('FLOWCO','EOD_METERS'): {'SourceKey_key':'GradeNumber', 'SourceTitle_key':'GradeTitle'},
            ('FIRSTPRO','EOD_TANKS'): {'SourceKey_key':'GradeNumber', 'SourceTitle_key':'GradeTitle'},
            ('FLOWCO','EOD_TANKS'): {'SourceKey_key': 'GradeNumber', 'SourceTitle_key':'GradeTitle'},
            # ('FIRSTPRO','FREE'): {'SourceKey_key':'', 'SourceTitle_key':''},
            # ('FLOWCO','FREE'): {'SourceKey_key':'', 'SourceTitle_key':''},
        }
        
        self.lookupproduct_no_title_key = {
            ('FIRSTPRO','TRN'): {'SourceKey_key':'Grade', 'StationKey_key':'StationKey'},
            ('FLOWCO','TRN'): {'SourceKey_key':'Grade', 'StationKey_key':'StationKey'},
            ('FIRSTPRO','AR_TRANSACTIONS'): {'SourceKey_key':'GradeNumber', 'StationKey_key':'StationKey'},
            ('FLOWCO','AR_TRANSACTIONS'): {'SourceKey_key':'GradeNumber', 'StationKey_key':'StationKey'},
            ('FIRSTPRO','EOD_METERS'): {'SourceKey_key':'GradeNumber', 'StationKey_key':'StationKey'},
            ('FLOWCO','EOD_METERS'): {'SourceKey_key':'GradeNumber', 'StationKey_key':'StationKey'},
            ('FIRSTPRO','EOD_TANKS'): {'SourceKey_key':'GradeNumber', 'StationKey_key':'StationKey'},
            # ('FIRSTPRO','FREE'): {'SourceKey_key':'', 'StationKey_key':''},
            # ('FLOWCO','FREE'): {'SourceKey_key':'', 'StationKey_key':''},
        }

        self.sourcefile_mapping = {
            'TRN': 'TRANSACTION', 'EOD_METERS':'METERS', 'AR_TRANSACTIONS':'AR', 'EOD_TANKS':'TANKS'
        }

        self.sourcefile_no_title_mapping = {
            'TRN': 'TRANSACTION', 'EOD_METERS':'METERS', 'AR_TRANSACTIONS':'TRANSACTION', 'EOD_TANKS':'TANKS'
        }

        self.add_condition_no_title_mapping = {
            ('FIRSTPRO', 'TRN'): (col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull()),
            ('FLOWCO', 'TRN'): (col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull()),
            ('FIRSTPRO', 'EOD_METERS'): (col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'METERS') & (col('sourcetitle').isNull()),
            ('FLOWCO', 'EOD_METERS'): (col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'METERS') & (col('StationKey').isNotNull()),
            ('FIRSTPRO', 'AR_TRANSACTIONS'): (col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull()),
            ('FLOWCO', 'AR_TRANSACTIONS'): (col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull()),
            ('FIRSTPRO', 'EOD_TANKS'): (col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TANKS') & (col('StationKey').isNotNull())
        }

        self.mappingkey_rename = {
            'TANKS': {'MappingKey':'ProductKey'},
            'TRN': {'MappingKey':'ProductKey'},
            'EOD_METERS': {'MappingKey':'ProductKey'},
            'AR_TRANSACTIONS': {'MappingKey':'GradeKey','LBAmount':'LubeAmount','AttendantNumber':'AttendeeNumber'},
        }

        self.log = {}

    def allocate_filekey(self):
        current_date_list = [int(x) for x in (datetime.now() + timedelta(hours=7)).strftime('%Y%m%d %H%M%S').split(' ')]

        try:
            df = spark.read.format('csv').option("encoding", "TIS-620").load(f'{self.BronzeLH_path}/Files/Ingest/POS/*/*')\
                            .withColumn("FilePath", input_file_name()).select('FilePath').drop_duplicates() \
                            .withColumn("FileName", regexp_replace(regexp_extract("FilePath", r".*/([^/?]+)", 1), "%23", "#"))\
                            .withColumn("cat_subcat",regexp_extract("FilePath", r".*/[\d\%]+([^/?]+)\.CSV", 1))\
                            .withColumn("CategoryName", when(col("cat_subcat").contains("FLOWCO"), "FLOWCO").otherwise("FIRSTPRO"))\
                            .withColumn("SubCategoryName", when(col("cat_subcat").contains("FLOWCO"), regexp_extract("cat_subcat", r"(.*)FLOWCO", 1)).otherwise(col("cat_subcat")))\
                            .filter(col('SubCategoryName')!='CPU')\
                            .withColumn("LoadDateKey", lit(current_date_list[0])).withColumn("LoadTimeKey", lit(current_date_list[1]))\
                            .withColumn('LoadStatus', lit(None).cast(ShortType())).cache()
        except:
            df = None

        if df is not None:
            df = df.replace(self.RENAME_MAPPING, subset=["SubCategoryName"])

            window_spec = Window.orderBy(col("FileName").asc())
            path_to_factfile = f'{self.SilverLH_path}/Tables/factfile'
            factFile = spark.read.load(path_to_factfile)
            LastId = factFile.agg(max('FileKey').alias('maxFileKey')).collect()[0].maxFileKey if factFile.agg(max('FileKey').alias('maxFileKey')).collect()[0].maxFileKey else 0
            df = df.withColumn('FileKey', dense_rank().over(window_spec) + LastId).select('FileKey','FileName','CategoryName','SubCategoryName','LoadDateKey','LoadTimeKey','LoadStatus')

            df.write.mode('append').partitionBy(['SubCategoryName', 'CategoryName', 'LoadStatus']).save(path_to_factfile)

            self.log['count_df'] = df.groupBy(['CategoryName','SubCategoryName']).count()
            self.reservedFileKey = df.select('FileKey').collect()
            return self.reservedFileKey
        else:
            return None
        
    def updateFactFile_in_remove_previous_all(self,rows_to_delete):
        self.Logger.append('updateFactFile_in_remove_previous')
        rows_to_update = rows_to_delete.filter((col('LoadStatus').isin([1,3,5,-99]))|(col('LoadStatus').isNull())).select("FileKey").collect()
        rows_to_update = [r.FileKey for r in rows_to_update]
        self.FactFileHandler.factFile = self.FactFileHandler.factFile.withColumn('LoadStatus',when(col('FileKey').isin(rows_to_update), 4).otherwise(col('LoadStatus')))
        self.FactFileHandler.saveTable()

    def updateFactFile_final_all(self,FILENAME_FILEKEY_mapper_succes,FILENAME_FILEKEY_mapper_fail):
        self.Logger.append('updateFactFile_final')
        self.FactFileHandler.EditManyRecords(
            FILENAME_FILEKEY_mapper=FILENAME_FILEKEY_mapper_succes,
            LoadStatus=1
            )
        self.FactFileHandler.EditManyRecords(
            FILENAME_FILEKEY_mapper=FILENAME_FILEKEY_mapper_fail,
            LoadStatus=3
            )

    def post_ETL_all(self,rows_to_delete_all,FILENAME_FILEKEY_mapper_succes_all,FILENAME_FILEKEY_mapper_fail_all):
        self.Logger.append('Start post_ETL... to edit factfile')
        FILENAME_FILEKEY_mapper_succes = {}
        FILENAME_FILEKEY_mapper_fail = {}
        if len(rows_to_delete_all) > 0:
            rows_to_delete = rows_to_delete_all[0]
            FILENAME_FILEKEY_mapper_succes = FILENAME_FILEKEY_mapper_succes_all[0]
            FILENAME_FILEKEY_mapper_fail = FILENAME_FILEKEY_mapper_fail_all[0]
            for row in rows_to_delete_all[1:]:
                rows_to_delete = rows_to_delete.unionByName(row)
            for row in FILENAME_FILEKEY_mapper_succes_all[1:]:
                FILENAME_FILEKEY_mapper_succes.update(row)
            for row in FILENAME_FILEKEY_mapper_fail_all[1:]:
                FILENAME_FILEKEY_mapper_fail.update(row)
            self.updateFactFile_in_remove_previous_all(rows_to_delete)
            self.updateFactFile_final_all(FILENAME_FILEKEY_mapper_succes,FILENAME_FILEKEY_mapper_fail)
        self.Logger.append('post_ETL complete')
        print('\n'.join(self.Logger))

    def dataConversion(self, stagingTable, ColumnsCast: Dict[str, Any]):
        stagingTable = stagingTable.withColumns(
            {column: col(column).cast(ColumnsCast[column]()) for column in ColumnsCast}
        )
        return stagingTable

    def derivedField(self, stagingTable, ColumnsExpr: Dict[str, Any]):
        stagingTable = stagingTable.withColumns(
            {column: ColumnsExpr[column] for column in ColumnsExpr}
        )
        return stagingTable

    def addFileNameToStagingTable(self, stagingTable, factFile):
        self.Logger.append('\t\t\tAdding Filname from factfile by FileKey')
        stagingTable = stagingTable.join(factFile.select('FileKey','FileName'), on='FileKey', how='left')
        return stagingTable
    
    @staticmethod
    def _look_up_product(incomingDF, mappingProduct, CATEGORY, SUBCATEGORY, SourceKey_key, SourceTitle_key):
        incomingDF = incomingDF.withColumns({'key1':col(SourceKey_key),'key2':col(SourceTitle_key)})
        mappingProductjoin = mappingProduct.withColumnsRenamed({'SourceKey':'key1','SourceTitle':'key2'})
        lookupCondition = ['key1','key2']
        matchProduct = incomingDF.join(mappingProductjoin, on=lookupCondition, how='inner').drop('key1','key2')
        UnMatchProduct  = incomingDF.join(mappingProductjoin, on=lookupCondition, how='left_anti').withColumn('Error',lit("Mismatch ProductKey")).drop('key1','key2')
        assert incomingDF.count() == matchProduct.count() + UnMatchProduct.count(), f'Error in lookup mappingProduct: incomingDF.count() != matchProduct.count() + UnMatchProduct.count(); incomingDF.count() = {incomingDF.count()}; matchProduct.count() = {matchProduct.count()}; UnMatchProduct.count() = {UnMatchProduct.count()}'
        if SUBCATEGORY == 'EOD_TANKS' and CATEGORY == 'FLOWCO':
            UnMatchProduct = UnMatchProduct.filter((col('GradeNumber')==2)&(col('OpenVolume')==0)&(col('CloseVolume')==0)&(col('DeliveryVolume')==0))
        return matchProduct, UnMatchProduct
    
    @staticmethod
    def _look_up_product_no_title(incomingDF,mappingProductNoTitle, SourceKey_key, StationKey_key):
        incomingDF = incomingDF.drop('MappingKey').withColumns({'key1':col(SourceKey_key),'key2':col(StationKey_key)})
        mappingProductjoin = mappingProductNoTitle.withColumnsRenamed({'SourceKey':'key1','StationKey':'key2'})
        mappingProductjoin.printSchema()
        lookupCondition = ['key1','key2']
        matchProduct = incomingDF.join(mappingProductjoin, on=lookupCondition, how='inner')
        matchProduct.printSchema()
        matchProduct = matchProduct.drop('key1','key2')
        UnMatchProduct  = incomingDF.join(mappingProductjoin, on=lookupCondition, how='left_anti').withColumn('Error',lit("Mismatch ProductKey")).drop('key1','key2')
        assert incomingDF.count() == matchProduct.count() + UnMatchProduct.count(), f'Error in lookup mappingProductNoTitle: incomingDF.count() != matchProduct.count() + UnMatchProduct.count(); incomingDF.count() = {incomingDF.count()}; matchProduct.count() = {matchProduct.count()}; UnMatchProduct.count() = {UnMatchProduct.count()}'
        return matchProduct, UnMatchProduct
    
class POS_to_staging(POS):
    '''
    Expected all file in this state have thier own FileKey in FactFile whose `LoadStatus = lit(None)`
    '''
    dev = None
    def __init__(self, WS_ID, BronzeLH_ID, SilverLH_ID, SUBCATEGORY):
        # TODO: make it independent from category (move table name back to ETLModule_POS)
        super().__init__(
            WS_ID = WS_ID,
            BronzeLH_ID = BronzeLH_ID,
            SilverLH_ID = SilverLH_ID
            )
        
        # self.CATEGORY = config["CATEGORY"] # read both FIRSTPRO and FLOWCO in the same time like mismatch does
        self.SUBCATEGORY = SUBCATEGORY
        self.STAGING_TABLE_NAME = 'stagingPos'   + self.staging_suffix_mapper.get(self.SUBCATEGORY,TypeError('NOT CORRECT SUBCATEGORY'))
        # self.FACT_TABLE_NAME    = 'factpos'      + self.fact_suffix_mapper.get(self.SUBCATEGORY,TypeError('NOT CORRECT SUBCATEGORY'))
        # self.MIS_TABLE_NAME     = 'mismatchpos' + self.mismatch_suffix_mapper.get(self.SUBCATEGORY,TypeError('NOT CORRECT SUBCATEGORY'))
        
        self.path_to_staging    = f'{self.BronzeLH_path}/Tables/{self.STAGING_TABLE_NAME}'
        self.path_to_factfile    = f'{self.SilverLH_path}/Tables/factfile'
        self.LoadedStagingDir = f'{self.BronzeLH_path}/Files/LoadedStaging/'
        self.BadFilesDir =  f'{self.BronzeLH_path}/Files/BadFiles/'

        if not notebookutils.fs.exists(f'{self.LoadedStagingDir}/POS'):
            notebookutils.fs.mkdirs(f'{self.LoadedStagingDir}/POS')
        if not notebookutils.fs.exists(f'{self.BadFilesDir}/POS'):
            notebookutils.fs.mkdirs(f'{self.BadFilesDir}/POS')

        self.stagingTable = spark.read.load(self.path_to_staging)
        self.stagingColumns = self.stagingTable.columns

        self.FactFileHandler = FactFileHandler(self.WS_ID,self.SilverLH_ID)
        self.stagingSchema = spark.read.load(self.path_to_staging).limit(0)
        self.log = {}
        self.Logger = []

    # def truncate_staging(self):
    #     tablePath = f'{self.BronzeLH_path}/Tables/{self.STAGING_TABLE_NAME}'
    #     empty_df = spark.createDataFrame([], schema=spark.read.format("delta").load(tablePath).schema)
    #     empty_df.write.format("delta").mode("overwrite").save(tablePath)
    #     return True
    
    def addFileKeyToStaging(self, df):
        factfile = spark.read.load(self.path_to_factfile).filter((col('SubCategoryName')==self.RENAME_MAPPING.get(self.SUBCATEGORY,self.SUBCATEGORY))&(col('LoadStatus').isNull()))
        window_spec = Window.partitionBy("FileName").orderBy(col("FileKey").desc())
        tmpFactFile = factfile.withColumn("RowNum", row_number().over(window_spec))
        lookUpFileKey = tmpFactFile.filter(col('RowNum')==1).select('FileName','FileKey').withColumn('FileKey',col('FileKey').cast(IntegerType()))
        df = df.join(lookUpFileKey, on='FileName', how='left')
        return df

    def readFilePos(self, file_path, columns):
        current_date_list = [int(x) for x in (datetime.now() + timedelta(hours=7)).strftime('%Y%m%d %H%M%S').split(' ')]

        df = spark.read.format("csv") \
            .option("encoding", "TIS-620").option("delimiter", "|") \
            .load(file_path)
            
        if df.count() != 0:
            df = df.toDF(*columns) \
                .withColumn("FilePath", input_file_name()) \
                .withColumn("FileName", regexp_replace(regexp_extract("FilePath", r".*/([^/?]+)", 1), "%23", "#"))\
                .withColumn("LoadDateKey", lit(current_date_list[0])).withColumn("LoadTimeKey", lit(current_date_list[1]))
        else:
            df = self.stagingSchema\
                .withColumn("FilePath", input_file_name()) \
                .withColumn("FileName", regexp_replace(regexp_extract("FilePath", r".*/([^/?]+)", 1), "%23", "#"))\
                .withColumn("LoadDateKey", lit(current_date_list[0])).withColumn("LoadTimeKey", lit(current_date_list[1]))
        return df.drop('FilePath')
    
    def readRawFiles(self):
        dfs = []
        dfs.append(self.readFilePos(f'{self.BronzeLH_path}/Files/Ingest/POS/FIRSTPRO/{self.SUBCATEGORY}/', self.staging_columns[('FIRSTPRO',self.SUBCATEGORY)]))
        if self.SUBCATEGORY not in ['REFUND','POINTS']:
            dfs.append(self.readFilePos(f'{self.BronzeLH_path}/Files/Ingest/POS/FLOWCO/{self.SUBCATEGORY}/', self.staging_columns[('FLOWCO',self.SUBCATEGORY)]))
        
        df = spark.read.load(self.path_to_staging).drop('FileKey').limit(0)
        for d in dfs:
            df = df.unionByName(d,allowMissingColumns=True)

        df = self.addFileKeyToStaging(df)
        df = utils.copySchemaByName(df,self.stagingSchema)
        return df # already have fileKey
    
    def move_to_loadedstaging(self):
        notebookutils.fs.mkdirs(f'{self.LoadedStagingDir}/POS/FIRSTPRO/{self.SUBCATEGORY}/')
        notebookutils.fs.mv(f'{self.BronzeLH_path}/Files/Ingest/POS/FIRSTPRO/{self.SUBCATEGORY}/', f'{self.LoadedStagingDir}/POS/FIRSTPRO/',create_path=True,overwrite=True)
        notebookutils.fs.mkdirs(f'{self.BronzeLH_path}/Files/Ingest/POS/FIRSTPRO/{self.SUBCATEGORY}/')

        if self.SUBCATEGORY not in ['REFUND','POINTS']:
            notebookutils.fs.mkdirs(f'{self.LoadedStagingDir}/POS/FLOWCO/{self.SUBCATEGORY}/')
            notebookutils.fs.mv(f'{self.BronzeLH_path}/Files/Ingest/POS/FLOWCO/{self.SUBCATEGORY}/', f'{self.LoadedStagingDir}/POS/FLOWCO/',create_path=True,overwrite=True)
            notebookutils.fs.mkdirs(f'{self.BronzeLH_path}/Files/Ingest/POS/FLOWCO/{self.SUBCATEGORY}/')

    def move_to_badfiles(self):
        notebookutils.fs.mkdirs(f'{self.BadFilesDir}/POS/FIRSTPRO/{self.SUBCATEGORY}/')
        notebookutils.fs.mv(f'{self.BronzeLH_path}/Files/Ingest/POS/FIRSTPRO/{self.SUBCATEGORY}/', f'{self.BadFilesDir}/POS/FIRSTPRO/',create_path=True,overwrite=True)
        notebookutils.fs.mkdirs(f'{self.BronzeLH_path}/Files/Ingest/POS/FIRSTPRO/{self.SUBCATEGORY}/')

        if self.SUBCATEGORY not in ['REFUND','POINTS']:
            notebookutils.fs.mkdirs(f'{self.BadFilesDir}/POS/FLOWCO/{self.SUBCATEGORY}/')
            notebookutils.fs.mv(f'{self.BronzeLH_path}/Files/Ingest/POS/FLOWCO/{self.SUBCATEGORY}/', f'{self.BadFilesDir}/POS/FLOWCO/',create_path=True,overwrite=True)
            notebookutils.fs.mkdirs(f'{self.BronzeLH_path}/Files/Ingest/POS/FLOWCO/{self.SUBCATEGORY}/')

    def saveStagingTable(self, df):
        df.select(*set(self.stagingSchema.columns + ['LoadDateKey', 'LoadTimeKey', "FileName"])).write.mode('overwrite').option("overwriteSchema", "true").save(self.path_to_staging)

    def fromRawToStaging(self):
        try:
            # self.truncate_staging()
            self.stagingTable = self.readRawFiles()
            self.saveStagingTable(self.stagingTable)
            self.move_to_loadedstaging() # Ingest -> LoadedStaging
            self.STATUS = 'Save to staging succeed'
            return self
        except Exception as e:
            self.move_to_badfiles() # Ingest -> badfiles
            return self
    
    def fromRawToStaging_no_catch(self):
        # self.truncate_staging()
        self.stagingTable = self.readRawFiles()
        self.saveStagingTable(self.stagingTable)
        self.move_to_loadedstaging() # Ingest -> LoadedStaging
        self.STATUS = 'Save to staging succeed'
        return self
        
class POS_to_staging_all(POS):
    def __init__(self, config):
        self.config = config
        super().__init__(
            WS_ID = config["WS_ID"],
            BronzeLH_ID = config["BronzeLH_ID"],
            SilverLH_ID = config["SilverLH_ID"]
            )
        self.SUBCATEGORIES = config.get('SUBCATEGORIES', ['AR_TRANSACTIONS', 'DSC', 'EOD_METERS','EOD_TANKS','LUB','PMT','POINTS','REFUND','TRN'])
        assert isinstance(self.SUBCATEGORIES, list), 'SUBCATEGORIES should be a list'

    def create_pos_to_staging(self, SUBCATEGORY):
        load_obj = POS_to_staging(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID, SUBCATEGORY)
        s = load_obj.fromRawToStaging()
        return s
    
    def run_load_to_staging(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            self.futures = {SUBCATEGORY: executor.submit(self.create_pos_to_staging, SUBCATEGORY) for SUBCATEGORY in self.SUBCATEGORIES}

class POS_load_to_fact(POS): #both main etl and mismatch will use this wherer staging contains both FIRSTPRO and FLOWCO at the same time
    def __init__(self, config):
        super().__init__(
            WS_ID = config["WS_ID"],
            BronzeLH_ID = config["BronzeLH_ID"],
            SilverLH_ID = config["SilverLH_ID"]
            )
        self.SUBCATEGORY = config["SUBCATEGORY"]
        self.STAGING_TABLE_NAME = 'stagingPos'   + self.staging_suffix_mapper.get(self.SUBCATEGORY,TypeError('NOT CORRECT SUBCATEGORY'))
        self.FACT_TABLE_NAME    = 'factpos'      + self.fact_suffix_mapper.get(self.SUBCATEGORY,TypeError('NOT CORRECT SUBCATEGORY'))
        self.MIS_TABLE_NAME     = 'mismatchpos' + self.mismatch_suffix_mapper.get(self.SUBCATEGORY,TypeError('NOT CORRECT SUBCATEGORY'))
        
        self.path_to_staging    = f'{self.BronzeLH_path}/Tables/{self.STAGING_TABLE_NAME}'
        self.path_to_factfile   = f'{self.SilverLH_path}/Tables/factfile'
        self.path_to_mismatch   = f'{self.SilverLH_path}/Tables/{self.MIS_TABLE_NAME}'
        self.path_to_fact      = f'{self.SilverLH_path}/Tables/{self.FACT_TABLE_NAME}'

        self.pre_transform_map = {
            'EOD_TANKS': self.pre_transform_TANKS,
            'EOD_METERS': self.pre_transform_METERS,
            'TRN': self.pre_transform_TRN,
            'PMT': self.pre_transform_PMT,
            'LUB': self.pre_transform_LUB,
            'AR_TRANSACTIONS': self.pre_transform_AR,
            'DSC': self.pre_transform_DSC,
            'POINTS':self.pre_transform_PMT,
            'REFUND':self.pre_transform_REFUND
        }

    def load_staging_table(self): # do with mismatch table simultaneously
        mismatch = spark.read.load(self.path_to_mismatch)                           # load mismatch table
        oriMismatchColumns = mismatch.columns
        mismatch = mismatch.join(spark.read.load(self.path_to_factfile).select('FileKey','FileName','LoadDateKey', 'LoadTimeKey'), on='FileKey', how='left') # join with factfile to get FileName
        mismatch.drop('Error').write.mode('append').save(self.path_to_staging)      # append to staging table
        mismatch.limit(0).select(oriMismatchColumns).write.mode('overwrite').save(self.path_to_mismatch)       # clear mismatch table (because we already append to staging table)
        staging = spark.read.load(self.path_to_staging)
        return staging

    def add_category_column(self, stagingTable):
        stagingTable = self.derivedField(stagingTable, {
                'IsFlowCo': when(instr(col('FileName'), 'FLOWCO') > 0, True).otherwise(False),
            })
        return stagingTable 

    def pre_transform_TANKS(self, stagingTable):
        stagingTable = self.dataConversion(stagingTable, {'GradeNumber': StringType})
        stagingTable = self.derivedField(stagingTable, {
                'ReStationName': regexp_replace('StationName', ' ', ''),
            })
        return stagingTable
    
    def pre_transform_METERS(self, stagingTable):
        stagingTable = self.dataConversion(stagingTable, {'GradeNumber': StringType})
        stagingTable = self.derivedField(stagingTable, {
                'ReStationName': regexp_replace('StationName', ' ', ''),
            })
        return stagingTable
    
    def pre_transform_TRN(self, stagingTable):
        stagingTable = self.dataConversion(stagingTable, {'Grade': StringType})
        stagingTable = self.derivedField(stagingTable, {
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
                'ReGradeTitle': when((col('GradeTitle') == "") & (col('Volume') == 0) & (col('Amount') == 0), "LUBE").otherwise(col('GradeTitle')),
                'YearKey': floor(col('TransDateKey') / 10000),
            })
        return stagingTable
    
    def pre_transform_PMT(self, stagingTable):

        stagingTable_flowco = stagingTable.filter(col('IsFlowCo')==True).cache()
        stagingTable_firstpro = stagingTable.filter(col('IsFlowCo')==False).cache()

        stagingTable_firstpro = self.derivedField(stagingTable_firstpro,{
                    'PAY_Time': when(length(col('PAY_Time')) > 1, col('PAY_Time')).otherwise("999999"),
                    'End_Time': when(length(col('End_Time')) > 1, concat(regexp_replace('End_Time', ':', ''), lit('00'))).otherwise("999999"),
                    'PAY_Date': when(length(col('PAY_Date')) > 1, col('PAY_Date')).otherwise("00000000"),
                    'End_Date': when(length(col('End_Date')) > 1, col('End_Date')).otherwise("00000000"),
                    'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                    'ReStationName': regexp_replace('StationName', ' ', ''),
                })
        stagingTable_firstpro = self.derivedField(stagingTable_firstpro,{
                    'PayTimeKey': regexp_replace('PAY_Time', ':', '')
                })
        stagingTable_firstpro = self.derivedField(stagingTable_firstpro,{
                    'End_Time': when(length(col('End_Time')) > 1, col('End_Time')).otherwise("99999999"),
                    'PAY_Date': when(length(col('PAY_Date')) > 1, col('PAY_Date')).otherwise("00000000"),
                    'PAY_Time': when(length(col('PAY_Time')) > 1, col('PAY_Time')).otherwise("99999999"),
                    'End_Date': when(length(col('End_Date')) > 1, col('End_Date')).otherwise("00000000"),
                })
        stagingTable_firstpro = self.derivedField(stagingTable_firstpro,{
                    'Pay_TimeKey': col('PayTimeKey').cast(IntegerType()),
                    'END_TimeKey': col('End_Time').cast(IntegerType())
                })
        
        stagingTable_flowco = self.derivedField(stagingTable_flowco, {
                    'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                    'ReStationName': regexp_replace('StationName', ' ', '')
                })

        stagingTable = stagingTable_firstpro.unionByName(stagingTable_flowco,allowMissingColumns=True)
        return stagingTable
    
    def pre_transform_LUB(self, stagingTable):
        stagingTable = self.derivedField(stagingTable, {
                'ReStationName': regexp_replace('StationName', ' ', ''),
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00'))
            })
        return stagingTable
    
    def pre_transform_AR(self, stagingTable):
        stagingTable = self.derivedField(stagingTable, {
                'GradeNumber': coalesce(col('GradeNumber'),lit(0)).cast(StringType()),
                'GradeTitle': coalesce(col('GradeTitle'),lit('')),
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'CastLubeCode': when(col('LubeCode') == "", "0").otherwise(col('LubeCode')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
                'ReGradeTitle': when((col('GradeTitle') == "") & (col('Volume').cast(ShortType()) == 0) & (col('Amount').cast(ShortType()) == 0), "LUBE").otherwise(col('GradeTitle'))
            })
        return stagingTable
    
    def pre_transform_DSC(self, stagingTable):
        stagingTable = self.derivedField(stagingTable, {
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
            })
        return stagingTable
    
    def pre_transform_POINTS(self, stagingTable):
        stagingTable = self.derivedField(stagingTable, {
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
                'Collect_Date': substring(col('Collect_TimeStemp'), 1, 8),
                'Collect_Time': trim(substring_index(col('Collect_TimeStemp'), ' ', -1)),
                'YearKey': floor(col('TransDateKey') / 10000)
            })
        return stagingTable
        
    def pre_transform_REFUND(self, stagingTable):
        stagingTable = self.derivedField(stagingTable, {
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
            })
        return stagingTable

    def pre_transform(self, stagingTable, SUBCATEGORY):
        stagingTable = self.add_category_column(stagingTable)
        pre_transform_func = self.pre_transform_map[SUBCATEGORY]
        stagingTable = pre_transform_func(stagingTable)
        return stagingTable

    def look_up_stationKey(self, stagingTable, POSName_key, POSCode_key, CustomerCode_key):
        if POSName_key:
            self.dimstation = spark.read.load(f'{self.SilverLH_path}/Tables/dimstation')\
                                .select(col('StationKey'),col('CustomerCode'),col('POSCode'),regexp_replace(col('PosName'), ' ', '').alias('POSName')).drop_duplicates()
            
            incomingDF = stagingTable.withColumns({'key1':col(POSName_key),'key2':col(POSCode_key),'key3':col(CustomerCode_key)})
            dimstationjoin = self.dimstation.withColumnsRenamed({'POSName':'key1','POSCode':'key2','CustomerCode':'key3'})
            lookupCondition = ['key1','key2','key3']

            matchStation = incomingDF.join(dimstationjoin, on= lookupCondition, how='inner').drop('key1','key2','key3')
            UnMatchStation  = incomingDF.join(dimstationjoin, on=lookupCondition, how='left_anti').withColumn('Error',lit("Mismatch StationKey")).drop('key1','key2','key3').withColumn('StationKey',lit(None).cast(IntegerType()))
            assert incomingDF.count() == matchStation.count() + UnMatchStation.count(), f'Error in lookup station: incomingDF.count() != matchStation.count() + UnMatchStation.count();\nincomingDF.count() = {incomingDF.count()}\nmatchStation.count() = {matchStation.count()}\nUnMatchStation.count() = {UnMatchStation.count()}'
            return matchStation, UnMatchStation
        else:
            self.dimstation = spark.read.load(f'{self.SilverLH_path}/Tables/dimstation')\
                                .select(col('StationKey'),col('CustomerCode'),col('POSCode')).drop_duplicates()
            
            incomingDF = stagingTable.withColumns({'key1':col(POSCode_key),'key2':col(CustomerCode_key)})
            dimstationjoin = self.dimstation.withColumnsRenamed({'POSCode':'key1','CustomerCode':'key2'})
            lookupCondition = ['key1','key2']

            matchStation = incomingDF.join(dimstationjoin, on= lookupCondition, how='inner').drop('key1','key2')
            UnMatchStation  = incomingDF.join(dimstationjoin, on=lookupCondition, how='left_anti').withColumn('Error',lit("Mismatch StationKey")).drop('key1','key2').withColumn('StationKey',lit(None).cast(IntegerType()))
            assert incomingDF.count() == matchStation.count() + UnMatchStation.count(), f'Error in lookup station: incomingDF.count() != matchStation.count() + UnMatchStation.count();\nincomingDF.count() = {incomingDF.count()}\nmatchStation.count() = {matchStation.count()}\nUnMatchStation.count() = {UnMatchStation.count()}'
            return matchStation, UnMatchStation
        
    def getMappingProduct(self, CATEGORY, SUBCATEGORY):
        if SUBCATEGORY not in self.sourcefile_mapping.keys():
            return None
        else:
            mappingProduct = spark.read.load(f'{self.SilverLH_path}/Tables/mappingproduct')
            mappingProduct = mappingProduct.filter((col('sourcename') == CATEGORY) & (col('sourcefile') == self.sourcefile_mapping[SUBCATEGORY])).select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).drop_duplicates()
            mappingProduct = utils.trim_string_columns(mappingProduct)
            return mappingProduct
    
    def getMappingProductNoTitle(self, CATEGORY, SUBCATEGORY):
        if (CATEGORY, SUBCATEGORY) not in self.add_condition_no_title_mapping.keys():
            return None
        else:
            mappingProductNoTitle = spark.read.load(f'{self.SilverLH_path}/Tables/mappingproduct')
            mappingProductNoTitle = mappingProductNoTitle.filter(self.add_condition_no_title_mapping[(CATEGORY, SUBCATEGORY)]).select(col('SourceKey'),col('StationKey'),col('MappingKey')).drop_duplicates()
            mappingProductNoTitle = utils.trim_string_columns(mappingProductNoTitle)
            return mappingProductNoTitle

    def look_up_product(self, incomingDF, CATEGORY, SUBCATEGORY, SourceKey_key, SourceTitle_key):
        # TODO: add job for None incomingDF -> return None
        mappingProduct = self.getMappingProduct(CATEGORY, SUBCATEGORY)
        if (incomingDF is None) | (mappingProduct is None):
            return (None, None)
        else:
            matchProduct, UnMatchProduct = self._look_up_product(incomingDF, mappingProduct, CATEGORY, SUBCATEGORY, SourceKey_key, SourceTitle_key)
            return matchProduct, UnMatchProduct

    def look_up_product_no_title(self, incomingDF, CATEGORY, SUBCATEGORY, SourceKey_key, StationKey_key):
        mappingProductNoTitle = self.getMappingProductNoTitle(CATEGORY, SUBCATEGORY)
        if (incomingDF is None) | (mappingProductNoTitle is None):
            return (None, None)
        else:
            matchProduct, UnMatchProduct = self._look_up_product_no_title(incomingDF, mappingProductNoTitle, SourceKey_key, StationKey_key)
            return matchProduct, UnMatchProduct

class POS_ETL(POS_load_to_fact):
    def __init__(self,config):
        super().__init__(config)

    def FIRSTPRO(self, stagingTable_firstpro):
        sbc = self.SUBCATEGORY.upper()
        if stagingTable_firstpro.count() == 0:
            return None, None

        # 1) Station lookup
        match_sta, unmatch_sta = self.look_up_stationKey(
            stagingTable_firstpro,
            **self.lookupstation_key[('FIRSTPRO', sbc)]
        )

        # 2) No-product bypass
        if sbc in ['REFUND','POINTS','DISCOUNT','LUB','PMT']:
            if sbc == 'PMT':
                # regenerate only the time columns, preserve PAY_Date & End_Date
                match_table = (
                    match_sta
                      .withColumn('PAY_Time', col('PayTimeKey').cast(StringType()))
                      .drop('PayTimeKey')
                      .withColumn('End_Time', col('END_TimeKey').cast(StringType()))
                      .drop('END_TimeKey')
                )
            else:
                match_table = match_sta
            mismatch_table = unmatch_sta
            return match_table, mismatch_table

        # 3) Product lookups
        prod_args = self.lookupproduct_key.get(('FIRSTPRO', sbc), {})
        match_prod, unmatch_prod = self.look_up_product(
            incomingDF    = match_sta,
            CATEGORY      = 'FIRSTPRO',
            SUBCATEGORY   = sbc,
            **prod_args
        )
        no_title_args = self.lookupproduct_no_title_key.get(
            ('FIRSTPRO', sbc),
            {'SourceKey_key': None, 'StationKey_key': None}
        )
        match_no, unmatch_no = self.look_up_product_no_title(
            incomingDF    = match_prod,
            CATEGORY      = 'FIRSTPRO',
            SUBCATEGORY   = sbc,
            **no_title_args
        )

        # 4) Assemble match + mismatch
        match_table    = match_no
        mismatch_table = unmatch_sta
        if unmatch_prod:
            mismatch_table = mismatch_table.unionByName(unmatch_prod, allowMissingColumns=True)
        if unmatch_no:
            mismatch_table = mismatch_table.unionByName(unmatch_no,  allowMissingColumns=True)

        # 5) FIRSTPRO‐only tweaks
        if sbc == 'TRN':
            m1 = match_table.filter(col('Volume') >= 0).drop('Volume','Amount')
            m2 = (
                match_table
                  .groupBy('CloseDateKey','TransNumber','FileKey')
                  .agg(
                      sum('Volume').alias('VOLUME'),
                      sum('Amount').alias('AMOUNT')
                  )
                  .withColumn('VOLUME', col('VOLUME').cast(DecimalType(15,3)))
            )
            match_table = m1.join(m2, ['CloseDateKey','TransNumber','FileKey'], 'inner') \
                             .withColumn('Vat', lit(None))

        elif sbc == 'EOD_TANKS':
            mismatch_table = mismatch_table.filter(~(
                (col('GradeNumber') == 2) &
                (col('OpenVolume') == 0) &
                (col('CloseVolume') == 0) &
                (col('DeliveryVolume') == 0)
            ))

        return match_table, mismatch_table


    def FLOWCO(self, stagingTable_flowco):
        sbc = self.SUBCATEGORY.upper()
        if stagingTable_flowco.count() == 0:
            return None, None

        # 1) Station lookup
        match_sta, unmatch_sta = self.look_up_stationKey(
            stagingTable_flowco,
            **self.lookupstation_key[('FLOWCO', sbc)]
        )

        # 2) No-product bypass
        if sbc in ['REFUND','POINTS','DISCOUNT','LUB','PMT']:
            if sbc == 'PMT':
                # inject null string columns for date/time
                null_str = lit(None).cast(StringType())
                match_table = (
                    match_sta
                      .withColumn('PAY_Date',  null_str)
                      .withColumn('PAY_Time',  null_str)
                      .withColumn('End_Date',  null_str)
                      .withColumn('End_Time',  null_str)
                )
                mismatch_table = (
                    unmatch_sta
                      .withColumn('PAY_Date',  null_str)
                      .withColumn('PAY_Time',  null_str)
                      .withColumn('End_Date',  null_str)
                      .withColumn('End_Time',  null_str)
                )
            else:
                match_table, mismatch_table = match_sta, unmatch_sta

            return match_table, mismatch_table

        # 3) Product lookups
        prod_args = self.lookupproduct_key.get(('FLOWCO', sbc), {})
        match_prod, unmatch_prod = self.look_up_product(
            incomingDF    = match_sta,
            CATEGORY      = 'FLOWCO',
            SUBCATEGORY   = sbc,
            **prod_args
        )
        no_title_args = self.lookupproduct_no_title_key.get(
            ('FLOWCO', sbc),
            {'SourceKey_key': None, 'StationKey_key': None}
        )
        match_no, unmatch_no = self.look_up_product_no_title(
            incomingDF    = match_prod,
            CATEGORY      = 'FLOWCO',
            SUBCATEGORY   = sbc,
            **no_title_args
        )

        # 4) Assemble match + mismatch
        match_table    = match_no
        mismatch_table = unmatch_sta
        if unmatch_prod:
            mismatch_table = mismatch_table.unionByName(unmatch_prod, allowMissingColumns=True)
        if unmatch_no:
            mismatch_table = mismatch_table.unionByName(unmatch_no,  allowMissingColumns=True)

        # 5) FLOWCO-only tweaks
        if sbc == 'EOD_TANKS':
            mismatch_table = mismatch_table.filter(~(
                (col('GradeNumber') == 2) &
                (col('OpenVolume') == 0) &
                (col('CloseVolume') == 0) &
                (col('DeliveryVolume') == 0)
            ))

        return match_table, mismatch_table
        
    def run_lookup(self, stagingTable_firstpro, stagingTable_flowco):
        with ThreadPoolExecutor(max_workers=2) as executor:
            future1 = executor.submit(self.FIRSTPRO, stagingTable_firstpro)
            future2 = executor.submit(self.FLOWCO, stagingTable_flowco)

            match_firstpro, mismatch_firstpro = future1.result()
            match_flowco, mismatch_flowco = future2.result()

            match_result = [x for x in [match_firstpro, match_flowco] if x is not None]
            mismatch_result = [x for x in [mismatch_firstpro, mismatch_flowco] if x is not None]

        if len(match_result) > 0:
            match_table = match_result[0]
            for i in range(1, len(match_result)):
                match_table = match_table.unionByName(match_result[i], allowMissingColumns=True)
        else:
            match_table = None

        if len(mismatch_result) > 0:
            mismatch_table = mismatch_result[0]
            for i in range(1, len(mismatch_result)):
                mismatch_table = mismatch_table.unionByName(mismatch_result[i], allowMissingColumns=True)
        else:
            mismatch_table = None

        return match_table, mismatch_table
    
    def rename_mappingKey(self, df):
        """
        Rename the generic MappingKey (and any other columns) into each
        subcategory's true key names.  Safe on df=None.
        """
        if df is None:
            return None

        sbc = self.SUBCATEGORY.upper()
        renames = {
            'TRN':             {'MappingKey':'ProductKey'},
            'EOD_METERS':      {'MappingKey':'ProductKey'},
            'METERS':          {'MappingKey':'ProductKey'},
            'EOD_TANKS':       {'MappingKey':'ProductKey'},
            'TANKS':           {'MappingKey':'ProductKey'},
            'AR_TRANSACTIONS': {
                'MappingKey':'GradeKey',
                'LBAmount':'LubeAmount',
                'AttendantNumber':'AttendeeNumber'
            },
        }

        mapping = renames.get(sbc)
        return df.withColumnsRenamed(mapping) if mapping else df

    from pyspark.sql.functions import col

    def saveMatchTable(self, match_table, path_to_save):
        # 1. Load the existing table to capture its schema
        facttable = spark.read.load(path_to_save)
        target_schema = facttable.schema

        # 2. For each field in the target schema, cast the incoming column
        for field in target_schema:
            match_table = match_table.withColumn(
                field.name,
                col(field.name).cast(field.dataType)
            )

        # 3. Re-order/select to exactly the table’s columns
        match_table = match_table.select([col(f.name) for f in target_schema])

        # 4. Append into Delta, now that types line up
        match_table.write.mode('append').save(path_to_save)


    def saveMisMatchTable(self, mismatch_table, path_to_save):
        # 1. Load the existing table to capture its schema
        facttable = spark.read.load(path_to_save)
        target_schema = facttable.schema
        # 2. For each field in the target schema, cast the incoming column  
        for field in target_schema:
            mismatch_table = mismatch_table.withColumn(
                field.name,
                col(field.name).cast(field.dataType)
            )
        # 3. Re-order/select to exactly the table’s columns
        mismatch_table = mismatch_table.select([col(f.name) for f in target_schema])
        
        # 4. Append into Delta, now that types line up  
        mismatch_table.write.mode('append').save(path_to_save)

    def remove_previous_data(self, comingFileName):
        factfile = spark.read.load(self.path_to_factfile)

        fact_table = spark.read.load(self.path_to_fact)
        fact_table = fact_table.join(factfile.select('FileKey', 'FileName'), on='FileKey', how='left') # join เพื่อเอา FileName มาใส่ใน fact_table
        remove_filekey_fact = fact_table.select('FileName', 'FileKey').join(comingFileName, on='FileName', how='inner').select('FileKey').distinct().cache()
        new_fact_table = fact_table.join(remove_filekey_fact, on='FileKey', how='left_anti').drop('FileName')
        new_fact_table.write.mode('overwrite').save(self.path_to_fact)

        mismatch_table = spark.read.load(self.path_to_mismatch)
        mismatch_table = mismatch_table.join(factfile.select('FileKey', 'FileName'), on='FileKey', how='left')
        remove_filekey_mismatch = mismatch_table.select('FileKey', 'FileName').join(comingFileName, on='FileName', how='inner').select('FileKey').distinct().cache()
        new_mismatch_table = mismatch_table.join(remove_filekey_mismatch, on='FileKey', how='left_anti').drop('FileName')
        new_mismatch_table.write.mode('overwrite').save(self.path_to_mismatch)

        remove_filekey = remove_filekey_fact.unionByName(remove_filekey_mismatch, allowMissingColumns=True).distinct() # for update LoadStatus = 4 in the last step together with other subcategory

        return remove_filekey
        
    def run_etl(self):
        self.stagingTable = self.load_staging_table()
        assert "FileName" in self.stagingTable.columns, f"FileName column not found in staging table {self.STAGING_TABLE_NAME}"
        self.comingFileName = self.stagingTable.select('FileName').distinct().cache()
        self.remove_filekey = self.remove_previous_data(self.comingFileName).cache() # now same file records is removed from fact and mismatch table

        self.stagingTable = self.pre_transform(self.stagingTable, self.SUBCATEGORY)

        self.stagingTable_firstpro = self.stagingTable.filter(~col('IsFlowCo')).cache()
        self.stagingTable_flowco = self.stagingTable.filter(col('IsFlowCo')).cache()
        self.match_table, self.mismatch_table = self.run_lookup(self.stagingTable_firstpro, self.stagingTable_flowco)
        # TODO; make below code can handle None
        if self.match_table is not None:
            self.match_table = self.rename_mappingKey(self.match_table)
            self.saveMatchTable(self.match_table, self.path_to_fact)
        
        if self.mismatch_table is not None:
            self.saveMisMatchTable(self.mismatch_table, self.path_to_mismatch)



        return self # จะเอา self.remove_filekey ไปใช้ใน factfile update




















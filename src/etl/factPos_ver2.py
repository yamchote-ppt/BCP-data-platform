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

class LoadToFact:
    def __init__(self, path_to_fact_table, path_to_mismatch, stagingTable, CATEGORY, SUBCATEGORY,WS_ID,SilverLH_ID, factTable,mismatchTable):
        self.path_to_fact_table = path_to_fact_table
        self.path_to_mismatch = path_to_mismatch
        self.CATEGORY = CATEGORY
        self.SUBCATEGORY = SUBCATEGORY
        self.stagingTable = stagingTable
        self.WS_ID = WS_ID
        self.SilverLH_ID =SilverLH_ID
        self.factTable = factTable
        self.mismatchTable = mismatchTable
        self.dimStation = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/dimstation')\
                               .select(col('StationKey'),col('CustomerCode'),col('POSCode'),regexp_replace(col('PosName'), ' ', '').alias('POSName')).drop_duplicates()
        self.log = {'stagingTable':stagingTable}
        self.Logger = []

    def dataConversion(self, ColumnsCast: Dict[str, Any]):
        self.stagingTable = self.stagingTable.withColumns(
            {column: col(column).cast(ColumnsCast[column]()) for column in ColumnsCast}
        )

    def derivedField(self,ColumnsExpr: Dict[str, Any]):
        self.stagingTable = self.stagingTable.withColumns(
            {column: ColumnsExpr[column] for column in ColumnsExpr}
        )

    def lookUpStation(self, incomingDF, POSName_key, POSCode_key, CustomerCode_key):
        
        self.Logger.append('\t\t\t\t\tStarting Lookup Station...')
        incomingDF = incomingDF.withColumns({'key1':col(POSName_key),'key2':col(POSCode_key),'key3':col(CustomerCode_key)})
        dimstationjoin = self.dimStation.withColumnsRenamed({'POSName':'key1','POSCode':'key2','CustomerCode':'key3'})
        lookupCondition = ['key1','key2','key3']

        matchStation = incomingDF.join(dimstationjoin, on= lookupCondition, how='inner').drop('key1','key2','key3')
        UnMatchStation  = incomingDF.join(dimstationjoin, on=lookupCondition, how='left_anti').withColumn('Error',lit("Mismatch StationKey")).drop('key1','key2','key3').withColumn('StationKey',lit(None).cast(IntegerType()))
        assert incomingDF.count() == matchStation.count() + UnMatchStation.count(), f'Error in lookup station: incomingDF.count() != matchStation.count() + UnMatchStation.count();\nincomingDF.count() = {incomingDF.count()}\nmatchStation.count() = {matchStation.count()}\nUnMatchStation.count() = {UnMatchStation.count()}'
        self.Logger.append('\t\t\t\t\tLookup Station completed')
        return matchStation, UnMatchStation
    
    def lookUpProduct(self, incomingDF, SourceKey_key, SourceTitle_key, StationKey_key,mappingProduct, mappingProductNoTitle):
        self.Logger.append('\t\t\t\t\tStarting Lookup Product...')
        if SourceTitle_key:
            incomingDF = incomingDF.withColumns({'key1':col(SourceKey_key),'key2':col(SourceTitle_key),'key3':col(StationKey_key)})
            mappingProductjoin = mappingProduct.withColumnsRenamed({'SourceKey':'key1','SourceTitle':'key2'})
            mappingProductNoTitlejoin = mappingProductNoTitle.withColumnsRenamed({'SourceKey':'key1','StationKey':'key3'})
            lookupCondition = ['key1','key2']
            # mappingProduct.select('CustomerCode')
            matchProduct = incomingDF.join(mappingProductjoin, on=lookupCondition, how='inner').drop('key1','key2','key3')
            UnMatchProduct  = incomingDF.join(mappingProductjoin, on=lookupCondition, how='left_anti')

            assert incomingDF.count() == matchProduct.count() + UnMatchProduct.count(), f'Error in lookup mappingProduct: incomingDF.count() != matchProduct.count() + UnMatchProduct.count(); incomingDF.count() = {incomingDF.count()}; matchProduct.count() = {matchProduct.count()}; UnMatchProduct.count() = {UnMatchProduct.count()}'

            #lookup mappingKey
            lookUpConditionNoTitle = ['key1','key3']
            matchProductNoTitle = UnMatchProduct.join(mappingProductNoTitlejoin, on=lookUpConditionNoTitle, how='inner').drop('key1','key2','key3')
            UnMatchProductNoTitle = UnMatchProduct.join(mappingProductNoTitlejoin, on=lookUpConditionNoTitle, how='left_anti').withColumn('Error',lit("Mismatch ProductKey")).drop('key1','key2','key3')
            assert UnMatchProduct.count() == matchProductNoTitle.count() + UnMatchProductNoTitle.count(), 'Error in lookup mappingProduct: UnMatchProduct.count() != matchProductNoTitle.count() + UnMatchProductNoTitle.count()'
            matchTable = matchProduct.unionByName(matchProductNoTitle)
            self.Logger.append('\t\t\t\t\tLookup Product completed')
            return matchTable, UnMatchProductNoTitle

        else: #TANKS is the only one package that not use SourceTitle
            incomingDF = incomingDF.withColumns({'key1':col(SourceKey_key),'key3':col(StationKey_key)})
            self.log['incomingDF'] = incomingDF
            self.log['mappingProduct'] = mappingProduct
            mappingProductjoin = mappingProduct.drop('StationKey').withColumnsRenamed({'SourceKey':'key1'})
            self.log['mappingProductjoin'] = mappingProductjoin
            mappingProductNoTitlejoin = mappingProductNoTitle.withColumnsRenamed({'SourceKey':'key1','StationKey':'key3'})
            lookupCondition = ['key1']
            # mappingProduct.select('CustomerCode')
            matchProduct = incomingDF.join(mappingProductjoin, on=lookupCondition, how='inner').drop('key1','key3')
            UnMatchProduct  = incomingDF.join(mappingProductjoin, on=lookupCondition, how='left_anti')

            # assert incomingDF.count() == matchProduct.count() + UnMatchProduct.count(), f'Error in lookup mappingProduct: incomingDF.count() != matchProduct.count() + UnMatchProduct.count(); incomingDF.count() = {incomingDF.count()}; matchProduct.count() = {matchProduct.count()}; UnMatchProduct.count() = {UnMatchProduct.count()}'

            #lookup mappingKey
            lookUpConditionNoTitle = ['key1','key3']
            matchProductNoTitle = UnMatchProduct.join(mappingProductNoTitlejoin, on=lookUpConditionNoTitle, how='inner').drop('key1','key3')
            UnMatchProductNoTitle = UnMatchProduct.join(mappingProductNoTitlejoin, on=lookUpConditionNoTitle, how='left_anti').withColumn('Error',lit("Mismatch ProductKey")).drop('key1','key3')
            # assert UnMatchProduct.count() == matchProductNoTitle.count() + UnMatchProductNoTitle.count(), 'Error in lookup mappingProduct: UnMatchProduct.count() != matchProductNoTitle.count() + UnMatchProductNoTitle.count()'
            self.log['matchProduct'] = matchProduct
            self.log['matchProductNoTitle'] = matchProductNoTitle
            matchTable = matchProductNoTitle.unionByName(matchProduct,allowMissingColumns=True)
            self.log['matchTable'] = matchTable

            self.Logger.append('\t\t\t\t\tLookup Product completed')

            return matchTable, UnMatchProductNoTitle
        
class LoadToFact_main(LoadToFact):
    def __init__(self, path_to_fact_table, path_to_mismatch, stagingTable, CATEGORY, SUBCATEGORY,WS_ID,SilverLH_ID, factTable,mismatchTable):
        super().__init__(path_to_fact_table, path_to_mismatch, stagingTable, CATEGORY, SUBCATEGORY,WS_ID,SilverLH_ID, factTable,mismatchTable)

    def preTransform(self):
        self.Logger.append('\t\t\t\tdata type coversion and derived column process...')

        if self.SUBCATEGORY.upper() == 'TANKS':
            if self.CATEGORY.upper() == 'FIRSTPRO':
                self.dataConversion({'GradeNumber': StringType})
                self.derivedField({'ReStationName': regexp_replace('StationName', ' ', '')})
            elif self.CATEGORY.upper() == 'FLOWCO':
                self.dataConversion({'GradeNumber': StringType})
                self.derivedField({'ReStationName': regexp_replace('StationName', ' ', '')})
        elif self.SUBCATEGORY.upper() == 'METERS':
            self.dataConversion({'GradeNumber': StringType})
            self.derivedField({'ReStationName': regexp_replace('StationName', ' ', '')})
        elif self.SUBCATEGORY.upper() == 'TRN':
            self.dataConversion({'Grade': StringType}) # it is Cast_Grade
            self.derivedField({
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
                'ReGradeTitle': when((col('GradeTitle') == "") & (col('Volume') == 0) & (col('Amount') == 0), "LUBE").otherwise(col('GradeTitle')),
                'YearKey': floor(col('TransDateKey') / 10000)
            })
        elif self.SUBCATEGORY.upper() == 'PAYMENT':
            if self.CATEGORY.upper() == 'FIRSTPRO':
                self.derivedField({
                    'PAY_Time': when(length(col('PAY_Time')) > 1, col('PAY_Time')).otherwise("999999"),
                    'End_Time': when(length(col('End_Time')) > 1, concat(regexp_replace('End_Time', ':', ''), lit('00'))).otherwise("999999"),
                    'PAY_Date': when(length(col('PAY_Date')) > 1, col('PAY_Date')).otherwise("00000000"),
                    'End_Date': when(length(col('End_Date')) > 1, col('End_Date')).otherwise("00000000"),
                    'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                    'ReStationName': regexp_replace('StationName', ' ', ''),
                })
                self.derivedField({
                    'PayTimeKey': regexp_replace('PAY_Time', ':', '')
                })
                self.derivedField({
                    'End_Time': when(length(col('End_Time')) > 1, col('End_Time')).otherwise("99999999"),
                    'PAY_Date': when(length(col('PAY_Date')) > 1, col('PAY_Date')).otherwise("00000000"),
                    'PAY_Time': when(length(col('PAY_Time')) > 1, col('PAY_Time')).otherwise("99999999"),
                    'End_Date': when(length(col('End_Date')) > 1, col('End_Date')).otherwise("00000000"),
                })
                self.derivedField({
                    'Pay_TimeKey': col('PayTimeKey').cast(IntegerType()),
                    'END_TimeKey': col('End_Time').cast(IntegerType())
                })
            elif self.CATEGORY.upper() == 'FLOWCO':
                self.derivedField({
                    'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                    'ReStationName': regexp_replace('StationName', ' ', '')
                })
        elif self.SUBCATEGORY.upper() == 'LUBE':
            self.derivedField({
                'ReStationName': regexp_replace('StationName', ' ', ''),
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00'))
            })
        elif self.SUBCATEGORY.upper() == 'AR TRANSACTIONS':
            self.derivedField({'GradeNumber': coalesce(col('GradeNumber'),lit(0)), 'GradeTitle': coalesce(col('GradeTitle'),lit(''))})
            self.dataConversion({'GradeNumber': StringType})
            self.derivedField({
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'CastLubeCode': when(col('LubeCode') == "", "0").otherwise(col('LubeCode')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
                'ReGradeTitle': when((col('GradeTitle') == "") & (col('Volume').cast(ShortType()) == 0) & (col('Amount').cast(ShortType()) == 0), "LUBE").otherwise(col('GradeTitle'))
            })
        elif self.SUBCATEGORY.upper() == 'DISCOUNT':
            self.derivedField({
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
            })
        elif self.CATEGORY.upper() == 'FIRSTPRO' and self.SUBCATEGORY.upper() == 'POINTS':
            self.derivedField({
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
                'Collect_Date': substring(col('Collect_TimeStemp'), 1, 8),
                'Collect_Time': trim(substring_index(col('Collect_TimeStemp'), ' ', -1)),
                'YearKey': floor(col('TransDateKey') / 10000)
            })
        elif self.CATEGORY.upper() == 'FIRSTPRO' and self.SUBCATEGORY.upper() == 'REFUND':
            self.derivedField({
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
            })
        elif self.SUBCATEGORY.upper() == 'FREE':
            self.derivedField({
                'ReStationName': regexp_replace('StationName', ' ', ''),
                'TransTimeKey': rpad(regexp_replace('TransTime', ':', ''),6,'0')
            })
            
        self.Logger.append('\t\t\t\tprocessed completed')

    def lookUpStation_FLOWCO_TANKS(self, incomingDF, POSCode_key, CustomerCode_key):
        self.Logger.append('\t\t\t\t\tStarting Lookup Station...')
        tmpDimstation = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/dimstation')\
                               .select(col('StationKey'),col('CustomerCode'),col('POSCode')).drop_duplicates()
        incomingDF = incomingDF.withColumns({'key1':col('POSCode'),'key2':col('CustomerCode')})
        dimstationjoin = tmpDimstation.withColumnsRenamed({'POSCode':'key1','CustomerCode':'key2'})
        lookupCondition = ['key1','key2']

        matchStation = incomingDF.join(dimstationjoin, on= lookupCondition, how='inner').drop('key1','key2')
        UnMatchStation  = incomingDF.join(dimstationjoin, on=lookupCondition, how='left_anti').withColumn('Error',lit("Mismatch StationKey")).drop('key1','key2').withColumn('StationKey',lit(None).cast(IntegerType()))
        assert incomingDF.count() == matchStation.count() + UnMatchStation.count(), f'Error in lookup station: incomingDF.count() != matchStation.count() + UnMatchStation.count();\nincomingDF.count() = {incomingDF.count()}\nmatchStation.count() = {matchStation.count()}\nUnMatchStation.count() = {UnMatchStation.count()}'
        self.Logger.append('\t\t\t\t\tLookup Station completed')
        return matchStation, UnMatchStation
    
    def lookUpProduct_FLOWCO_TANKS(self, incomingDF, mappingProduct):
        self.Logger.append('\t\t\t\t\tStarting Lookup Product...')

        incomingDF = incomingDF.withColumns({'key1':col('GradeNumber'), 'key2':trim(col('GradeTitle'))})
        mappingProductjoin = mappingProduct.withColumnsRenamed({'SourceKey':'key1','SourceTitle':'key2'}).drop_duplicates()
        lookupCondition = ['key1','key2']
        matchTable = incomingDF.join(mappingProductjoin, on=lookupCondition, how='inner').drop('key1','key2')
        UnMatchProduct  = incomingDF.join(mappingProductjoin, on=lookupCondition, how='left_anti').withColumn('Error',lit("Mismatch ProductKey")).drop('key1','key2')
        # assert incomingDF.count() == matchTable.count() + UnMatchProduct.count(), f'Error in lookup mappingProduct'

        UnMatchProduct = UnMatchProduct.filter((col('GradeNumber')==2)&(col('OpenVolume')==0)&(col('CloseVolume')==0)&(col('DeliveryVolume')==0)) 
        return matchTable, UnMatchProduct

    def lookUp(self):
        self.Logger.append('\t\t\t\tStarting Lookup process...')
        mappingProduct = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct') #.select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).drop_duplicates()
        mappingProductNoTitle = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct') #.select(col('SourceKey'),col('StationKey'),col('MappingKey')).drop_duplicates()
        mappingProduct = utils.trim_string_columns(mappingProduct)
        mappingProductNoTitle = utils.trim_string_columns(mappingProductNoTitle)

        if not (self.CATEGORY.upper() == 'FLOWCO' and self.SUBCATEGORY.upper() == 'TANKS'):
            matchStation, UnMatchStation = self.lookUpStation(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')
        else: # flowco tanks
            matchStation, UnMatchStation = self.lookUpStation_FLOWCO_TANKS(self.stagingTable, POSCode_key='POSCode', CustomerCode_key='CustomerCode')

            mappingProduct = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TANKS')).select(col('SourceKey'),col('MappingKey'), col('SourceTitle')).drop_duplicates()
            matchFinal, UnMatchProduct = self.lookUpProduct_FLOWCO_TANKS(matchStation, mappingProduct=mappingProduct)
            self.log['UnMatchStation'] = UnMatchStation
            self.log['UnMatchProduct'] = UnMatchProduct

            UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct)
            matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'ProductKey'})

        if (self.CATEGORY.upper() == 'FIRSTPRO' and self.SUBCATEGORY.upper() in ['REFUND', 'POINTS']) or (self.SUBCATEGORY.upper() in ['DISCOUNT', 'LUBE']):
            matchFinal, UnMatchFinal = matchStation, UnMatchStation
        
        elif self.SUBCATEGORY.upper() == 'FREE':
            if self.CATEGORY.upper() == 'FIRSTPRO':
                matchFinal, UnMatchFinal = matchStation.withColumns({"DeliveryId":lit(None)}), UnMatchStation.withColumns({"DeliveryId":lit(None)})
            elif self.CATEGORY.upper() == 'FLOWCO':
                matchFinal, UnMatchFinal = matchStation, UnMatchStation

        elif self.SUBCATEGORY.upper() == 'PAYMENT':
            if self.CATEGORY.upper() == 'FIRSTPRO':
                matchFinal = matchStation.withColumn('PAY_Time',col('PayTimeKey')).drop('PayTimeKey').withColumn('END_Time',col('END_TimeKey')).drop('END_TimeKey')
                UnMatchFinal = UnMatchStation
            elif self.CATEGORY.upper() == 'FLOWCO':
                matchFinal = matchStation.withColumn('PAY_Date',lit(None)).withColumn('PAY_Time',lit(None)).withColumn('END_Date',lit(None)).withColumn('END_Time',lit(None))
                UnMatchFinal = UnMatchStation.withColumn('PAY_Date',lit(None)).withColumn('PAY_Time',lit(None)).withColumn('END_Date',lit(None)).withColumn('END_Time',lit(None))

        elif self.SUBCATEGORY.upper() == 'TRN':
            if self.CATEGORY.upper() == 'FIRSTPRO':
                mappingProduct = mappingProduct.filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TRANSACTION')).select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).drop_duplicates()
                mappingProductNoTitle = mappingProductNoTitle.filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull())).select(col('SourceKey'),col('StationKey'),col('MappingKey')).drop_duplicates()
            elif self.CATEGORY.upper() == 'FLOWCO':
                mappingProduct = mappingProduct.filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TRANSACTION')).select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).drop_duplicates()
                mappingProductNoTitle = mappingProductNoTitle.filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull())).select(col('SourceKey'),col('StationKey'),col('MappingKey')).drop_duplicates()

            matchFinal, UnMatchProduct = self.lookUpProduct(matchStation, SourceKey_key='Grade', SourceTitle_key='ReGradeTitle', StationKey_key='StationKey', mappingProduct=mappingProduct, mappingProductNoTitle=mappingProductNoTitle)
            UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct).withColumn('YearKey',lit(None))

            if self.CATEGORY.upper() == 'FIRSTPRO':
                match1 = matchFinal.filter(col('Volume')>=0).orderBy('FileKey','TransNumber','CloseDateKey').drop('Volume','Amount')
                match2 = matchFinal.groupBy("CloseDateKey", "TransNumber", "FileKey").agg(
                    sum("Volume").alias("VOLUME"),
                    sum("Amount").alias("AMOUNT")  # Assuming 'Amount' is another column in your DataFrame
                ).orderBy('FileKey','TransNumber','CloseDateKey')
                matchFinal = match1.join(match2, on=['CloseDateKey', 'TransNumber', 'FileKey'], how='inner').withColumn('VOLUME',col('VOLUME').cast(DecimalType(15, 3)))

            matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'ProductKey'}).withColumn('Vat',lit(None))

        elif self.SUBCATEGORY.upper() == 'METERS':
            if self.CATEGORY.upper() == 'FIRSTPRO':
                mappingProduct = mappingProduct.filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'METERS')).select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).drop_duplicates()
                mappingProductNoTitle = mappingProductNoTitle.filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'METERS') & (col('sourcetitle').isNull())).select(col('SourceKey'),col('StationKey'),col('MappingKey')).drop_duplicates()

            elif self.CATEGORY.upper() == 'FLOWCO':
                mappingProduct = mappingProduct.filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'METERS')).select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).drop_duplicates()
                mappingProductNoTitle = mappingProductNoTitle.filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'METERS') & (col('StationKey').isNotNull())).select(col('SourceKey'),col('StationKey'),col('MappingKey')).drop_duplicates()

            matchFinal, UnMatchProduct = self.lookUpProduct(matchStation, SourceKey_key='GradeNumber', SourceTitle_key='GradeTitle', StationKey_key='StationKey', mappingProduct=mappingProduct, mappingProductNoTitle=mappingProductNoTitle)
            UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct) #.withColumn('GradeNumber',col('GradeNumber').cast(ShortType()))
            matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'ProductKey'})
        
        elif self.SUBCATEGORY.upper() == 'AR TRANSACTIONS':
            if self.CATEGORY.upper() == 'FIRSTPRO':
                mappingProduct = mappingProduct.filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'AR')).select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).drop_duplicates()
                mappingProductNoTitle = mappingProductNoTitle.filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull())).select(col('SourceKey'),col('StationKey'),col('MappingKey')).drop_duplicates()
            elif self.CATEGORY.upper() == 'FLOWCO':
                mappingProduct = mappingProduct.filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'AR')).select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).drop_duplicates()
                mappingProductNoTitle = mappingProductNoTitle.filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull())).select(col('SourceKey'),col('StationKey'),col('MappingKey')).drop_duplicates()
            
            matchFinal, UnMatchProduct = self.lookUpProduct(matchStation, SourceKey_key='GradeNumber', SourceTitle_key='ReGradeTitle', StationKey_key='StationKey', mappingProduct=mappingProduct, mappingProductNoTitle=mappingProductNoTitle)
            UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct)
            matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'GradeKey','LBAmount':'LubeAmount','AttendantNumber':'AttendeeNumber'})

        elif (self.SUBCATEGORY.upper() == 'TANKS') and (self.CATEGORY.upper() == 'FIRSTPRO'):
                mappingProduct = mappingProduct.filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TANKS')).select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).drop_duplicates()
                mappingProductNoTitle = mappingProductNoTitle.filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TANKS') & (col('StationKey').isNotNull())).select(col('SourceKey'),col('StationKey'),col('MappingKey')).drop_duplicates()
                matchFinal, UnMatchProduct = self.lookUpProduct(matchStation, SourceKey_key='GradeNumber', SourceTitle_key='GradeTitle', StationKey_key='StationKey', mappingProduct=mappingProduct, mappingProductNoTitle=mappingProductNoTitle)
                UnMatchProduct = UnMatchProduct.filter((col('GradeNumber').cast(IntegerType()) == 2) & (col('OpenVolume').cast(IntegerType()) == 0) & (col('CloseVolume').cast(IntegerType()) == 0) & (col('DeliveryVolume').cast(IntegerType()) == 0))

                UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct).drop('ReStationName')
                matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'ProductKey'})
        else:
            self.Logger.append(f'Waited to implement lookUp for {self.CATEGORY} and {self.SUBCATEGORY}')

        # if self.SUBCATEGORY.upper() == 'FREE':
        #     matchFinal.printSchema()
        #     UnMatchFinal.printSchema()
        self.matchFinal = matchFinal
        self.UnMatchFinal = UnMatchFinal

        self.UnMatchFinal = self.UnMatchFinal.select(spark.read.load(self.path_to_mismatch).limit(0).columns)
        self.UnMatchFinal = utils.copySchemaByName(self.UnMatchFinal, self.mismatchTable)
        
        self.matchFinal = self.matchFinal.select(spark.read.load(self.path_to_fact_table).limit(0).columns)
        self.matchFinal = utils.copySchemaByName(self.matchFinal, self.factTable)
        self.Logger.append('\t\t\t\tLookup process completed')

    def saveMatchTable(self,path_to_save):
        self.Logger.append(f'\t\t\t\t\tbefore save matchFinal.count() = {spark.read.load(path_to_save).count()}')
        self.matchFinal.write.mode('append').save(path_to_save)
        self.Logger.append(f'\t\t\t\t\tmatchFinal.count() = {self.matchFinal.count()} is added to {path_to_save}')
        self.Logger.append(f'\t\t\t\t\tafter save matchFinal.count() = {spark.read.load(path_to_save).count()}')

    def saveMisMatchTable(self,path_to_save):
        self.Logger.append(f'\t\t\t\t\tbefore save mismatchFinal.count() = {spark.read.load(path_to_save).count()}')
        self.UnMatchFinal.write.mode('append').save(path_to_save)
        self.Logger.append(f'\t\t\t\t\tUnMatchFinal.count() = {self.UnMatchFinal.count()} is added to {path_to_save}')
        self.Logger.append(f'\t\t\t\t\tafter save mismatchFinal.count() = {spark.read.load(path_to_save).count()}')


    def run(self):
        self.Logger.append('\t\t\tStarting Load to Fact... by loadtofactObj.run()')
        '''
        run ther following steps:
        preTransform: data conversion and derived field
        step2: lookup
        '''
        try:
            self.preTransform()
            self.lookUp()
            self.saveMatchTable(self.path_to_fact_table)
            self.saveMisMatchTable(self.path_to_mismatch)

        except Exception as e:
            self.Logger.append(f"Error in run load_to_fact: {e}")

class LoadToFact_mismatch(LoadToFact):  
    def __init__(self, path_to_fact_table, path_to_mismatch, stagingTable, CATEGORY, SUBCATEGORY,WS_ID,SilverLH_ID, factTable,mismatchTable):
        super().__init__(path_to_fact_table, path_to_mismatch, stagingTable, CATEGORY, SUBCATEGORY,WS_ID,SilverLH_ID, factTable,mismatchTable)
        self.factFile = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factfile')
        self.Logger = []
    
    def addFileNameToStagingTable(self):
        self.Logger.append('\t\t\tAdding Filname from factfile by FileKey')
        self.stagingTable = self.stagingTable.join(self.factFile.select('FileKey','FileName'), on='FileKey', how='left')

    def preTransform(self):
        # ReStationName is the station name in the cleaned way (removing white space)
        # ReGradeTitle is the grade title in the cleaned way (removing white space)
        # When storing, StationName and GradeTitle should be stored in the original way
        # These are used in joining table
        self.Logger.append('\t\t\t\tpreTransform process...')
        if self.SUBCATEGORY.upper() == 'TANKS':
            self.dataConversion({'GradeNumber': StringType})
            self.derivedField({
                'IsFlowCo': when(instr(col('FileName'), 'FLOWCO') > 0, True).otherwise(False),
                'ReStationName': regexp_replace('StationName', ' ', ''),
            })
        elif self.SUBCATEGORY.upper() == 'METERS':
            self.dataConversion({'GradeNumber': StringType})
            self.derivedField({
                'IsFlowCo': when(instr(col('FileName'), 'FLOWCO') > 0, True).otherwise(False),
                'ReStationName': regexp_replace('StationName', ' ', ''),
            })
        elif self.SUBCATEGORY.upper() == 'TRN':
            self.dataConversion(
                {
                    'Grade': StringType # Cast_Grade
                }
            )
            self.derivedField(
                {
                    'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                    'ReStationName': regexp_replace('StationName', ' ', ''),
                    'ReGradeTitle': when((col('GradeTitle') == "") & (col('Volume') == 0) & (col('Amount') == 0), "LUBE").otherwise(col('GradeTitle')),
                    'IsFlowCo': when(instr(col('FileName'), 'FLOWCO') > 0, True).otherwise(False)
                }
            )
        elif self.SUBCATEGORY.upper() == 'PAYMENT':
            self.derivedField(
                {
                    'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                    'IsFlowCo': when(instr(col('FileName'), 'FLOWCO') > 0, True).otherwise(False),
                    'ReStationName': regexp_replace('StationName', ' ', ''),
                }
            )
        elif self.SUBCATEGORY.upper() == 'LUBE':
            self.derivedField({
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'IsFlowCo': when(instr(col('FileName'), 'FLOWCO') > 0, True).otherwise(False),
                'ReStationName': regexp_replace('StationName', ' ', ''),
            })
        elif self.SUBCATEGORY.upper() == 'AR TRANSACTIONS':
            self.dataConversion(
                {
                    'GradeNumber': StringType
                }
            )
            self.derivedField(
                {
                    'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                    'CastLubeCode': when(col('LubeCode') == "", "0").otherwise(col('LubeCode')),
                    'IsFlowCo': when(instr(col('FileName'), 'FLOWCO') > 0, True).otherwise(False),
                    'ReStationName': regexp_replace('StationName', ' ', ''),
                    'ReGradeTitle': when((col('GradeTitle') == "") & (col('Volume') == 0) & (col('Amount') == 0), "LUBE").otherwise(col('GradeTitle'))
                }
            )
        elif self.SUBCATEGORY.upper() == 'DISCOUNT':
            self.derivedField({
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
            })
        elif self.SUBCATEGORY.upper() == 'POINTS':
            pass
        elif self.SUBCATEGORY.upper() == 'REFUND':
            self.derivedField({
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
            })
        self.Logger.append('\t\t\t\tpreTransform processed completed')

    def lookUp(self):
        self.Logger.append('\t\t\t\tStarting Lookup process...')
        mappingProduct = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct')#.select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).drop_duplicates()
        mappingProductNoTitle = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct')#.select(col('SourceKey'),col('StationKey'),col('MappingKey')).drop_duplicates()

        if self.SUBCATEGORY.upper() == 'POINTS':
            pass # recheck again whether ther actually is no such file
        elif self.SUBCATEGORY.upper() == 'REFUND':
            matchFinal, UnMatchFinal =      self.lookUpStation(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')
        elif self.SUBCATEGORY.upper() == 'PAYMENT':
            matchFinal, UnMatchFinal =      self.lookUpStation(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')
        elif self.SUBCATEGORY.upper() == 'DISCOUNT':
            matchFinal, UnMatchFinal =      self.lookUpStation(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')
        elif self.SUBCATEGORY.upper() == 'LUBE':
            matchFinal, UnMatchFinal =      self.lookUpStation(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')
        elif self.SUBCATEGORY.upper() == 'TRN':
            matchStation, UnMatchStation =  self.lookUpStation(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')
            self.log['matchStation'] = matchStation
            self.log['UnMatchStation'] = UnMatchStation

            self.Logger.append('FIRSTPRO') # FirstPro
            matchStation_FirstPro = matchStation.filter(~col('IsFlowCo'))
            self.Logger.append(f'matchStation_FirstPro.count() = {matchStation_FirstPro.count()}')
            self.log['matchStation_FirstPro'] = matchStation_FirstPro.cache()

            mappingProduct_FirstPro = mappingProduct.filter(
                (col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TRANSACTION')
                ).select(
                    col('SourceKey'),
                    col('SourceTitle'),
                    col('MappingKey')
                    ).drop_duplicates()

            mappingProductNoTitle_FirstPro = mappingProductNoTitle.filter(
                (col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull())
                ).select(
                    col('SourceKey'),
                    col('StationKey'),
                    col('MappingKey')
                    ).drop_duplicates()

            matchSemiFinal_FirstPro, UnMatchProduct_FirstPro = self.lookUpProduct(
                matchStation_FirstPro, 
                SourceKey_key='Grade', 
                SourceTitle_key='ReGradeTitle', 
                StationKey_key='StationKey', 
                mappingProduct=mappingProduct_FirstPro, 
                mappingProductNoTitle=mappingProductNoTitle_FirstPro
                )

            match1 = matchSemiFinal_FirstPro.filter(col('Volume')>=0).orderBy('FileKey','TransNumber','CloseDateKey').drop('Volume','Amount')
            match2 = matchSemiFinal_FirstPro.groupBy("CloseDateKey", "TransNumber", "FileKey").agg(
                    sum("Volume").alias("VOLUME"),
                    sum("Amount").alias("AMOUNT")  # Assuming 'Amount' is another column in your DataFrame
                ).orderBy('FileKey','TransNumber','CloseDateKey')
            matchFinal_FirstPro = match1.join(match2, on=['FileKey', 'TransNumber', 'CloseDateKey'], how='inner').withColumn('VOLUME',col('VOLUME').cast(DecimalType(15, 3)))
            
            self.Logger.append(f'matchFinal_FirstPro.count() = {matchFinal_FirstPro.count()}')
            self.Logger.append(f'UnMatchProduct_FirstPro.count() = {UnMatchProduct_FirstPro.count()}')

            self.Logger.append('FLOWCO') # FlowCo
            matchStation_FlowCo   = matchStation.filter(col('IsFlowCo'))
            self.Logger.append(f'matchStation_FlowCo.count() = {matchStation_FlowCo.count()}')
            
            mappingProduct_FlowCo = mappingProduct.filter(
                (col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TRANSACTION')
                ).select(
                    col('SourceKey'),
                    col('SourceTitle'),
                    col('MappingKey')
                    ).drop_duplicates()

            mappingProductNoTitle_FlowCo = mappingProductNoTitle.filter(
                (col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull())
                ).select(
                    col('SourceKey'),
                    col('StationKey'),
                    col('MappingKey')
                    ).drop_duplicates()

            matchFinal_FlowCo, UnMatchProduct_FlowCo = self.lookUpProduct(
                matchStation_FlowCo, 
                SourceKey_key='Grade', 
                SourceTitle_key='ReGradeTitle', 
                StationKey_key='StationKey', 
                mappingProduct=mappingProduct_FlowCo, 
                mappingProductNoTitle=mappingProductNoTitle_FlowCo
                )
            self.Logger.append(f'matchFinal_FlowCo.count() = {matchFinal_FlowCo.count()}')
            self.Logger.append(f'UnMatchProduct_FlowCo.count() = {UnMatchProduct_FlowCo.count()}')

            matchFinal = matchFinal_FirstPro.unionByName(matchFinal_FlowCo,allowMissingColumns=True)
            matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'ProductKey'}).withColumn('Vat',lit(None))
            self.Logger.append(f'matchFinal.count() = {matchFinal.count()}')

            UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct_FirstPro,allowMissingColumns=True).unionByName(UnMatchProduct_FlowCo,allowMissingColumns=True)

        elif self.SUBCATEGORY.upper() == 'METERS': 
            matchStation, UnMatchStation =  self.lookUpStation(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')
            self.log['matchStation'] = matchStation
            self.log['UnMatchStation'] = UnMatchStation

            self.Logger.append('FIRSTPRO') # FirstPro
            matchStation_FirstPro = matchStation.filter(~col('IsFlowCo'))
            self.Logger.append(f'matchStation_FirstPro.count() = {matchStation_FirstPro.count()}')
            self.log['matchStation_FirstPro'] = matchStation_FirstPro.cache()

            mappingProduct_FirstPro = mappingProduct.filter(
                (col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'METERS')
                ).select(
                    col('SourceKey'),
                    col('SourceTitle'),
                    col('MappingKey')
                    ).drop_duplicates()

            mappingProductNoTitle_FirstPro = mappingProductNoTitle.filter(
                (col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'METERS') & (col('StationKey').isNotNull())
                ).select(
                    col('SourceKey'),
                    col('StationKey'),
                    col('MappingKey')
                    ).drop_duplicates()

            matchFinal_FirstPro, UnMatchProduct_FirstPro = self.lookUpProduct(
                matchStation_FirstPro, 
                SourceKey_key='GradeNumber', 
                SourceTitle_key='GradeTitle', 
                StationKey_key='StationKey', 
                mappingProduct=mappingProduct_FirstPro, 
                mappingProductNoTitle=mappingProductNoTitle_FirstPro
                )

            self.Logger.append(f'matchFinal_FirstPro.count() = {matchFinal_FirstPro.count()}')
            self.Logger.append(f'UnMatchProduct_FirstPro.count() = {UnMatchProduct_FirstPro.count()}')

            self.Logger.append('FLOWCO') # FlowCo
            matchStation_FlowCo   = matchStation.filter(col('IsFlowCo'))
            self.Logger.append(f'matchStation_FlowCo.count() = {matchStation_FlowCo.count()}')

            mappingProduct_FlowCo = mappingProduct.filter(
                (col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'METERS')
                ).select(
                    col('SourceKey'),
                    col('SourceTitle'),
                    col('MappingKey')
                    ).drop_duplicates()

            mappingProductNoTitle_FlowCo = mappingProductNoTitle.filter(
                (col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'METERS') & (col('StationKey').isNotNull())
                ).select(
                    col('SourceKey'),
                    col('StationKey'),
                    col('MappingKey')
                    ).drop_duplicates()

            matchFinal_FlowCo, UnMatchProduct_FlowCo = self.lookUpProduct(
                matchStation_FlowCo, 
                SourceKey_key='GradeNumber', 
                SourceTitle_key='GradeTitle', 
                StationKey_key='StationKey', 
                mappingProduct=mappingProduct_FlowCo, 
                mappingProductNoTitle=mappingProductNoTitle_FlowCo
                )
            self.Logger.append(f'matchFinal_FlowCo.count() = {matchFinal_FlowCo.count()}')
            self.Logger.append(f'UnMatchProduct_FlowCo.count() = {UnMatchProduct_FlowCo.count()}')

            matchFinal = matchFinal_FirstPro.unionByName(matchFinal_FlowCo,allowMissingColumns=True)
            matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'ProductKey'})
            self.Logger.append(f'matchFinal.count() = {matchFinal.count()}')

            UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct_FirstPro,allowMissingColumns=True).unionByName(UnMatchProduct_FlowCo,allowMissingColumns=True)
        elif self.SUBCATEGORY.upper() == 'AR TRANSACTIONS':
            matchStation, UnMatchStation =  self.lookUpStation(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')
            self.Logger.append(f'matchStation.count() = {matchStation.count()}')
            self.Logger.append(f'UnMatchStation.count() = {UnMatchStation.count()}')
            
            self.log['matchStation'] = matchStation
            self.log['UnMatchStation'] = UnMatchStation
            
            self.Logger.append('FIRSTPRO') # FirstPro
            matchStation_FirstPro = matchStation.filter(~col('IsFlowCo'))
            self.Logger.append(f'matchStation_FirstPro.count() = {matchStation_FirstPro.count()}')
            self.log['matchStation_FirstPro'] = matchStation_FirstPro.cache()
            
            mappingProduct_FirstPro = mappingProduct.filter(
                (col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'AR')
                ).select(
                    col('SourceKey'),
                    col('SourceTitle'),
                    col('MappingKey')
                    ).drop_duplicates()

            mappingProductNoTitle_FirstPro = mappingProductNoTitle.filter(
                (col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull())
                ).select(
                    col('SourceKey'),
                    col('StationKey'),
                    col('MappingKey')
                    ).drop_duplicates()

            matchFinal_FirstPro, UnMatchProduct_FirstPro = self.lookUpProduct(
                matchStation_FirstPro, 
                SourceKey_key='GradeNumber',
                SourceTitle_key='ReGradeTitle', 
                StationKey_key='StationKey', 
                mappingProduct=mappingProduct_FirstPro, 
                mappingProductNoTitle=mappingProductNoTitle_FirstPro
                )

            self.Logger.append(f'matchFinal_FirstPro.count() = {matchFinal_FirstPro.count()}')
            self.Logger.append(f'UnMatchProduct_FirstPro.count() = {UnMatchProduct_FirstPro.count()}')

            self.Logger.append('FLOWCO') # FlowCo
            matchStation_FlowCo   = matchStation.filter(col('IsFlowCo'))
            self.Logger.append(f'matchStation_FlowCo.count() = {matchStation_FlowCo.count()}')
            
            mappingProduct_FlowCo = mappingProduct.filter(
                (col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'AR')
                ).select(
                    col('SourceKey'),
                    col('SourceTitle'),
                    col('MappingKey')
                    ).drop_duplicates()

            mappingProductNoTitle_FlowCo = mappingProductNoTitle.filter(
                (col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull())
                ).select(
                    col('SourceKey'),
                    col('StationKey'),
                    col('MappingKey')
                    ).drop_duplicates()

            matchFinal_FlowCo, UnMatchProduct_FlowCo = self.lookUpProduct(
                matchStation_FlowCo, 
                SourceKey_key='GradeNumber', 
                SourceTitle_key='ReGradeTitle', 
                StationKey_key='StationKey', 
                mappingProduct=mappingProduct_FlowCo, 
                mappingProductNoTitle=mappingProductNoTitle_FlowCo
                )
            self.Logger.append(f'matchFinal_FlowCo.count() = {matchFinal_FlowCo.count()}')
            self.Logger.append(f'UnMatchProduct_FlowCo.count() = {UnMatchProduct_FlowCo.count()}')

            matchFinal = matchFinal_FirstPro.unionByName(matchFinal_FlowCo,allowMissingColumns=True)
            matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'GradeKey','LBAmount':'LubeAmount','AttendantNumber':'AttendeeNumber'})
            self.Logger.append(f'matchFinal.count() = {matchFinal.count()}')

            UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct_FirstPro,allowMissingColumns=True).unionByName(UnMatchProduct_FlowCo,allowMissingColumns=True)
        elif self.SUBCATEGORY.upper() == 'TANKS':
            matchStation, UnMatchStation =  self.lookUpStation(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')
            
            self.Logger.append('FIRSTPRO') # FirstPro
            matchStation_FirstPro = matchStation.filter(~col('IsFlowCo'))
            self.Logger.append(f'matchStation_FirstPro.count() = {matchStation_FirstPro.count()}')
            
            mappingProduct_FirstPro = mappingProduct.filter(
                (col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TANKS')
                ).select(
                    col('SourceKey'),
                    col('SourceTitle'),
                    col('MappingKey')
                    ).drop_duplicates()

            mappingProductNoTitle_FirstPro = mappingProductNoTitle.filter(
                (col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TANKS') & (col('StationKey').isNotNull())
                ).select(
                    col('SourceKey'),
                    col('StationKey'),
                    col('MappingKey')
                    ).drop_duplicates()

            matchFinal_FirstPro, UnMatchProduct_FirstPro = self.lookUpProduct(
                matchStation_FirstPro,
                SourceKey_key='GradeNumber',
                SourceTitle_key='GradeTitle',
                StationKey_key='StationKey',
                mappingProduct=mappingProduct_FirstPro,
                mappingProductNoTitle=mappingProductNoTitle_FirstPro
                )

            self.Logger.append(f'matchFinal_FirstPro.count() = {matchFinal_FirstPro.count()}')
            self.Logger.append(f'UnMatchProduct_FirstPro.count() = {UnMatchProduct_FirstPro.count()}')

            self.Logger.append('FLOWCO') # FlowCo
            matchStation_FlowCo   = matchStation.filter(col('IsFlowCo'))
            self.Logger.append(f'matchStation_FlowCo.count() = {matchStation_FlowCo.count()}')

            mappingProduct_FlowCo = mappingProduct.filter(
                (col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TANKS')
                ).select(
                    col('SourceKey'),
                    col('StationKey'),
                    col('MappingKey')
                    ).drop_duplicates()

            mappingProductNoTitle_FlowCo = mappingProductNoTitle.filter(
                (col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TANKS') & (col('StationKey').isNotNull())
                )

            matchFinal_FlowCo, UnMatchProduct_FlowCo = self.lookUpProduct(
                matchStation_FlowCo,
                SourceKey_key='GradeNumber',
                SourceTitle_key=None,
                StationKey_key='StationKey',
                mappingProduct=mappingProduct_FlowCo,
                mappingProductNoTitle=mappingProductNoTitle_FlowCo
                )
            self.Logger.append(f'matchFinal_FlowCo.count() = {matchFinal_FlowCo.count()}')
            self.Logger.append(f'UnMatchProduct_FlowCo.count() = {UnMatchProduct_FlowCo.count()}')

            matchFinal = matchFinal_FirstPro.unionByName(matchFinal_FlowCo,allowMissingColumns=True)
            matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'ProductKey'})
            self.Logger.append(f'matchFinal.count() = {matchFinal.count()}')

            UnMatchFinal = UnMatchProduct_FlowCo.unionByName(UnMatchProduct_FirstPro,allowMissingColumns=True).filter((col('GradeNumber') == 2) & (col('OpenVolume') == 0) & (col('CloseVolume') == 0) & (col('DeliveryVolume') == 0)).unionByName(UnMatchStation,allowMissingColumns=True)
        else:
            self.Logger.append(f'Waited to implement lookUp for {self.CATEGORY} and {self.SUBCATEGORY}')

        matchFinal = matchFinal.select(spark.read.load(self.path_to_fact_table).limit(0).columns)
        matchFinal = utils.copySchemaByName(matchFinal, self.factTable)
        matchFinal.write.mode('append').save(self.path_to_fact_table)
        self.Logger.append('\t\t\t\t\tmatchFinal.count() = '+str(matchFinal.count())+ 'is added to factTable')

        UnMatchFinal = UnMatchFinal.select(spark.read.load(self.path_to_mismatch).limit(0).columns)
        UnMatchFinal = utils.copySchemaByName(UnMatchFinal, self.mismatchTable)
        UnMatchFinal.write.mode('overwrite').save(self.path_to_mismatch)
        self.Logger.append('\t\t\t\t\tUnMatchFinal.count() = '+str(UnMatchFinal.count())+ 'is saved at mismatchTable')

        self.Logger.append('\t\t\t\tLookup process completed')

    def run(self):
        self.Logger.append('\t\t\tStarting Load to Fact... by loadtofactObj.run()')
        try:
            self.addFileNameToStagingTable()
            self.preTransform()
            self.lookUp()

        except Exception as e:
            self.Logger.append(f"Error in run load_to_fact: {e}")

class FactFileHandler:

    def __init__(self, WS_ID: int, LH_ID: int):
        self.WS_ID = WS_ID
        self.LH_ID = LH_ID
        self.path_to_factfile = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID}/Tables/factfile'
        self.factFile = spark.read.load(self.path_to_factfile).withColumn('LoadStatus', col('LoadStatus').cast(ShortType())).cache()
        self.new_file_key = self.factFile.agg(max('FileKey').alias('maxFileKey')).collect()[0].maxFileKey + 1 if self.factFile.agg(max('FileKey').alias('maxFileKey')).collect()[0].maxFileKey else 1
        self.new_record = {'FileKey':self.new_file_key, 'FileName':None, 'CategoryName':None, 'SubCategoryName':None, 'LoadStatus':None}
        self.log = {}

    def __str__(self):
        return str(self.new_record)

    def getLastId_from_lake(self):
        maxFileKey = self.factFile.selectExpr('MAX(FileKey) as maxFileKey').collect()[0].maxFileKey #spark.sql("SELECT MAX(FileKey) as maxFileKey FROM SilverLH.factfile").collect()[0].maxFileKey
        LastId = maxFileKey + 1
        return LastId

    def reload(self):
        self.factFile = spark.read.load(self.path_to_factfile).withColumn('LoadStatus', col('LoadStatus').cast(ShortType()))
    
    def saveTable(self):
        self.factFile.withColumn('LoadStatus', col('LoadStatus').cast(ShortType())).write.mode('overwrite').partitionBy(['SubCategoryName', 'CategoryName', 'LoadStatus']).option('mergeSchema','true').option('mergeSchema','true').save(self.path_to_factfile)

    
    def addNewRecord(self):
        current_date_list = [int(x) for x in (datetime.now() + timedelta(hours=7)).strftime('%Y%m%d %H%M%S').split(' ')]
        new_row = Row(
            FileKey=self.new_record['FileKey'],
            FileName=self.new_record['FileName'],
            CategoryName=self.new_record['CategoryName'],
            SubCategoryName=self.new_record['SubCategoryName'],
            LoadDateKey=current_date_list[0],
            LoadTimeKey=current_date_list[1],
            LoadStatus=self.new_record['LoadStatus']
            )
        new_row_df = spark.createDataFrame([new_row], schema = self.factFile.schema)
        new_row_df.withColumn('LoadStatus', col('LoadStatus').cast(ShortType())).write.mode('append').partitionBy(['SubCategoryName', 'CategoryName', 'LoadStatus']).option('mergeSchema','true').option('mergeSchema','true').save(self.path_to_factfile)
        self.reload()
    
    def editRecord_byFileKey(self,FileKey: int,keyChange: Dict[str,Any] ,LoadStatusList: List[int] = None) -> None:
        condition = (self.factFile.FileKey == FileKey)

        if LoadStatusList:
            condition = condition & (self.factFile.LoadStatus.isin(LoadStatusList))

        oldRowToUpdate = self.factFile.filter(col('FileKey')==FileKey)
        numCheck = oldRowToUpdate.count()
        assert numCheck != 0, f'No records of FileKey = {FileKey}'
        assert numCheck == 1, f'There are {numCheck} rows for FileKey = {FileKey}; It\'s weird'

        self.factFile = self.factFile.withColumns({column: when(col('FileKey')==FileKey, keyChange[column]).otherwise(col(column)) for column in keyChange})
        self.saveTable()
        self.reload()

    def set_new_record(self,FileName,CategoryName,SubCategoryName,LoadStatus):
        self.new_record['FileName'] = FileName
        self.new_record['CategoryName'] = CategoryName
        self.new_record['SubCategoryName'] = SubCategoryName
        self.new_record['LoadStatus'] = LoadStatus

    def addNewManyRecords(self,FILENAME_FILEKEY_mapper,CategoryName,SubCategoryName,LoadStatus):
        current_date_list = [int(x) for x in (datetime.now() + timedelta(hours=7)).strftime('%Y%m%d %H%M%S').split(' ')]

        self.records = []
        for FILE_NAME in FILENAME_FILEKEY_mapper:

            self.records.append(
                Row(
                    FileKey=FILENAME_FILEKEY_mapper[FILE_NAME],
                    FileName=FILE_NAME,
                    CategoryName=CategoryName,
                    SubCategoryName=SubCategoryName,
                    LoadDateKey=current_date_list[0],
                    LoadTimeKey=current_date_list[1],
                    LoadStatus=LoadStatus
                )
            )

        self.new_rows_df = spark.createDataFrame(self.records, schema = self.factFile.schema)
        self.new_rows_df = self.new_rows_df.withColumn('LoadStatus', col('LoadStatus').cast(ShortType()))
        self.log['new_rows_df'] = self.new_rows_df
        self.new_rows_df.write.mode('append').partitionBy(['SubCategoryName', 'CategoryName', 'LoadStatus']).option('mergeSchema','true').option('mergeSchema','true').save(self.path_to_factfile)
        self.reload()

    def EditManyRecords(self, FILENAME_FILEKEY_mapper, LoadStatus):
        self.reload()
        self.factFile = self.factFile.withColumn(
            'LoadStatus',
            when(col('FileKey').isin(list(FILENAME_FILEKEY_mapper.values())), LoadStatus).otherwise(col('LoadStatus'))
        )
        success = False
        retries = 0
        while not success and retries < 10:
            try:
                self.factFile.withColumn('LoadStatus', col('LoadStatus').cast(ShortType())).write.mode('overwrite').partitionBy(['SubCategoryName', 'CategoryName', 'LoadStatus']).option('mergeSchema','true').option('mergeSchema','true').save(self.path_to_factfile)
                success = True
            except:
                retries += 1
                time.sleep(5)
        if not success:
            raise Exception("Failed to update LoadStatus in FactFile after 10 retries.")
        self.reload()

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
        incomingDF = incomingDF.withColumns({'key1':col(SourceKey_key),'key2':col(StationKey_key)})
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
            mappingProductNoTitle = mappingProductNoTitle.filter(self.add_condition_no_title_mapping[(CATEGORY, SUBCATEGORY)]).select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).drop_duplicates()
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
        if stagingTable_firstpro.count() > 0:
            match_station_table_firstpro, not_match_station_table_firstpro = self.look_up_stationKey(stagingTable_firstpro, **self.lookupstation_key[('FIRSTPRO', self.SUBCATEGORY)])
            match_product, not_match_product = self.look_up_product(incomingDF = match_station_table_firstpro, CATEGORY = 'FIRSTPRO', SUBCATEGORY=self.SUBCATEGORY, **self.lookupproduct_key.get(('FIRSTPRO', self.SUBCATEGORY), {'SourceKey_key': None, 'SourceTitle_key':None}))
            match_product_no_title, not_match_product_no_title = self.look_up_product_no_title(incomingDF = match_product, CATEGORY = 'FIRSTPRO', SUBCATEGORY=self.SUBCATEGORY, **self.lookupproduct_no_title_key.get(('FIRSTPRO', self.SUBCATEGORY),{'SourceKey_key':None, 'StationKey_key':None}))
            
            match_table = match_product_no_title
            mismatch_table = not_match_station_table_firstpro
            
            if not_match_product:
                mismatch_table = mismatch_table.unionByName(not_match_product,allowMissingColumns=True)
            if not_match_product_no_title:  
                mismatch_table = mismatch_table.unionByName(not_match_product_no_title,allowMissingColumns=True)

            return (match_table, mismatch_table)
        else:
            return (None, None)

    def FLOWCO(self, stagingTable_flowco):
        if stagingTable_flowco.count() > 0:
            match_staion_table_flowco, not_match_staion_table_flowco = self.look_up_stationKey(stagingTable_flowco, **self.lookupstation_key[('FLOWCO', self.SUBCATEGORY)])
            match_product, not_match_product = self.look_up_product(incomingDF = match_staion_table_flowco, CATEGORY = 'FLOWCO', SUBCATEGORY=self.SUBCATEGORY, **self.lookupproduct_key.get(('FLOWCO', self.SUBCATEGORY), {'SourceKey_key': None, 'SourceTitle_key':None}))
            match_product_no_title, not_match_product_no_title = self.look_up_product_no_title(incomingDF = match_product, CATEGORY = 'FLOWCO', SUBCATEGORY=self.SUBCATEGORY, **self.lookupproduct_no_title_key.get(('FLOWCO', self.SUBCATEGORY),{'SourceKey_key':None, 'StationKey_key':None}))
            match_table = match_product_no_title

            mismatch_table = not_match_staion_table_flowco
            if not_match_product:
                mismatch_table = mismatch_table.unionByName(not_match_product,allowMissingColumns=True)
            if not_match_product_no_title:  
                mismatch_table = mismatch_table.unionByName(not_match_product_no_title,allowMissingColumns=True)

            return (match_table, mismatch_table)
        else:
            return (None, None)
        
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
    
    def rename_mappingKey(self, match_table):
        if self.SUBCATEGORY in self.mappingkey_rename.keys():
            match_table = match_table.withColumnsRenamed(self.mappingkey_rename[self.SUBCATEGORY])
        return match_table

    def saveMatchTable(self,match_table,path_to_save):
        facttable = spark.read.load(path_to_save)
        match_table = utils.copySchemaByName(match_table, facttable).select(facttable.columns)
        match_table.write.mode('append').save(path_to_save)

    def saveMisMatchTable(self, mismatch_table, path_to_save):
        mismatchtable = spark.read.load(path_to_save)
        mismatch_table = utils.copySchemaByName(mismatch_table, mismatchtable).select(mismatchtable.columns)
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

class POS_mismatch(POS_load_to_fact):
    pass
























class ETLModule_POS(POS):
    dev = None
    def __init__(self, config):
        # assert , keys in config, 'Missing keys in config'
        super().__init__(config)
        self.FILE_PATH_ON_LAKE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Ingest/POS/{self.CATEGORY}/{self.SUBCATEGORY}/'
        self.BadFilesDir =  f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/BadFiles/POS/{self.CATEGORY}/{self.SUBCATEGORY}/'
        self.ProcessedDir = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Processed/POS/{self.CATEGORY}/{self.SUBCATEGORY}/'
        self.BlankdDir = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/BlankFiles/POS/{self.CATEGORY}/{self.SUBCATEGORY}/'
        MAPPING = {'PMT':'PAYMENT',
                    'LUB':'LUBE',
                    'AR_TRANSACTIONS':'AR TRANSACTIONS',
                    'DSC':'DISCOUNT',
                    'EOD_METERS':'METERS',
                    'EOD_TANKS':'TANKS'}
        config['SUBCATEGORY'] = MAPPING.get(config['SUBCATEGORY'],config['SUBCATEGORY'])
        self.SUBCATEGORY = config['SUBCATEGORY']
        self.list_Processed_file = self.FactFileHandler.factFile.filter((col('CategoryName')==self.CATEGORY)&(col('SubCategoryName')==self.SUBCATEGORY)).select('FileName').collect()
        self.list_Processed_file = [ROW.FileName for ROW in self.list_Processed_file]
        self.Logger = []
        self.sameFile = []
        # For Fisrt Time
        if not notebookutils.fs.exists(self.ProcessedDir):
            notebookutils.fs.mkdirs(self.ProcessedDir)
        if not notebookutils.fs.exists(self.BadFilesDir):
            notebookutils.fs.mkdirs(self.BadFilesDir)

        if self.dev:
            self.Logger.append("===============================================================")
            self.Logger.append(f'Running {self.CATEGORY} | {self.SUBCATEGORY} in dev mode')
            self.Logger.append("===============================================================")
        else:
            self.Logger.append("===============================================================")
            self.Logger.append(f'Running {self.CATEGORY} | {self.SUBCATEGORY}')
            self.Logger.append("===============================================================")

    # def getLastId(self):
    #     self.LastId = self.FactFileHandler.new_file_key
    #     return True

    def readFilePos(self, names):
        self.current_date_list = [int(x) for x in (datetime.now() + timedelta(hours=7)).strftime('%Y%m%d %H%M%S').split(' ')]
        # return pd.read_csv(self.FILE_PATH_ON_LAKE,encoding='TIS-620',delimiter='|',header=None,names=names)
        df = spark.read.format("csv") \
            .option("encoding", "TIS-620").option("delimiter", "|") \
            .load(self.FILE_PATH_ON_LAKE)
            
        if df.count() != 0:
            df = df.toDF(*names) \
                .withColumn("FilePath", input_file_name()) \
                .withColumn("CategoryName", lit(self.CATEGORY)).withColumn("SubCategoryName", lit(self.SUBCATEGORY))\
                .withColumn("FileName", regexp_replace(regexp_extract("filepath", r".*/([^/?]+)", 1), "%23", "#"))\
                .withColumn("LoadDateKey", lit(current_date_list[0])).withColumn("LoadTimeKey", lit(current_date_list[1]))
        else:
            df = self.stagingSchema\
                .withColumn("FilePath", input_file_name()) \
                .withColumn("CategoryName", lit(self.CATEGORY)).withColumn("SubCategoryName", lit(self.SUBCATEGORY))\
                .withColumn("FileName", regexp_replace(regexp_extract("filepath", r".*/([^/?]+)", 1), "%23", "#"))\
                .withColumn("LoadDateKey", lit(current_date_list[0])).withColumn("LoadTimeKey", lit(current_date_list[1]))
            
        return df

    def getstagingFromRaw(self):
        match self.SUBCATEGORY.upper():
            case 'TRN':
                columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","Dispenser","Hose","Grade","GradeTitle","UnitPrice","Volume","Amount","AttendeeNumber","ChiefNumber"]        
            case 'PAYMENT':
                if self.CATEGORY == 'FIRSTPRO':
                    columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","PMCode","ReceiveAmount","ARNumber","LPNumber","DocCode","PAY_Date","PAY_Time","End_Date","End_Time"]
                elif self.CATEGORY == 'FLOWCO':
                    columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","PMCode","ReceiveAmount","ARNumber","LPNumber","DocCode"]
            case 'LUBE':
                columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","LubeCode","LubeName","UnitPrice","Quantity","Amount"]
            case 'AR TRANSACTIONS':
                columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","Dispenser","Hose","GradeNumber","GradeTitle","GradePrice","Volume","Amount","LubeCode","UnitPrice","Quantity","LBAmount","PMCode","ARNumber","ARName","LicensePlate","DocCode","ODOMeter","AttendantNumber","DONumber"]
            case 'DISCOUNT':
                columns = ["KeyId","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","DCCode","DCAmount"]
            case 'METERS':
                if self.CATEGORY == 'FLOWCO':
                    columns = ["KeyId","CustomerCode","POSCode","StationName","CloseDateKey","ShiftNumber","DispenserNumber","HoseNumber","TankNumber","GradeNumber","GradeTitle","GradePrice","StartVolume","EndVolume","StartAmount","EndAmount","TestVolume","UsName"]
                elif self.CATEGORY == 'FIRSTPRO':
                    columns = ["KeyId","CustomerCode","POSCode","StationName","CloseDateKey","TankNumber","DispenserNumber","HoseNumber","ShiftNumber","GradeNumber","GradeTitle","GradePrice","StartVolume","EndVolume","StartAmount","EndAmount","TestVolume","UsName"]
            case 'POINTS':
                columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","PMCode","Terminal_ID","Merchant_Name","Batch_Number","Card_Trace_Number","Card_Number","Available_Balance","Collect_Point","Redeem_Point","Collect_TimeStemp"]
            case 'REFUND':
                columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","Volume","Amount","Vat"]
            case 'TANKS': # TANKS
                columns = ["KeyID","CustomerCode","POSCode","StationName","StartDateKey","TankNumber","GradeNumber","GradeTitle","OpenVolume","CloseVolume","DeliveryVolume","EndDateKey"]
            case 'FREE':
                if self.CATEGORY == 'FLOWCO':
                    columns = ['KeyID','CustomerCode','POSCode','StationName','EOD_Date','ReceiptNumber','TransDateKey','TransTime','TransNumber','FreeItemNo','FreeItemName','FreeQTY','FreeTopupQTY','FreeMemberQTY','FreeRemainQTY','ReceiptItemQTY','MemberCard','LoyaltyRedeemQTY','Rate','AccrualPoint','LoyaltyProductCode','LoyaltyProductName','Promotion','DeliveryId']
                elif self.CATEGORY == 'FIRSTPRO':
                    columns = ['KeyID','CustomerCode','POSCode','StationName','EOD_Date','ReceiptNumber','TransDateKey','TransTime','TransNumber','FreeItemNo','FreeItemName','FreeQTY','FreeTopupQTY','FreeMemberQTY','FreeRemainQTY','ReceiptItemQTY','MemberCard','LoyaltyRedeemQTY','Rate','AccrualPoint','LoyaltyProductCode','LoyaltyProductName','Promotion']
            case _:
                raise TypeError('NOT CORRECT SUBCATEGORY')

        self.Logger.append('\tFile is already being read ...')
        df = self.readFilePos(names = columns)
        self.Logger.append('\tFile is already read')

        window_spec = Window.partitionBy("FileName").orderBy(col("FileKey").desc())
        self.FactFile = spark.read.load(self.FactFileHandler.path_to_factfile)
        self.tmpFactFile = self.FactFile.filter((col('CategoryName')==self.CATEGORY)&(col('SubcategoryName')==self.SUBCATEGORY)&(col('LoadStatus').isNull()))
        self.tmpFactFile = self.tmpFactFile.withColumn("RowNum", row_number().over(window_spec))
        self.lookUpFileKey = self.tmpFactFile.filter(col('RowNum')==1).select('FileName','FileKey').withColumn('FileKey',col('FileKey').cast(IntegerType()))
        df = df.join(self.lookUpFileKey, on='FileName', how='left')
        # df = df.withColumn('FileKey_',generate_filekey_udf(df['filename'], df['load_date']))

        if self.SUBCATEGORY == 'TRN':
            df = df.withColumn('YearKey',lit(None)) # df['YearKey'] = None

        self.log['rawFile'] = df
        
        if df.count() != 0:
            self.Logger.append('\t\tFile is not blank')
        else:
            self.Logger.append('\t\tFile is blank')

        self.stagingTable = utils.copySchemaByName(df,self.stagingSchema) #spark.createDataFrame([],schema=self.stagingSchema.schema)
        if self.CATEGORY == 'FLOWCO' and self.SUBCATEGORY == 'PAYMENT':
            self.stagingTable.withColumns({"PAY_Date":lit(None),"PAY_Time":lit(None),"End_Date":lit(None),"End_Time":lit(None)}).select(self.stagingSchema.columns).write.mode('overwrite').save(self.path_to_staging)
        elif self.CATEGORY == 'FIRSTPRO' and self.SUBCATEGORY == 'FREE':
             self.stagingTable.withColumns({"DeliveryId":lit(None)}).select(self.stagingSchema.columns).write.mode('overwrite').save(self.path_to_staging)
        else:
            self.stagingTable.select(self.stagingSchema.columns).write.mode('overwrite').save(self.path_to_staging)

        # self.stagingTable = spark.createDataFrame(df[self.stagingSchema.columns],schema=self.stagingSchema.schema)
        self.stagingTable = utils.fillNaAll(self.stagingTable)
        self.stagingTable = utils.trim_string_columns(self.stagingTable)
        self.stagingTable = self.stagingTable.withColumn('CustomerCode', col('CustomerCode').cast(StringType()))

        self.stagingTable = self.stagingTable.cache()
        
        self.log['stagingTable'] = self.stagingTable
        self.FileNameList = self.stagingTable.select('FileName').distinct().collect()
        self.FileNameList = [ROW.FileName for ROW in self.FileNameList]
        mappers = self.stagingTable.select('FileName','FileKey').distinct()
        mappers = mappers.cache().collect()

        self.FILENAME_FILEKEY_mapper = {row['FileName']: row['FileKey'] for row in mappers}
        self.Logger.append('\tstagingTable is created')

        return True

    def addToFactFile(self, FILENAME_FILEKEY_mapper, status_code):
        # do in batch
        self.FactFileHandler.addNewManyRecords(
            FILENAME_FILEKEY_mapper=FILENAME_FILEKEY_mapper,
            CategoryName=self.CATEGORY, 
            SubCategoryName=self.SUBCATEGORY, 
            LoadStatus=status_code
            )
        return True

    def move_to_badfile(self, FileNameList):
        # `FileNameList`` 
        self.Logger.append('move all to badfiles')
        if not self.dev:
            if not notebookutils.fs.exists(self.BadFilesDir):
                notebookutils.fs.mkdirs(self.BadFilesDir)
            for FILE_NAME in FileNameList:
                notebookutils.fs.mv(os.path.join(self.FILE_PATH_ON_LAKE,FILE_NAME),self.BadFilesDir)
        return True

    def load_staging(self):
        self.Logger.append("\tStarting Load Staging process...")

        if not self.truncate_staging():
            raise Exception("Failed to truncate staging.")

        if not self.getstagingFromRaw():
            raise Exception("Failed to load data to staging.")
        self.Logger.append(f"\tLoad to Staging complete: size of staging = {self.stagingTable.count()}")

    def updateGradeAndHose(self):
        self.Logger.append('\t\tupdateGradeAndHose...: but no implementation (in SSIS, it is commented out)')
        pass

    def load_to_fact(self):
        self.Logger.append('\t\tload to fact...')
        # TODO []: add load to fact function
        self.loadtofactObj = LoadToFact_main(self.path_to_fact_table, self.path_to_mismatch, self.stagingTable, self.CATEGORY, self.SUBCATEGORY,self.WS_ID,self.SilverLH_ID, self.factTable, self.mismatchTable)
        self.Logger.append('\t\t\tStarting Load to Fact are going to run()')
        self.loadtofactObj.run()
        self.Logger = self.Logger + self.loadtofactObj.Logger

    def remove_from_fact(self):
        self.Logger.append('\t\tremove_from_fact processing')
        factTable_remaining = self.factTable.join(self.rows_to_delete.select("FileKey"), on="FileKey", how="left_anti")
        factTable_remaining.write.mode("overwrite").save(self.path_to_fact_table)
        self.factTable = spark.read.load(self.path_to_fact_table)

    def remove_from_mismatch(self):
        self.Logger.append('\t\tremove_from_mismatch processing')
        mismatchTable_remaining = self.mismatchTable.join(self.rows_to_delete.select("FileKey"), on="FileKey", how="left_anti")
        mismatchTable_remaining.write.mode("overwrite").save(self.path_to_mismatch)
        self.mismatchTable = spark.read.load(self.path_to_mismatch)

    def updateFactFile_in_remove_previous(self):
        self.Logger.append('updateFactFile_in_remove_previous')
        rows_to_update = self.rows_to_delete.filter((col('LoadStatus').isin([1,3,5,-99]))|(col('LoadStatus').isNull())).select("FileKey").collect()
        rows_to_update = [r.FileKey for r in rows_to_update]
        self.FactFileHandler.factFile = self.FactFileHandler.factFile.withColumn('LoadStatus',when(col('FileKey').isin(rows_to_update), 4).otherwise(col('LoadStatus')))
        self.FactFileHandler.saveTable()

    def remove_previous_data(self):
        self.Logger.append('\t\tremove_previous_data processing')
        window_spec = Window.partitionBy("FileName").orderBy(coalesce(col("FileKey"),lit(0)).desc())
        self.rows_to_delete = self.FactFileHandler.factFile\
                                    .filter(col("FileName").isin(self.sameFile))\
                                    .withColumn("row_number", row_number().over(window_spec))\
                                    .filter((col("row_number") > 1)) \
                                    .select("FileKey", 'LoadStatus')
    
        self.remove_from_fact()
        self.remove_from_mismatch()
        
    def move_to_processed_old(self,FILENAME_FILEKEY_mapper):
        """
        Move the file to the processed directory.
        """
        self.FILENAME_FILEKEY_mapper_succes = {}
        self.FILENAME_FILEKEY_mapper_fail = {}
        self.Logger.append(f'\t\tmove file to processed ...: num of file = {len(self.FileNameList)}')

        for FILE_NAME in self.FileNameList:
            if FILE_NAME in self.list_Processed_file:
                self.sameFile.append(FILE_NAME)

            try:
                if not self.dev:
                    notebookutils.fs.mv(os.path.join(self.FILE_PATH_ON_LAKE,FILE_NAME),self.ProcessedDir,create_path=True, overwrite=True)
                    self.FILENAME_FILEKEY_mapper_succes[FILE_NAME] = FILENAME_FILEKEY_mapper[FILE_NAME]
            except:
                self.FILENAME_FILEKEY_mapper_fail[FILE_NAME] = FILENAME_FILEKEY_mapper[FILE_NAME]

    def move_to_processed(self,FILENAME_FILEKEY_mapper):
        """
        Move the file to the processed directory: in batch.
        """
        self.FILENAME_FILEKEY_mapper_succes = {}
        self.FILENAME_FILEKEY_mapper_fail = {}
        self.Logger.append(f'\t\tmove file to processed ...: num of file = {len(self.FileNameList)}')

        for FILE_NAME in self.FileNameList:
            # print(self.list_Processed_file)
            if FILE_NAME in self.list_Processed_file:
                self.sameFile.append(FILE_NAME)

            try:
                if not self.dev:
                    notebookutils.fs.mv(os.path.join(self.FILE_PATH_ON_LAKE,FILE_NAME),self.ProcessedDir,create_path=True, overwrite=True)
                    self.FILENAME_FILEKEY_mapper_succes[FILE_NAME] = FILENAME_FILEKEY_mapper[FILE_NAME]
            except:
                self.FILENAME_FILEKEY_mapper_fail[FILE_NAME] = FILENAME_FILEKEY_mapper[FILE_NAME]
        
        # now we have a list of existing filename (sameFile)

        self.Logger.append(f'\t\tnum file that have ever come = {len(self.sameFile)}')
        
        # print('\t\t updating fact file ...') #move outside to do in batch
        # self.addToFactFile(self.FILENAME_FILEKEY_mapper_succes, 1)
        # self.addToFactFile(self.FILENAME_FILEKEY_mapper_fail, 3)
        # self.EditFactFile(, 1) #ETL success and move file to processed
        # self.EditFactFile(self.FILENAME_FILEKEY_mapper_fail, 3) #ETL success but move file to processed failed
        # self.FactFileHandler.EditManyRecords(
        #     FILENAME_FILEKEY_mapper=self.FILENAME_FILEKEY_mapper_succes,
        #     LoadStatus=1
        #     )
        # self.FactFileHandler.EditManyRecords(
        #     FILENAME_FILEKEY_mapper=self.FILENAME_FILEKEY_mapper_fail,
        #     LoadStatus=3
        #     )
        return True

    def moveBlankFile(self):
        for FILE_INFO in notebookutils.fs.ls(self.FILE_PATH_ON_LAKE):
            if FILE_INFO.size == 0:
                notebookutils.fs.mv(FILE_INFO.path,os.path.join(self.BlankdDir,FILE_INFO.name),create_path=True, overwrite=True)

    def move_file(self):
        if not self.move_to_processed(self.FILENAME_FILEKEY_mapper): #if move_to_processed is error
            raise AssertionError('error in move_to_processed with load status = 3')

    def main_ETL(self):
        if self.SUBCATEGORY.upper() in ['TRN', 'AR', ]: # TODO: add package need `updateGradeAndHose`
            self.Logger.append("\tStarting UPDATE GRADE AND HOSE")
            self.updateGradeAndHose()
        self.Logger.append("\tStarting main ETL process...")
        self.load_to_fact()

    def updateFactFile_final(self):
        self.Logger.append('updateFactFile_final')
        self.FactFileHandler.EditManyRecords(
            FILENAME_FILEKEY_mapper=self.FILENAME_FILEKEY_mapper_succes,
            LoadStatus=1
            )
        self.FactFileHandler.EditManyRecords(
            FILENAME_FILEKEY_mapper=self.FILENAME_FILEKEY_mapper_fail,
            LoadStatus=3
            )

    def post_ETL(self):
        self.Logger.append('Start post_ETL... to edit factfile')
        self.updateFactFile_in_remove_previous()
        self.updateFactFile_final()
        self.Logger.append('post_ETL complete')

    def run_ETL(self):
        self.Logger.append("Starting ETL process...")
        try:
            self.moveBlankFile()
            self.load_staging()
            try:
                self.main_ETL()
                self.move_file()
                self.remove_previous_data()
                self.Logger.append("ETL process completed successfully.")
                self.Logger.append("========================================================================================")
                print('\n'.join(self.Logger))
                return self # to call post_ETL outside sequential

            except Exception as e:
                self.Logger.append(f"\t\tmain ETL process failed: {e}")
                # self.move_to_badfile(self.FileNameList)
                self.addToFactFile(self.FILENAME_FILEKEY_mapper, -99)
                self.Logger.append("========================================================================================")
                return self
            
        except Exception as e:
            self.Logger.append(f"Load Staging process failed: {e}")
            try:
                # self.move_to_badfile(self.FileNameList)
                return self
            except:
                return self
            # self.addToFactFile(2)
            self.Logger.append("========================================================================================")
            return self

class MismatchModule_POS(POS):
    dev = None
    def __init__(self, config):
        super().__init__(config)
        MAPPING = {'PMT':'PAYMENT',
                    'LUB':'LUBE',
                    'AR_TRANSACTIONS':'AR TRANSACTIONS',
                    'DSC':'DISCOUNT',
                    'EOD_METERS':'METERS',
                    'EOD_TANKS':'TANKS'}
        config['SUBCATEGORY'] = MAPPING.get(config['SUBCATEGORY'],config['SUBCATEGORY'])
        self.SUBCATEGORY = config['SUBCATEGORY']
        self.Logger = []

        if self.dev:
            self.Logger.append("===============================================================")
            self.Logger.append(f'Running Mismatch {self.SUBCATEGORY} in dev mode')
            self.Logger.append("===============================================================")
        else:
            self.Logger.append("===============================================================")
            self.Logger.append(f'Running Mismatch {self.SUBCATEGORY}')
            self.Logger.append("===============================================================")

    def truncate_mismatch(self):
        spark.read.load(self.path_to_mismatch).limit(0).write.mode('overwrite').save(self.path_to_mismatch)

    def getstagingFromMismatch(self):
        # the mismatch table contains an `Error` column to indicate the error message
        # when loading to stagingTable from mismatchTable, we need to drop the `Error` column (no need anymore)

        # to prevent the replacing of the old data so we replicate data in mismatch table to the staging table and process on such staging (not mismatch)
        # once the process is done, we will overwrite the data in the mismatch table with the new data in the staging table
        spark.read.load(self.path_to_mismatch).select(self.stagingTable.columns).drop('Error').write.mode('overwrite').save(self.path_to_staging) 

        self.stagingTable = spark.read.load(self.path_to_staging)
        self.stagingTable = utils.copySchemaByName(self.stagingTable,self.stagingSchema)
        self.stagingTable = utils.fillNaAll(self.stagingTable)
        self.stagingTable = utils.trim_string_columns(self.stagingTable)

        # self.truncate_mismatch()
        return True

    def updateLoadStatusTo1(self):
        self.Logger.append("Starting update LoadStatus in FactFile...")
        fileKeyUpdate = spark.sql(f"""
                                SELECT FileKey
                                FROM BronzeLH_POS.{self.STAGING_TABLE_NAME}
                                WHERE FileKey NOT IN (
                                    SELECT FileKey
                                    FROM SilverLH_POS.{self.MIS_TABLE_NAME}
                                )
                                """)

        toUpdate = self.FactFileHandler.factFile.join(fileKeyUpdate, on='FileKey', how='inner').withColumn('LoadStatus', lit(1).cast(ShortType()))
        toSame = self.FactFileHandler.factFile.join(fileKeyUpdate, on='FileKey', how='left_anti')
        newFactFile = toUpdate.unionByName(toSame)

        success = False
        retries = 0
        while not success and retries < 10:
            try:
                newFactFile.write.mode('overwrite').partitionBy(['SubCategoryName', 'CategoryName', 'LoadStatus']).option('mergeSchema','true').option('mergeSchema','true').save(self.FactFileHandler.path_to_factfile)
                success = True
            except:
                retries += 1
                time.sleep(5)
        if not success:
            raise Exception("Failed to update LoadStatus in FactFile after 5 retries.")

        self.FactFileHandler.reload()

    def load_staging_mismatch(self):
        self.Logger.append("Starting Mismatch process...")
        self.Logger.append("\tStarting Load Staging from mismatch process...")

        if not self.truncate_staging():
            raise Exception("Failed to truncate staging.")

        if not self.getstagingFromMismatch():
            raise Exception("Failed to load data to staging.")
        self.Logger.append(f"\tLoad to Staging complete: size of staging = {self.stagingTable.count()}")
        self.Logger.append('\tLoad Staging process completed successfully')

    def load_to_fact_mismatch(self):
        self.Logger.append("Starting main ETL process...")
        self.Logger.append('\t\tload to fact for mismatch...')
        self.loadtofactObj = LoadToFact_mismatch(self.path_to_fact_table, self.path_to_mismatch, self.stagingTable, self.CATEGORY, self.SUBCATEGORY,self.WS_ID,self.SilverLH_ID, self.factTable, self.mismatchTable)
        self.Logger.append('\t\t\tStarting Load to Fact for mismatch are going to run()')
        self.loadtofactObj.run()
        self.Logger = self.Logger + self.loadtofactObj.Logger

    def runMisMatch(self):
        '''
        Executor for Mismatch process
        '''
        
        self.load_staging_mismatch() # load from mismatch table -> staging        
        self.load_to_fact_mismatch()
        # self.updateLoadStatusTo1()
        self.Logger.append("ETL mismatch process completed successfully.")

        return self #to call updateLoadStatusTo1 outside sequential



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
                        substring(col('CheckTime').cast(StringType()), 1, 4),
                        substring(col('CheckTime').cast(StringType()), 6, 2)).cast(IntegerType())
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
        if self.CODE == '1000020':
            self.MISMATCH_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/bgn_mismatch_SAP_1000020'

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
        if self.CODE == '1106070':
            df = df[df['Text'].str.contains('CPICK')]
        elif self.CODE == '1000020':
            df = df[df['G_L'].str.len()>=1]
        else:
            raise NameError("Not Correct CODE")
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
        if self.CODE == '1106070':
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
        elif self.CODE == '1000020':
            df = df.withColumns(
                    {
                        'Amount_DOC_Curr': col('AmountCURR').cast(DecimalType(18,2)),
                        'DocumentNo': col('DocumentNo').cast(StringType()),
                    }
                ).withColumnsRenamed(
                    {
                        'ETL_Date':'ETL_DATE',
                        'Ty':'Type',
                        'DocDate': 'DOC_Date',
                        'DocumentNo': 'DOC_No',
                        'AssignmentDate':'AssignmentDateKey',
                        'POSNumber': 'StationCode'
                    }
                )
            df_not_match = df.filter(length(col('AssignmentDateKey'))!=8).select(spark.read.load(self.STAGING_TABLE_PATH).columns).withColumn('Error', lit("MismatchAssignment"))
            df_not_match.write.mode('append').save(self.MISMATCH_PATH)
            df = df.filter(length(col('AssignmentDateKey'))==8)
            
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
        self.table = self.table.unionByName(
            self.STAGING_TABLE.withColumnRenamed('YYYYMM_DocDate','YearMonth_DocDate').drop('EntryDate')
        )
        self.table.write.mode('overwrite').save(self.FACT_TABLE_PATH)

    def move_to_processed(self):
        notebookutils.fs.mkdirs(self.ProcessedDir)
        notebookutils.fs.mv(self.FILE_PATH_ON_LAKE, self.ProcessedDir, create_path=True)

    def runETL(self):
        self.load_to_staging()
        self.remove_previous_data()
        self.save_to_fact()
        self.move_to_processed()

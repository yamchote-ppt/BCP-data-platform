# Welcome to your new notebook
# Type here in the cell editor to add code!
import numpy as np
from pyspark.sql import Row
from pyspark.sql.functions import col, when, concat, lit, format_string, upper, substring, substring_index, expr, current_date, current_timestamp, length, to_timestamp,concat_ws, isnull, date_format, asc, trim, trunc, date_sub, year,coalesce, row_number, sum, lpad, max, regexp_replace, floor
from pyspark.sql.types import IntegerType, DecimalType, StringType, LongType, TimestampType, StructType, StructField, DoubleType, ShortType
import pandas as pd
import re
import os
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from datetime import datetime
import env.utils as utils
from pyspark.sql.window import Window
import shutil
from decimal import Decimal
import notebookutils

from typing import Callable, List, Dict, Any
# from tabulate import tabulate

spark = SparkSession.builder.config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

factFile = spark.sql("SELECT * FROM SilverLH.factfile LIMIT 0")
current_date = [int(x) for x in (datetime.now() + timedelta(hours=7)).strftime('%Y%m%d %H%M%S').split(' ')]

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
                               .select(col('StationKey'),col('CustomerCode').cast(IntegerType()),col('POSCode'),regexp_replace(col('PosName'), ' ', '').alias('POSName')).drop_duplicates()
        self.log = {'stagingTable':stagingTable}

    def dataConversion(self, ColumnsCast: Dict[str, Any]):
        self.stagingTable = self.stagingTable.withColumns(
            {column: col(column).cast(ColumnsCast[column]()) for column in ColumnsCast}
        )

    def derivedField(self,ColumnsExpr: Dict[str, Any]):
        self.stagingTable = self.stagingTable.withColumns(
            {column: ColumnsExpr[column] for column in ColumnsExpr}
        )

    def step1(self):
        print('\t\t\t\tstep1 process...')
        if self.CATEGORY.upper() == 'FIRSTPRO' and self.SUBCATEGORY.upper() == 'EOD_TANKS':
            self.dataConversion({'GradeNumber': StringType})
            self.derivedField({'ReStationName': regexp_replace('StationName', ' ', '')})
        elif self.SUBCATEGORY.upper() == 'EOD_METERS':
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
        elif self.SUBCATEGORY.upper() == 'PMT':
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
        elif self.SUBCATEGORY.upper() == 'LUB':
            self.derivedField({
                'ReStationName': regexp_replace('StationName', ' ', ''),
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00'))
            })
        elif self.SUBCATEGORY.upper() == 'AR_TRANSACTIONS':
            self.dataConversion({'GradeNumber': StringType})
            self.derivedField({
                'TransTimeKey': concat(regexp_replace('TransTime', ':', ''), lit('00')),
                'CastLubeCode': when(col('LubeCode') == "", "0").otherwise(col('LubeCode')),
                'ReStationName': regexp_replace('StationName', ' ', ''),
                'ReGradeTitle': when((col('GradeTitle') == "") & (col('Volume') == 0) & (col('Amount') == 0), "LUBE").otherwise(col('GradeTitle'))
            })
        elif self.SUBCATEGORY.upper() == 'DSC':
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
        elif self.CATEGORY.upper() == 'FLOWCO' and self.SUBCATEGORY.upper() == 'EOD_TANKS':
            self.dataConversion({'GradeNumber': StringType})
        print('\t\t\t\tstep1 processed completed')

    def lookUpStation_FIRSTPRO(self, incomingDF, POSName_key, POSCode_key, CustomerCode_key):
        print('\t\t\t\t\tStarting Lookup Station...')
        incomingDF = incomingDF.withColumns({'key1':col(POSName_key),'key2':col(POSCode_key),'key3':col(CustomerCode_key)})
        dimstationjoin = self.dimStation.withColumnsRenamed({'POSName':'key1','POSCode':'key2','CustomerCode':'key3'})
        lookupCondition = ['key1','key2','key3']

        matchStation = incomingDF.join(dimstationjoin, on= lookupCondition, how='inner').drop('key1','key2','key3')
        UnMatchStation  = incomingDF.join(dimstationjoin, on=lookupCondition, how='left_anti').withColumn('Error',lit("Mismatch StationKey")).drop('key1','key2','key3').withColumn('StationKey',lit(None).cast(IntegerType()))
        assert incomingDF.count() == matchStation.count() + UnMatchStation.count(), f'Error in lookup station: incomingDF.count() != matchStation.count() + UnMatchStation.count();\nincomingDF.count() = {incomingDF.count()}\nmatchStation.count() = {matchStation.count()}\nUnMatchStation.count() = {UnMatchStation.count()}'
        print('\t\t\t\t\tLookup Station completed')
        return matchStation, UnMatchStation
    
    def lookUpStation_FLOWCO_TANKS(self, incomingDF, POSCode_key, CustomerCode_key):
        print('\t\t\t\t\tStarting Lookup Station...')
        tmpDimstation = self.dimStation.select('StationKey','CustomerCode','POSCode')
        incomingDF = incomingDF.withColumns({'key1':col('POSCode'),'key2':col('CustomerCode')})
        dimstationjoin = tmpDimstation.withColumnsRenamed({'POSCode':'key1','CustomerCode':'key2'})
        lookupCondition = ['key1','key2']

        matchStation = incomingDF.join(dimstationjoin, on= lookupCondition, how='inner').drop('key1','key2')
        UnMatchStation  = incomingDF.join(dimstationjoin, on=lookupCondition, how='left_anti').withColumn('Error',lit("Mismatch StationKey")).drop('key1','key2').withColumn('StationKey',lit(None).cast(IntegerType()))
        assert incomingDF.count() == matchStation.count() + UnMatchStation.count(), f'Error in lookup station: incomingDF.count() != matchStation.count() + UnMatchStation.count();\nincomingDF.count() = {incomingDF.count()}\nmatchStation.count() = {matchStation.count()}\nUnMatchStation.count() = {UnMatchStation.count()}'
        print('\t\t\t\t\tLookup Station completed')
        return matchStation, UnMatchStation
    
    def lookUpProduct_FIRSTPRO(self, incomingDF, SourceKey_key, SourceTitle_key, StationKey_key,mappingProduct, mappingProductNoTitle):
        print('\t\t\t\t\tStarting Lookup Product...')
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
        print('\t\t\t\t\tLookup Product completed')
        return matchTable, UnMatchProductNoTitle
    
    def lookUpProduct_FLOWCO_TANKS(self, incomingDF, mappingProduct):
        print('\t\t\t\t\tStarting Lookup Product...')

        incomingDF = incomingDF.withColumns({'key1':col('GradeNumber')})
        mappingProductjoin = mappingProduct.withColumnsRenamed({'SourceKey':'key1'}).drop_duplicates()
        lookupCondition = ['key1']
        matchTable = incomingDF.join(mappingProductjoin, on=lookupCondition, how='inner').drop('key1')
        UnMatchProduct  = incomingDF.join(mappingProductjoin, on=lookupCondition, how='left_anti').withColumn('Error',lit("Mismatch ProductKey")).drop('key1')
        # assert incomingDF.count() == matchTable.count() + UnMatchProduct.count(), f'Error in lookup mappingProduct'

        UnMatchProduct = UnMatchProduct.filter((col('GradeNumber')==2)&(col('OpenVolume')==0)&(col('CloseVolume')==0)&(col('DeliveryVolume')==0)) 
        return matchTable, UnMatchProduct
    
    def lookUp(self):
        print('\t\t\t\tStarting Lookup process...')
        if self.CATEGORY.upper() == 'FIRSTPRO' and self.SUBCATEGORY.upper() in ['REFUND', 'POINTS']:
            matchFinal, UnMatchFinal = self.lookUpStation_FIRSTPRO(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')

        elif self.SUBCATEGORY.upper() == 'PMT':
            matchFinal, UnMatchFinal = self.lookUpStation_FIRSTPRO(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')
            if self.CATEGORY.upper() == 'FIRSTPRO':
                matchFinal = matchFinal.withColumn('PAY_Time',col('PayTimeKey')).drop('PayTimeKey').withColumn('END_Time',col('END_TimeKey')).drop('END_TimeKey')

        elif self.SUBCATEGORY.upper() in ['DSC', 'LUB']:
            matchFinal, UnMatchFinal = self.lookUpStation_FIRSTPRO(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')
        
        elif self.SUBCATEGORY.upper() == 'TRN':
            matchStation, UnMatchStation = self.lookUpStation_FIRSTPRO(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')

            if self.CATEGORY.upper() == 'FIRSTPRO':
                mappingProduct = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TRANSACTION')).drop_duplicates()
                mappingProductNoTitle = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('StationKey'),col('MappingKey')).filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull())).drop_duplicates()
            elif self.CATEGORY.upper() == 'FLOWCO':
                mappingProduct = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TRANSACTION')).drop_duplicates()
                mappingProductNoTitle = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('StationKey'),col('MappingKey')).filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull())).drop_duplicates()

            matchFinal, UnMatchProduct = self.lookUpProduct_FIRSTPRO(matchStation, SourceKey_key='Grade', SourceTitle_key='ReGradeTitle', StationKey_key='StationKey', mappingProduct=mappingProduct, mappingProductNoTitle=mappingProductNoTitle)
            UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct)

            if self.CATEGORY.upper() == 'FIRSTPRO':
                match1 = matchFinal.filter(col('Volume')>=0).orderBy('FileKey','TransNumber','CloseDateKey').drop('Volume','Amount')
                match2 = matchFinal.groupBy("CloseDateKey", "TransNumber", "FileKey").agg(
                    sum("Volume").alias("VOLUME"),
                    sum("Amount").alias("AMOUNT")  # Assuming 'Amount' is another column in your DataFrame
                ).orderBy('FileKey','TransNumber','CloseDateKey')
                matchFinal = match1.join(match2, on=['CloseDateKey', 'TransNumber', 'FileKey'], how='inner').withColumn('VOLUME',col('VOLUME').cast(DecimalType(15, 3)))

            matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'ProductKey'}).withColumn('Vat',lit(None))

        elif self.SUBCATEGORY.upper() == 'EOD_METERS':
            matchStation, UnMatchStation = self.lookUpStation_FIRSTPRO(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')

            if self.CATEGORY.upper() == 'FIRSTPRO':
                mappingProduct = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'METERS')).drop_duplicates()
                mappingProductNoTitle = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('StationKey'),col('MappingKey')).filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'METERS') & (col('sourcetitle').isNull())).drop_duplicates()

            elif self.CATEGORY.upper() == 'FLOWCO':
                mappingProduct = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'METERS')).drop_duplicates()
                mappingProductNoTitle = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('StationKey'),col('MappingKey')).filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'METERS') & (col('StationKey').isNotNull())).drop_duplicates()

            matchFinal, UnMatchProduct = self.lookUpProduct_FIRSTPRO(matchStation, SourceKey_key='GradeNumber', SourceTitle_key='GradeTitle', StationKey_key='StationKey', mappingProduct=mappingProduct, mappingProductNoTitle=mappingProductNoTitle)
            UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct) #.withColumn('GradeNumber',col('GradeNumber').cast(ShortType()))
            matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'ProductKey'})
        
        elif self.SUBCATEGORY.upper() == 'AR_TRANSACTIONS':
            matchStation, UnMatchStation = self.lookUpStation_FIRSTPRO(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')
            if self.CATEGORY.upper() == 'FIRSTPRO':
                mappingProduct = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'AR')).drop_duplicates()
                mappingProductNoTitle = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('StationKey'),col('MappingKey')).filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull())).drop_duplicates()
            elif self.CATEGORY.upper() == 'FLOWCO':
                mappingProduct = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'AR')).drop_duplicates()
                mappingProductNoTitle = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('StationKey'),col('MappingKey')).filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TRANSACTION') & (col('StationKey').isNotNull())).drop_duplicates()
            
            matchFinal, UnMatchProduct = self.lookUpProduct_FIRSTPRO(matchStation, SourceKey_key='GradeNumber', SourceTitle_key='ReGradeTitle', StationKey_key='StationKey', mappingProduct=mappingProduct, mappingProductNoTitle=mappingProductNoTitle)
            UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct)
            matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'GradeKey','LBAmount':'LubeAmount','AttendantNumber':'AttendeeNumber'})
        

            matchFinal, UnMatchProduct = self.lookUpProduct_FIRSTPRO(matchStation, SourceKey_key='GradeNumber', SourceTitle_key='ReGradeTitle', StationKey_key='StationKey', mappingProduct=mappingProduct, mappingProductNoTitle=mappingProductNoTitle)
            UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct)
            matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'GradeKey','LBAmount':'LubeAmount','AttendantNumber':'AttendeeNumber'})

        elif self.SUBCATEGORY.upper() == 'EOD_TANKS':
            if self.CATEGORY.upper() == 'FIRSTPRO':
                matchStation, UnMatchStation = self.lookUpStation_FIRSTPRO(self.stagingTable, POSName_key='ReStationName', POSCode_key='POSCode', CustomerCode_key='CustomerCode')

                mappingProduct = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('SourceTitle'),col('MappingKey')).filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TANKS')).drop_duplicates()
                mappingProductNoTitle = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('StationKey'),col('MappingKey')).filter((col('sourcename') == 'FIRSTPRO') & (col('sourcefile') == 'TANKS') & (col('StationKey').isNotNull())).drop_duplicates()
                matchFinal, UnMatchProduct = self.lookUpProduct_FIRSTPRO(matchStation, SourceKey_key='GradeNumber', SourceTitle_key='GradeTitle', StationKey_key='StationKey', mappingProduct=mappingProduct, mappingProductNoTitle=mappingProductNoTitle)
                UnMatchProduct = UnMatchProduct.filter((col('GradeNumber') == 2) & (col('OpenVolume') == 0) & (col('CloseVolume') == 0) & (col('DeliveryVolume') == 0))

                UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct).drop('ReStationName')
                matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'ProductKey'})

            elif self.CATEGORY.upper() == 'FLOWCO':
                matchStation, UnMatchStation = self.lookUpStation_FLOWCO_TANKS(self.stagingTable, POSCode_key='POSCode', CustomerCode_key='CustomerCode')

                mappingProduct = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').select(col('SourceKey'),col('MappingKey')).filter((col('sourcename') == 'FLOWCO') & (col('sourcefile') == 'TRANSACTION')).drop_duplicates()
                matchFinal, UnMatchProduct = self.lookUpProduct_FLOWCO_TANKS(matchStation, mappingProduct=mappingProduct)
                self.log['UnMatchStation'] = UnMatchStation
                self.log['UnMatchProduct'] = UnMatchProduct

                UnMatchFinal = UnMatchStation.unionByName(UnMatchProduct)
                matchFinal = matchFinal.withColumnsRenamed({'MappingKey':'ProductKey'})
        
        else:
            print(f'Waited to implement lookUp for {self.CATEGORY} and {self.SUBCATEGORY}')

        UnMatchFinal = UnMatchFinal.select(spark.read.load(self.path_to_mismatch).limit(0).columns)
        self.log['UnMatchFinal'] = UnMatchFinal
        UnMatchFinal = utils.copySchemaByName(UnMatchFinal, self.mismatchTable)
        UnMatchFinal.write.mode('append').save(self.path_to_mismatch)
        print('\t\t\t\t\tUnMatchFinal.count() = ',UnMatchFinal.count(), 'is added to mismatchTable')
        
        self.log['matchFinal'] = matchFinal
        matchFinal = matchFinal.select(spark.read.load(self.path_to_fact_table).limit(0).columns)
        matchFinal = utils.copySchemaByName(matchFinal, self.factTable)
        matchFinal.write.mode('append').save(self.path_to_fact_table)
        print('\t\t\t\t\tmatchFinal.count() = ',matchFinal.count(), 'is added to factTable')

        print('\t\t\t\tLookup process completed')

    def run(self):
        print('\t\t\tStarting Load to Fact... by loadtofactObj.run()')
        '''
        run ther following steps:
        step1: data conversion and derived field
        step2: lookup
        '''
        try:
            self.step1()
            self.lookUp()

        except Exception as e:
            print(f"Error in run load_to_fact: {e}")

class FactFileHandler:

    def __init__(self, WS_ID: int, LH_ID: int):
        self.WS_ID = WS_ID
        self.LH_ID = LH_ID
        self.path_to_factfile = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.LH_ID}/Tables/factfile'
        self.factFile = spark.read.load(self.path_to_factfile)
        self.new_file_key = self.factFile.agg(max('FileKey').alias('maxFileKey')).collect()[0].maxFileKey + 1
        self.new_record = {'FileKey':self.new_file_key, 'FileName':None, 'CategoryName':None, 'SubCategoryName':None, 'LoadStatus':None}

    def __str__(self):
        return str(self.new_record)

    def reload(self):
        self.factFile = spark.read.load(self.path_to_factfile)
    
    def saveTable(self):
        self.factFile.write.mode('overwrite').partitionBy('LoadStatus').save(self.path_to_factfile)
    
    def addNewRecord(self):
        new_row = Row(
            FileKey=self.new_record['FileKey'],
            FileName=self.new_record['FileName'],
            CategoryName=self.new_record['CategoryName'],
            SubCategoryName=self.new_record['SubCategoryName'],
            LoadDateKey=current_date[0],
            LoadTimeKey=current_date[1],
            LoadStatus=self.new_record['LoadStatus']
            )
        new_row_df = spark.createDataFrame([new_row], schema = factFile.schema)
        new_row_df.write.mode('append').partitionBy('LoadStatus').save(self.path_to_factfile)
        self.reload()
    
    def editRecord_byFileKey(self,FileKey: int,keyChange: Dict[str,Any] ,LoadStatusList: List[int] = None) -> None:
        condition = (self.factFile.FileKey == FileKey)

        if LoadStatusList:
            condition = condition & (factFile.LoadStatus.isin(LoadStatusList))

        oldRowToUpdate = self.factFile.filter(col('FileKey')==FileKey)
        numCheck = oldRowToUpdate.count()
        assert numCheck != 0, f'No records of FileKey = {FileKey}'
        assert numCheck == 1, f'There are {numCheck} rows for FileKey = {FileKey}; It\'s weird'

        self.factFile = self.factFile.withColumns({column: when(col('FileKey')==FileKey, keyChange[column]).otherwise(col(column)) for column in keyChange})
        self.saveTable()
        self.reload()

    def getLastId_from_lake(self):
        maxFileKey = spark.sql("SELECT MAX(FileKey) as maxFileKey FROM SilverLH.factfile").collect()[0].maxFileKey
        LastId = maxFileKey + 1
        return LastId

    def set_new_record(self,FileName,CategoryName,SubCategoryName,LoadStatus):
        self.new_record['FileName'] = FileName
        self.new_record['CategoryName'] = CategoryName
        self.new_record['SubCategoryName'] = SubCategoryName
        self.new_record['LoadStatus'] = LoadStatus
    
class ETLModule_POS:
    dev = None
    def __init__(self, config):
        # assert , keys in config, 'Missing keys in config'
        self.config = config
        self.WS_ID = config["WS_ID"]
        self.BronzeLH_ID = config["BronzeLH_ID"]
        self.SilverLH_ID = config["SilverLH_ID"]
        self.CATEGORY = config["CATEGORY"]
        self.SUBCATEGORY = config["SUBCATEGORY"]
        self.FILE_NAME = config["FILE_NAME"]
        self.FILE_PATH_ON_LAKE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Ingest/POS/{self.CATEGORY}/{self.SUBCATEGORY}/{self.FILE_NAME}'
        self.BadFilesDir =  f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/BadFiles/POS/{self.CATEGORY}/{self.SUBCATEGORY}/'
        self.ProcessedDir = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Processed/POS/{self.CATEGORY}/{self.SUBCATEGORY}/'
        self.STAGING_TABLE_NAME = 'stagingPos'   + {'TRN':'Trn','PMT':'Payment','LUB':'Lube','AR_TRANSACTIONS':'AR','DSC':'Discount','EOD_METERS':'Meter','POINTS':'Points','REFUND':'Refund','EOD_TANKS':'Tank'}.get(self.SUBCATEGORY,TypeError('NOT CORRECT SUBCATEGORY'))
        self.FACT_TABLE_NAME    = 'factpos'      + {'TRN':'fuelsales','PMT':'payment','LUB':'lube','AR_TRANSACTIONS':'ar','DSC':'discount','EOD_METERS':'meter','POINTS':'points','REFUND':'refund','EOD_TANKS':'tank'}.get(self.SUBCATEGORY,TypeError('NOT CORRECT SUBCATEGORY'))
        self.MIS_TABLE_NAME     =  'mismatchpos' + {'TRN':'trn',  'PMT':'payment','LUB':'lube','AR_TRANSACTIONS':'ar','DSC':'discount','EOD_METERS':'meter','POINTS':'points','REFUND':'refund','EOD_TANKS':'tank'}.get(self.SUBCATEGORY,TypeError('NOT CORRECT SUBCATEGORY'))
        self.list_Processed_file = [x.name for x in notebookutils.fs.ls(f"abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Processed/POS/{self.CATEGORY}/{self.SUBCATEGORY}/")]
        self.path_to_fact_table = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/{self.FACT_TABLE_NAME}'
        self.path_to_mismatch   = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/{self.MIS_TABLE_NAME}'
        self.path_to_staging    = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/{self.STAGING_TABLE_NAME}'
        self.mismatchTable = spark.read.load(self.path_to_mismatch)
        self.stagingTable = spark.read.load(self.path_to_staging).limit(0)
        self.factTable = spark.read.load(self.path_to_fact_table)
        self.FactFileHandler = FactFileHandler(self.WS_ID,self.SilverLH_ID)
        self.dev = config.get('dev',True)
        self.log = {}

        if self.dev:
            print("===============================================================")
            print(f'Running {self.FILE_NAME} in dev mode')
            print("===============================================================")
        else:
            print("===============================================================")
            print(f'Running {self.FILE_NAME}')
            print("===============================================================")



    def truncate_staging(self):
        tablePath = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/{self.STAGING_TABLE_NAME}'
        empty_df = spark.createDataFrame([], schema=spark.read.format("delta").load(tablePath).schema)
        empty_df.write.format("delta").mode("overwrite").save(tablePath)
        self.stagingSchema = spark.read.load(tablePath).limit(0)
        return True
        
    def getLastId(self):
        self.LastId = self.FactFileHandler.new_file_key
        return True

    def readFilePos(self, names):
        return pd.read_csv(self.FILE_PATH_ON_LAKE,encoding='TIS-620',delimiter='|',header=None,names=names)

    def getstagingFromRaw(self):
        match self.SUBCATEGORY.upper():
            case 'TRN':
                columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","Dispenser","Hose","Grade","GradeTitle","UnitPrice","Volume","Amount","AttendeeNumber","ChiefNumber"]        
            case 'PMT':
                columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","PMCode","ReceiveAmount","ARNumber","LPNumber","DocCode","PAY_Date","PAY_Time","End_Date","End_Time"]
            case 'LUB':
                columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","LubeCode","LubeName","UnitPrice","Quantity","Amount"]
            case 'AR_TRANSACTIONS':
                columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","Dispenser","Hose","GradeNumber","GradeTitle","GradePrice","Volume","Amount","LubeCode","UnitPrice","Quantity","LBAmount","PMCode","ARNumber","ARName","LicensePlate","DocCode","ODOMeter","AttendantNumber","DONumber"]
            case 'DSC':
                columns = ["KeyId","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","DCCode","DCAmount"]
            case 'EOD_METERS':
                columns = ["KeyId","CustomerCode","POSCode","StationName","CloseDateKey","ShiftNumber","DispenserNumber","HoseNumber","TankNumber","GradeNumber","GradeTitle","GradePrice","StartVolume","EndVolume","StartAmount","EndAmount","TestVolume","UsName"]
            case 'POINTS':
                columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","PMCode","Terminal_ID","Merchant_Name","Batch_Number","Card_Trace_Number","Card_Number","Available_Balance","Collect_Point","Redeem_Point","Collect_TimeStemp"]
            case 'REFUND':
                columns = ["KeyID","CustomerCode","POSCode","StationName","CloseDateKey","TransNumber","TransDateKey","TransTime","ReceiptNumber","Volume","Amount","Vat"]
            case 'EOD_TANKS':
                columns = ["KeyID","CustomerCode","POSCode","StationName","StartDateKey","TankNumber","GradeNumber","GradeTitle","OpenVolume","CloseVolume","DeliveryVolume","EndDateKey"]
            case _:
                raise TypeError('NOT CORRECT SUBCATEGORY')

        print('\tFile is already being read ...')
        df = self.readFilePos(names = columns)
        print('\tFile is already read')
        df['FileKey'] = self.LastId
        # columnMap = {column.lower():column for column in stagingSchema.columns}

        if self.SUBCATEGORY == 'TRN':
            df['YearKey'] = None

        self.log['rawFile'] = df
        self.log['stagingTable'] = self.stagingTable

        if len(df) != 0:
            print('\t\tFile is not blank')
            self.stagingTable = utils.copySchemaByName(spark.createDataFrame(df[self.stagingSchema.columns]),self.stagingSchema)
        else:
            print('\t\tFile is blank')
            self.stagingTable = spark.createDataFrame([],schema=self.stagingSchema.schema)
        # self.stagingTable = spark.createDataFrame(df[self.stagingSchema.columns],schema=self.stagingSchema.schema)
        print('\tstagingTable is created')
        return True

    def addToFactFile(self, status_code):
        print(f"Updating load status to {status_code}...")
        self.FactFileHandler.set_new_record(FileName=self.FILE_NAME, CategoryName=self.CATEGORY, SubCategoryName=self.SUBCATEGORY, LoadStatus=status_code)
        self.FactFileHandler.addNewRecord()
        return True

    def move_to_badfile(self):
        print('move to badfiles')
        if not self.dev:
            if not notebookutils.fs.exists(self.BadFilesDir):
                notebookutils.fs.mkdirs(self.BadFilesDir)
            notebookutils.fs.mv(self.FILE_PATH_ON_LAKE,self.BadFilesDir)
        return True

    def load_staging(self):
        print("\tStarting Load Staging process...")

        if not self.truncate_staging():
            raise Exception("Failed to truncate staging.")

        if not self.getLastId():
            raise Exception("Failed to load factfile.")
        print(f"\tget the last id in Factfile complete: LastId = {self.LastId}")

        # raise NotImplementedError('Demo Error to Test for loadstatus to 2')

        if not self.getstagingFromRaw():
            raise Exception("Failed to load data to staging.")
        print(f"\tLoad to Staging complete: size of staging = {self.stagingTable.count()}")
        print('\tLoad Staging process completed successfully')

    def updateGradeAndHose(self):
        print('\t\tupdateGradeAndHose...: but no implementation (in SSIS, it is commented out)')
        pass

    def load_to_fact(self):
        print('\t\tload to fact...')
        # TODO []: add load to fact function
        self.loadtofactObj = LoadToFact(self.path_to_fact_table, self.path_to_mismatch, self.stagingTable, self.CATEGORY, self.SUBCATEGORY,self.WS_ID,self.SilverLH_ID, self.factTable, self.mismatchTable)
        print('\t\t\tStarting Load to Fact are going to run()')
        self.loadtofactObj.run()

        # raise NotImplementedError('Demo Error to Test for force stoping: loadstatus = 99')
    
       
        # raise NotImplementedError('Wait to implement "load_to_fact"')

    def remove_from_fact(self,rows_to_delete):
        '''
            delete from factPosAR where filekey in (
            select filekey
            from(
            select ROW_NUMBER() OVER (ORDER BY loaddatekey desc,loadtimekey desc) AS RowNr,filekey,LoadDateKey,LoadTimeKey
            from factFile
            where filename = '20180719#991211007EOD_METERS.CSV'
            )tmp where rownr > 1);
        '''
        factTable_remaining = self.factTable.join(rows_to_delete.select("FileKey"), on="filekey", how="left_anti")
        factTable_remaining.write.mode("overwrite").save(self.path_to_fact_table)
        self.factTable = spark.read.load(self.path_to_fact_table)

    def remove_from_mismatch(self,rows_to_delete):
        '''
            delete from mismatchposar where filekey in (
            select filekey
            from(
            select ROW_NUMBER() OVER (ORDER BY loaddatekey desc,loadtimekey desc) AS RowNr,filekey,LoadDateKey,LoadTimeKey
            from factFile
            where filename = '20180719#991211007EOD_METERS.CSV'
            )tmp where rownr > 1); 
        '''
        mismatchTable_remaining = self.mismatchTable.join(rows_to_delete.select("FileKey"), on="filekey", how="left_anti")
        mismatchTable_remaining.write.mode("overwrite").save(self.path_to_mismatch)
        self.mismatchTable = spark.read.load(self.path_to_mismatch)

    def updateFactFile_in_remove_previous(self,rows_to_delete):
        rows_to_update = rows_to_delete.filter(col('LoadStatus').isin([1,3,5])).select("FileKey").collect()
        rows_to_update = [r.FileKey for r in rows_to_update]
        '''
            update factfile
            set LoadStatus = 4
            where filekey in (
            select filekey
            from(
            select ROW_NUMBER() OVER (ORDER BY loaddatekey desc,loadtimekey desc) AS RowNr,filekey,LoadDateKey,LoadTimeKey
            from factFile
            where filename = '20180719#991211007EOD_METERS.CSV'
            )tmp where rownr > 1  and loadstatus in (1,3,5));
        '''
        self.FactFileHandler.factFile = self.FactFileHandler.factFile.withColumn('LoadStatus',when(col('FileKey').isin(rows_to_update), 4).otherwise(col('LoadStatus')))

    def remove_previous_data(self):
        window_spec = Window.partitionBy("FileKey").orderBy(col("LoadDateKey").desc(), col("LoadTimeKey").desc())
        factFile_with_rownr = self.FactFileHandler.factFile.filter(col("FileName") == self.FILE_NAME).withColumn("RowNr", row_number().over(window_spec)) # recheck again whether the latest fileKey is store to factFile
        rows_to_delete = factFile_with_rownr.filter((col("RowNr") > 1)).select("FileKey",'LoadStatus')

        # raise NotImplementedError('Demo Error to Test for remove_previous_data error: loadstatus = 3')
    
        self.remove_from_fact(rows_to_delete)
        self.remove_from_mismatch(rows_to_delete)
        self.updateFactFile_in_remove_previous(rows_to_delete)

    def move_to_processed(self):
        """
        Move the file to the processed directory.
        """
        print('\t\tmove file to processed ...')
        if self.FILE_NAME in self.list_Processed_file:
            print('\t\tfile already exists: meaning that this file name have ever been processed')
            try:
                self.remove_previous_data()
                if not self.dev:
                    print('\t\tactual move file to processed')
                    if not notebookutils.fs.exists(self.ProcessedDir):
                        notebookutils.fs.mkdirs(self.ProcessedDir)
                    notebookutils.fs.mv(self.FILE_PATH_ON_LAKE,self.ProcessedDir,create_path=True, overwrite=True)
                return True
            except:
                self.addToFactFile(3)
                return False
        else:
            print('\t\t\tThis File is the new one')
            if not self.dev:
                print('\t\tactual move file to processed')
                if not notebookutils.fs.exists(self.ProcessedDir):
                    notebookutils.fs.mkdirs(self.ProcessedDir)
                notebookutils.fs.mv(self.FILE_PATH_ON_LAKE,self.ProcessedDir,create_path=True, overwrite=False)
            return True

    def main_ETL(self):
        if self.SUBCATEGORY.upper() in ['TRN', 'AR', ]: # TODO: add package need `updateGradeAndHose`
            print("\tStarting UPDATE GRADE AND HOSE")
            self.updateGradeAndHose()
        print("\tStarting main ETL process...")
        self.load_to_fact()
        # at here, fact table should be processed completely and save to lake
        # no any returned object
        
        if not self.move_to_processed(): #if move_to_processed is error
            raise AssertionError('error in move_to_processed with load status = 3')

    def post_ETL(self):
        pass

    def run_ETL(self):
        print("Starting ETL process...")
        try:
            self.load_staging()
            try:
                self.main_ETL()
                print("ETL process completed successfully.")
                self.addToFactFile(1)
                print("========================================================================================")
            except Exception as e:
                print(f"\t\tmain ETL process failed: {e}")
                self.move_to_badfile()
                self.addToFactFile(99)
                print("========================================================================================")
            
        except Exception as e:
            print(f"Load Staging process failed: {e}")
            self.move_to_badfile()
            self.addToFactFile(2)
            print("========================================================================================")

class Mismatch_POS:
    dev = None
    def __init__(self, config):
        # assert , keys in config, 'Missing keys in config'
        self.config = config
        self.WS_ID = config["WS_ID"]
        self.BronzeLH_ID = config["BronzeLH_ID"]
        self.SilverLH_ID = config["SilverLH_ID"]
        self.CATEGORY = config["CATEGORY"]
        self.SUBCATEGORY = config["SUBCATEGORY"]
        self.FILE_NAME = config["FILE_NAME"]
        self.FILE_PATH_ON_LAKE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Ingest/POS/{self.CATEGORY}/{self.SUBCATEGORY}/{self.FILE_NAME}'
        self.BadFilesDir =  f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/BadFiles/POS/{self.CATEGORY}/{self.SUBCATEGORY}/'
        self.ProcessedDir = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Processed/POS/{self.CATEGORY}/{self.SUBCATEGORY}/'
        self.STAGING_TABLE_NAME = 'stagingPos'   + {'TRN':'Trn','PMT':'Payment','LUB':'Lube','AR_TRANSACTIONS':'AR','DSC':'Discount','EOD_METERS':'Meter','POINTS':'Points','REFUND':'Refund','EOD_TANKS':'Tank'}.get(self.SUBCATEGORY,TypeError('NOT CORRECT SUBCATEGORY'))
        self.FACT_TABLE_NAME    = 'factpos'      + {'TRN':'fuelsales','PMT':'payment','LUB':'lube','AR_TRANSACTIONS':'ar','DSC':'discount','EOD_METERS':'meter','POINTS':'points','REFUND':'refund','EOD_TANKS':'tank'}.get(self.SUBCATEGORY,TypeError('NOT CORRECT SUBCATEGORY'))
        self.MIS_TABLE_NAME     =  'mismatchpos' + {'TRN':'trn',  'PMT':'payment','LUB':'lube','AR_TRANSACTIONS':'ar','DSC':'discount','EOD_METERS':'meter','POINTS':'points','REFUND':'refund','EOD_TANKS':'tank'}.get(self.SUBCATEGORY,TypeError('NOT CORRECT SUBCATEGORY'))
        self.list_Processed_file = [x.name for x in notebookutils.fs.ls(f"abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Processed/POS/{self.CATEGORY}/{self.SUBCATEGORY}/")]
        self.path_to_fact_table = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/{self.FACT_TABLE_NAME}'
        self.path_to_mismatch   = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/{self.MIS_TABLE_NAME}'
        self.path_to_staging    = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/{self.STAGING_TABLE_NAME}'
        self.mismatchTable = spark.read.load(self.path_to_mismatch)
        self.stagingTable = spark.read.load(self.path_to_staging).limit(0)
        self.factTable = spark.read.load(self.path_to_fact_table)
        self.FactFileHandler = FactFileHandler(self.WS_ID,self.SilverLH_ID)
        self.dev = config.get('dev',True)
        self.log = {}
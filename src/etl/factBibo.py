# Welcome to your new notebook
# Type here in the cell editor to add code!
import numpy as np
from pyspark.sql import Row
from pyspark.sql.functions import col, when, concat, lit, format_string, upper, substring, expr, current_date, current_timestamp,to_timestamp,concat_ws, isnull, date_format, asc, trim, trunc, date_sub, year,coalesce, row_number, sum, lpad
from pyspark.sql.types import IntegerType, DecimalType, StringType, LongType, TimestampType, StructType, StructField, DoubleType, ShortType
import pandas as pd
import re
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from datetime import datetime
import env.utils as utils
from pyspark.sql.window import Window
import shutil
import notebookutils
from time import sleep
# from tabulate import tabulate


spark = SparkSession.builder.getOrCreate()

current_date = [int(x) for x in (datetime.now() + timedelta(hours=7)).strftime('%Y%m%d %H%M%S').split(' ')]

class Bibo:
    def __init__(self,WS_ID,BronzeLH_ID,SilverLH_ID):
        self.WS_ID = WS_ID
        self.BronzeLH_ID = BronzeLH_ID
        self.SilverLH_ID = SilverLH_ID
        self.factFile = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factfile')

    def factBiboFlow(self, stagingBiboDailySales,LastId=None): #LastId=None means load from mismatch
        if LastId:
            stagingBiboDailySales = stagingBiboDailySales.withColumns({
                'Int_Date':date_format(col('CalendarDay'),'yyyyMMdd').cast(IntegerType()),
                'FileKey':lit(LastId),
                'CustomerCode':col('SoldtoPartyKey').cast(StringType())
            })
        else:
            stagingBiboDailySales = stagingBiboDailySales.withColumns({
                'Int_Date':date_format(col('CalendarDay'),'yyyyMMdd').cast(IntegerType()),
                'CustomerCode':col('SoldtoPartyKey').cast(StringType())
            })
        
        dimstationForLookup = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/dimstation').select('CustomerCode','StationKey').dropDuplicates()
        matchedCustCode    = stagingBiboDailySales.join(dimstationForLookup, on='CustomerCode', how='inner')
        notMatchedCustCode = stagingBiboDailySales.join(dimstationForLookup, on='CustomerCode', how='left_anti').withColumn('Error',lit("Mismatch StationKey")) #Error Mismatched Station

        matched_oil = matchedCustCode.filter((lpad(col('DivisionKey'),2,"0").isin(["02","04"])))
        matched_nonoil = matchedCustCode.filter(~(lpad(col('DivisionKey'),2,"0").isin(["02","04"])))

        mappingProductForlookUp = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mappingproduct').filter((col('SourceName')=='BIBO')&(col('SourceFile')=='DailySales')).select('SourceKey','MappingKey').dropDuplicates()
        matchedProduct    = matched_oil.join(mappingProductForlookUp, on=matched_oil.MaterialKey==mappingProductForlookUp.SourceKey, how='inner') # แปลกเพราะมี record เพิ่ม จาก 64940 -> 65866
        notMatchedProduct = matched_oil.join(mappingProductForlookUp, on=matched_oil.MaterialKey==mappingProductForlookUp.SourceKey, how='left_anti').withColumn('Error',lit("Mismatch ProductKey")) #Error Mismatched ProductKey
        matched_nonoil = matched_nonoil.withColumn('MappingKey',lit(0).cast(IntegerType()))

        factBiboDailySalesCol  = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factbibodailysales').limit(0).columns
        MismatchBiboDailySalesCol = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mismatchbibodailysales').limit(0).columns
        
        factBiboDailySales    = matched_nonoil.unionByName(matchedProduct,allowMissingColumns=True).withColumnsRenamed({
            'Int_Date':'DateKey',
            'MappingKey':'ProductKey',
            "AccountGroupCustomerGroup1KeyNotCompounded": "AccountGroupCustomerGroup1Key",
            "ProvinceKeyNotCompounded": "ProvinceKey",
            "TransportationZoneKeyNotCompounded": "TransportationZoneKey"}).select(factBiboDailySalesCol)
            
        MismatchBiboDailySales = notMatchedCustCode.unionByName(notMatchedProduct,allowMissingColumns=True).select(MismatchBiboDailySalesCol)
        return factBiboDailySales, MismatchBiboDailySales
    
    def getLastId(self):
        maxFileKey = self.factFile.selectExpr('MAX(FileKey) as maxFileKey').collect()[0].maxFileKey #"SELECT MAX(FileKey) as maxFileKey FROM SilverLH.factfile"
        print(f'maxFileKey = {maxFileKey}')
        LastId = maxFileKey + 1
        return LastId

class factBibo(Bibo):

    def __init__(self,rawFileDirectory,WS_ID,BronzeLH_ID,SilverLH_ID):
        super().__init__(WS_ID,BronzeLH_ID,SilverLH_ID)
        self.rawFileDirectory = rawFileDirectory
        self.LastId = self.getLastId()
        
    def addFactFile(self,FileKey, FileName, CategoryName, SubCategoryName, LoadStatus):
        new_row = Row(FileKey=FileKey, FileName=FileName, CategoryName=CategoryName, SubCategoryName=SubCategoryName, LoadDateKey=current_date[0], LoadTimeKey=current_date[1], LoadStatus=LoadStatus)
        print(new_row)
        new_row = spark.createDataFrame([new_row], schema = self.factFile.schema)
        new_row.write.mode('append').partitionBy(['SubcategoryName','CategoryName','LoadStatus']).option('overwriteSchema','true')\
            .save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factfile')

    def updateFactFile(self,FileKey,keyChange):
        # factFile
        oldRowToUpdate = self.factFile.filter(col('FileKey')==FileKey)
        numCheck = oldRowToUpdate.count()
        assert numCheck != 0, f'No records of FileKey = {FileKey}'
        assert numCheck == 1, f'There are {numCheck} rows for FileKey = {FileKey}; It\'s weird'
        recordNotUpdate = self.factFile.filter(col('FileKey')!=FileKey)
        oldRowToUpdate = oldRowToUpdate.collect()[0]

        updateValue = oldRowToUpdate.asDict()
        updateValue['LoadDateKey'] = current_date[0]
        updateValue['LoadTimeKey'] = current_date[1]
        for columnKey in keyChange:
            updateValue[columnKey] = keyChange[columnKey]
        newRowToUpdate = Row(**updateValue)
        newRowToUpdateDF = spark.createDataFrame([newRowToUpdate], schema=self.factFile.schema)
        updatedFactFile = recordNotUpdate.unionByName(newRowToUpdateDF)
        updatedFactFile.write.mode('overwrite').partitionBy(['SubcategoryName','CategoryName','LoadStatus']).option('overwriteSchema','true')\
            .save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factfile')

    def transformSalesGroup(self,x):
        return "Metro" if ((x == "Bangkok") | (x == "Vicinity")) else x

    def getStagingBiboDailySalesFromRaw(self,FILE_PATH_ON_LAKE, LastId):
        stagingBiboDailySales_schema = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/stagingbibodailysales').limit(0)
        columnMap = {column.lower():column for column in stagingBiboDailySales_schema.columns}
        df = pd.read_excel(FILE_PATH_ON_LAKE,sheet_name='Report',engine='openpyxl',dtype={'Sold-to party - Key':np.int32})
        if len(df) == 0:
            return stagingBiboDailySales_schema
        else:
            df['Sales group (Sold to)'] = df['Sales group (Sold to)'].apply(self.transformSalesGroup)
            df.columns = [columnMap[re.sub(r'[^a-zA-Z0-9]','',column).lower()] for column in df.columns]
            df['FileKey'] = LastId
            df = spark.createDataFrame(df)
            stagingBiboDailySales = utils.copySchemaByName(df,stagingBiboDailySales_schema)
            stagingBiboDailySales = stagingBiboDailySales.withColumn('DivisionKey',lpad(col('DivisionKey'),2,'0')).withColumn('DistributionChannelKey',lpad(col('DistributionChannelKey'),2,'0'))
            stagingBiboDailySales.write.mode('overwrite').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/stagingbibodailysales')
            return True

    def updateDimstation(self,stagingBiboDailySales):
        SOURCE = stagingBiboDailySales.select('ShiptoPartyKey',
                                        'SoldtoParty',
                                        'CalendarDay',
                                        'SoldtoPartyKey', 
                                        'SalesPersonSoldToKey',
                                        'SalesPersonSoldTo',
                                        'SalesGroupSoldToKey',
                                        'SalesGroupSoldTo',
                                        'BusinessLineKey',
                                        'BusinessLine',
                                        'InvestmentTypeKey',
                                        'InvestmentType',
                                        'ProvinceKeyNotCompounded',
                                        'Province',
                                        'SalesOfficeSoldToKey',
                                        'SalesOfficeSoldTo',
                                        'SOR')

        window_spec = Window.partitionBy("SoldtoPartyKey").orderBy(col("CalendarDay").desc())
        SOURCE = SOURCE.withColumn(
                        "row_num",
                        row_number().over(window_spec)
                    )
        SOURCE = SOURCE.filter(col('row_num')==1)
        SOURCE = SOURCE.withColumn('SoldtoPartyKey',col('SoldtoPartyKey').cast(StringType()))
        old_columns = SOURCE.columns
        reverseMapper = {column+'_source':column for column in old_columns}
        columnMapper = {column:column+'_source' for column in old_columns}
        SOURCE = SOURCE.withColumnsRenamed(columnMapper)  
        
        SOURCE = SOURCE
        TARGET = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/dimstation')
        join_key = SOURCE.SoldtoPartyKey_source == TARGET.CustomerCode
        
        SAME = TARGET.join(SOURCE, on=join_key, how='left_anti').withColumn('status',lit('SAME'))
        UPDATE = TARGET.join(SOURCE, on=join_key, how='inner').withColumn('status',lit('UPDATE'))
        
        UPDATE = UPDATE.withColumns({'CustomerCode':col('SoldtoPartyKey_source'),
                                    'BiboSoldtoName':col('SoldtoParty_source'),
                                    'BiboSalesPersonKey':col('SalesPersonSoldToKey_source'),
                                    'BiboSalesPersonSoldTo':col('SalesPersonSoldTo_source'),
                                    'BiboSalesGroupSoldToKey':col('SalesGroupSoldToKey_source'),
                                    'BiboSalesGroupSoldTo':col('SalesGroupSoldTo_source'),
                                    'BiboBusinessLineKey':col('BusinessLineKey_source'),
                                    'BiboBusinessLine':col('BusinessLine_source'),
                                    'BiboInvestmentTypeKey':col('InvestmentTypeKey_source'),
                                    'BiboInvestmentType':col('InvestmentType_source'),
                                    'BiboProvinceKey':col('ProvinceKeyNotCompounded_source'),
                                    'BiboProvince':col('Province_source'),
                                    'BiboSalesOfficeSoldToKey':col('SalesOfficeSoldToKey_source'),
                                    'BiboSalesOfficeSoldTo':col('SalesOfficeSoldTo_source')}).drop(*[column for column in UPDATE.columns if '_source' in column])
        
        INSERT = SOURCE.join(TARGET, on =join_key, how='left_anti').withColumn('status',lit('INSERT')).select(
            lit('NA').alias('SOR'),
            col('SoldtoPartyKey_source').alias('CustomerCode'),
            lit('NA').alias('StationName'),
            lit('NA').alias('StatusCode'),
            lit('NA').alias('StationStyle'),
            lit('NA').alias('Dist'),
            lit('NA').alias('ManagerName'),
            lit('NA').alias('Location'),
            lit('NA').alias('AddressNo'),
            lit('NA').alias('AddressMoo'),
            lit('NA').alias('Soi'),
            lit('NA').alias('Street'),
            lit('NA').alias('SubDistrict'),
            lit('NA').alias('District'),
            lit('NA').alias('Province'),
            lit('NA').alias('ZipCode'),
            lit('NA').alias('HighwaySignType'),
            lit(99).alias('HighwaySignHeight'),
            lit('NA').alias('Latitude'),
            lit('NA').alias('Longitude'),
            lit(99).alias('HighwaySignLabelAmount'),
            lit('NA').alias('CorporationName'),
            lit('NA').alias('SaleId'),
            lit(None).alias('SelfServe'),
            lit('NA').alias('StationType'),
            lit('NA').alias('MRName'),
            lit('NA').alias('LandArea'),
            lit(99991231).alias('OperationStartDateKey'),
            lit(99991231).alias('OperationEndDateKey'),
            lit('NA').alias('LandWidth'),
            lit('NA').alias('OwnerName'),
            lit('NA').alias('StationManager'),
            lit('NA').alias('PhoneNo'),
            lit('NA').alias('FaxNo'),
            lit('NA').alias('MobileNo'),
            lit('NA').alias('Email'),
            lit('NA').alias('ProviderInternet'),
            lit('NA').alias('TRUECircuitId'),
            lit('NA').alias('InterlinkCircuitId'),
            lit('NA').alias('WDSCircuitId'),
            lit('NA').alias('AISCircuitId'),
            lit('NA').alias('POS'),
            lit(99991231).alias('AddRouting'),
            lit(0.0).cast(DecimalType(7,0)).alias('Rate'),
            lit('NA').alias('IP'),
            lit('NA').alias('Remark'),
            lit(99991231).alias('LoadDateKey'),
            lit(99991231).alias('LoadTimeKey'),
            lit('NA').alias('SSPic'),
            lit('NA').alias('SiteName'),
            lit('NA').alias('BGNDept'),
            lit('NA').alias('BGNDist'),
            lit('NA').alias('BGNMRName'),
            lit('NA').alias('StyleGroup'),
            lit('NA').alias('PosName'),
            lit('NA').alias('BGNMRView'),
            coalesce(col('SoldtoParty_source'), lit('NA')).alias('BiboSoldtoName'),
            coalesce(col('SalesPersonSoldToKey_source'), lit('NA')).alias('BiboSalesPersonKey'),
            coalesce(col('SalesPersonSoldTo_source'), lit('NA')).alias('BiboSalesPersonSoldTo'),
            coalesce(col('SalesGroupSoldToKey_source'), lit('NA')).alias('BiboSalesGroupSoldToKey'),
            coalesce(col('SalesGroupSoldTo_source'), lit('NA')).alias('BiboSalesGroupSoldTo'),
            coalesce(col('BusinessLineKey_source'), lit('NA')).alias('BiboBusinessLineKey'),
            coalesce(col('BusinessLine_source'), lit('NA')).alias('BiboBusinessLine'),
            coalesce(col('InvestmentTypeKey_source'), lit('NA')).alias('BiboInvestmentTypeKey'),
            coalesce(col('InvestmentType_source'), lit('NA')).alias('BiboInvestmentType'),
            coalesce(col('ProvinceKeyNotCompounded_source'), lit('NA')).alias('BiboProvinceKey'),
            coalesce(col('Province_source'), lit('NA')).alias('BiboProvince'),
            coalesce(col('SalesOfficeSoldToKey_source'), lit('NA')).alias('BiboSalesOfficeSoldToKey'),
            coalesce(col('SalesOfficeSoldTo_source'), lit('NA')).alias('BiboSalesOfficeSoldTo')
        ).withColumn('status',lit('INSERT'))
        INSERT = INSERT.drop(*[column for column in INSERT.columns if '_source' in column])
        
        newdimstation = SAME.unionByName(UPDATE,allowMissingColumns=True).unionByName(INSERT,allowMissingColumns=True)
        print('Status of Action for update dimStation:')
        newdimstation.groupBy('status').count().show()
        return newdimstation.drop('status')

    def updateDimBiboSomething(self,stagingbibodailysales, suffixName):
        sourceTableName = 'dimbibo' + suffixName
        column = suffixName
        columnKey = column + 'Key'
        assert (stagingbibodailysales.select(column).schema[column].dataType == StringType()) and (stagingbibodailysales.select(columnKey).schema[columnKey].dataType == StringType()), 'Data Type is not correct'

        SOURCE = stagingbibodailysales.select(columnKey,column).distinct()
        if suffixName.lower() not in ['investmenttype', 'regiongroup']:
            SOURCE = SOURCE.withColumn(columnKey, lpad(columnKey,2,'0')).select(columnKey,column)

        TARGET = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/{sourceTableName}')

        onKey = (TARGET[column] == SOURCE[column])&(TARGET[columnKey] == SOURCE[columnKey])

        INSERT = SOURCE.join(TARGET, on=onKey, how='left_anti')
        print(f'{INSERT.count()} new added rows to SilverLH.{sourceTableName}')
        spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/{sourceTableName.lower()}').dropna().drop_duplicates().write.mode('overwrite').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/{sourceTableName.lower()}')
        INSERT.write.mode('append').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/{sourceTableName.lower()}')

    def updateDimBiboProvince(self,stagingbibodailysales):
        assert (stagingbibodailysales.schema['Province'].dataType == StringType()) and (stagingbibodailysales.schema['ProvinceKeyNotCompounded'].dataType == StringType()), 'Data Type is not correct'

        SOURCE = stagingbibodailysales.select('ProvinceKeyNotCompounded','Province').distinct()
        TARGET = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/dimbiboprovince')

        onKey = (TARGET['Province'] == SOURCE['Province'])&(TARGET['ProvinceKey'] == SOURCE['ProvinceKeyNotCompounded'])

        INSERT = SOURCE.join(TARGET, on=onKey, how='left_anti')
        print(f'{INSERT.count()} new added rows to SilverLH.dimbiboprovince')
        spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/dimbiboprovince').dropna().drop_duplicates().write.mode('overwrite').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/dimbiboprovince')
        INSERT.withColumnRenamed('ProvinceKeyNotCompounded','ProvinceKey').write.mode('append').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/dimbiboprovince')

    def updateBibo(self,stagingbibodailysales):
        self.updateDimBiboSomething(stagingbibodailysales, 'distributionchannel')
        self.updateDimBiboSomething(stagingbibodailysales, 'division')
        self.updateDimBiboSomething(stagingbibodailysales, 'investmenttype')
        self.updateDimBiboProvince(stagingbibodailysales)
        self.updateDimBiboSomething(stagingbibodailysales, 'regiongroup')

    def updateMTD(self,factbibodailysales, mismatchbibodailysales, todayFileKey, maxretry = 10):
        datekeys = factbibodailysales.filter(col('FileKey')==todayFileKey).select('DateKey').distinct() # extract DateKey corresponding to today's FileKey (yyyyMM01 - yyyyMMdd)
        FileKeys = factbibodailysales.select('FileKey','DateKey').join(datekeys,on='DateKey',how='inner').select('FileKey').distinct() # expect  to get only 2 FileKeys (yesterday and today)
        FileKeys = [row.FileKey for row in FileKeys.select("FileKey").collect()]

        newfactFile = self.factFile.withColumn('LoadStatus',when(col('FileKey').isin(FileKeys) & (col('FileKey')!=todayFileKey),lit(6).cast(ShortType())).otherwise(col('LoadStatus')))

        retry = 0
        success = False
        while (not success) and (retry <= maxretry):
            try:
                newfactFile.write.mode('overwrite').partitionBy(['SubcategoryName','CategoryName','LoadStatus']).option('overwriteSchema','true')\
                    .save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factfile')

                self.factFile = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factfile')
                success = True
            except:
                retry += 1
                sleep(300)

        filekeys_to_delete = self.factFile.filter(\
            (col("CategoryName") == 'BIBO') &\
            (col("SubCategoryName") == 'DailySales') &\
            (col("LoadStatus") == 6)\
        ).select("FileKey")

        factbibodailysales_filtered = factbibodailysales.join(\
            filekeys_to_delete,\
            on="FileKey",\
            how="left_anti" \
        )

        mismatchbibodailysales_filtered = mismatchbibodailysales.join(\
            filekeys_to_delete,\
            on="FileKey",\
            how="left_anti" \
        )
        return factbibodailysales_filtered, mismatchbibodailysales_filtered

    def updateDSM(self,factbibodailysales, mismatchbibodailysales, todayFileKey):
        datekeys = factbibodailysales.filter(col('FileKey')==todayFileKey).select('DateKey').distinct() # extract DateKey corresponding to today's FileKey (yyyyMM01 - yyyyMMdd)
        FileKeys = factbibodailysales.select('FileKey','DateKey').join(datekeys,on='DateKey',how='inner').select('FileKey').distinct() # expect  to get only 2 FileKeys (yesterday and today)
        FileKeys = [row.FileKey for row in FileKeys.select("FileKey").collect()]

        newfactFile = self.factFile.withColumn('LoadStatus',when(col('FileKey').isin(FileKeys) & (col('FileKey')!=todayFileKey),lit(6).cast(ShortType())).otherwise(col('LoadStatus')))
        newfactFile.write.mode('overwrite').partitionBy(['SubcategoryName','CategoryName','LoadStatus']).option('overwriteSchema','true')\
            .save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factfile')

        self.factFile = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factfile')

        filekeys_to_delete = self.factFile.filter(\
            (col("CategoryName") == 'BIBO') &\
            (col("SubCategoryName") == 'DailySales') &\
            (col("LoadStatus") == 6)\
        ).select("FileKey")

        factbibodailysales_filtered = factbibodailysales.join(\
            filekeys_to_delete,\
            on="FileKey",\
            how="left_anti" \
        )

        mismatchbibodailysales_filtered = mismatchbibodailysales.join(\
            filekeys_to_delete,\
            on="FileKey",\
            how="left_anti" \
        )

        return factbibodailysales_filtered, mismatchbibodailysales_filtered

    def PostETL(self,):
        pass

    def runETL_bibo(self):
        fileList = [fileinfo.name for fileinfo in notebookutils.fs.ls(self.rawFileDirectory)]
        print(f'fileList = {fileList}')
        for fileName in fileList:
            print('===============================================================')
            print(fileName)
            print('===============================================================')
            
            FILE_PATH_ON_LAKE = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Ingest/BIBO/MKT1_026_DailySales/{fileName}'


            CATEGORY = 'BIBO'
            SUBCATEGORY = "DailySales"
            FILE_NAME = fileName

            BadFilesDir = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/BadFiles/BIBO/MKT1_026_DailySales/'

            ProcessedDir = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Files/Processed/BIBO/MKT1_026_DailySales/'

            LastId = self.getLastId()

            try:
                print(f'FILE_PATH_ON_LAKE = {FILE_PATH_ON_LAKE}')
                if self.getStagingBiboDailySalesFromRaw(FILE_PATH_ON_LAKE, LastId):
                    stagingBiboDailySales = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/stagingbibodailysales')
                else:
                    raise Exception('No data in the staging load')
                try:
                    print('\n=======>updateBibo')
                    self.updateBibo(stagingBiboDailySales)
                    print('\n=======>update dimstation')
                    newdimstation = self.updateDimstation(stagingBiboDailySales)
                    print('\n=======>run factBibo')
                    factBiboDailySalesNew, MismatchBiboDailySalesNew = self.factBiboFlow(stagingBiboDailySales,LastId)
                    print('\n=======>run factBibo success')
                    print('factBiboDailySalesNew',factBiboDailySalesNew.count())
                    print('MismatchBiboDailySalesNew',MismatchBiboDailySalesNew.count())

                    newdimstation.write.mode('overwrite').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/dimstation')
                    factBiboDailySalesNew.write.mode('append').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factbibodailysales')
                    MismatchBiboDailySalesNew.write.mode('append').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mismatchbibodailysales')
                    print('\n=======>add table success')

                    try:
                        if ('MTD' in FILE_NAME) or ('DSM' in FILE_NAME):
                            print('\n=======>removing yesterday records')
                            factBiboDailySales = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factbibodailysales')
                            MismatchBiboDailySales = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mismatchbibodailysales')
                            if ('MTD' in FILE_NAME):
                                print('\n=======>Go to updateMTD')
                                factBiboDailySales_after_deleted,MismatchBiboDailySales_after_deleted = self.updateMTD(factBiboDailySales,MismatchBiboDailySales, LastId)
                            if ('DSM' in FILE_NAME):
                                print('\n=======>Go to updateDSM')
                                factBiboDailySales_after_deleted,MismatchBiboDailySales_after_deleted = self.updateDSM(factBiboDailySales,MismatchBiboDailySales, LastId)
                            print('\n=======>remove success')
                            print('\n=======>update remove success')

                            factBiboDailySales_after_deleted.write.mode('overwrite').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factbibodailysales')
                            MismatchBiboDailySales_after_deleted.write.mode('overwrite').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mismatchbibodailysales')
                            print('\n=======>save remove success')
                        self.addFactFile(FileKey=LastId, FileName=FILE_NAME, CategoryName=CATEGORY, SubCategoryName=SUBCATEGORY, LoadStatus=1)
                    except Exception as e1:
                        self.addFactFile(FileKey=LastId, FileName=FILE_NAME, CategoryName=CATEGORY, SubCategoryName=SUBCATEGORY, LoadStatus=9)
                        print(f"An error occurred: {e1}")
                    notebookutils.fs.mv(FILE_PATH_ON_LAKE,ProcessedDir,create_path=True, overwrite=True)
                    self.PostETL()
                except Exception as e2:
                    notebookutils.fs.mv(FILE_PATH_ON_LAKE,BadFilesDir,create_path=True, overwrite=True)
                    self.addFactFile(FileKey=LastId, FileName=FILE_NAME, CategoryName=CATEGORY, SubCategoryName=SUBCATEGORY, LoadStatus=4)
                    print(f"An error occurred: {e2}")
            except Exception as e3:
                notebookutils.fs.mv(FILE_PATH_ON_LAKE,BadFilesDir,create_path=True, overwrite=True)
                self.addFactFile(FileKey=LastId, FileName=FILE_NAME, CategoryName=CATEGORY, SubCategoryName=SUBCATEGORY, LoadStatus=2)
                print(f"An error occurred: {e3}")


class MismatchBibo(Bibo):
    def __init__(self,WS_ID,BronzeLH_ID,SilverLH_ID):
        super().__init__(WS_ID,BronzeLH_ID,SilverLH_ID)

    def updLoadStatus1MisMatch(self,stagingBiboDailySales, MismatchBiboDailySales):
        staging_filekeys = stagingBiboDailySales.select("filekey").distinct()
        mismatch_filekeys = MismatchBiboDailySales.select("FileKey").distinct()

        valid_filekeys = staging_filekeys.join(mismatch_filekeys,\
                                        staging_filekeys.filekey == mismatch_filekeys.FileKey,\
                                        "left_anti").select(staging_filekeys.filekey)

        factfile_updated_df = self.factFile.alias("fact").join(valid_filekeys.alias("valid"), col("fact.filekey") == col("valid.filekey"), "left")\
            .withColumn("LoadStatus", when(col("valid.filekey").isNotNull(), 1).otherwise(col("fact.LoadStatus"))).select("fact.*",'LoadStatus')

        factfile_updated_df.write.mode("overwrite").partitionBy(['SubcategoryName','CategoryName','LoadStatus']).option('overwriteSchema','true')\
            .save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factfile')
        self.factFile = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factfile')

    def runMismatch(self):
        MismatchBiboDailySales_ori = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mismatchbibodailysales')
        stagingBiboDailySales = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/stagingbibodailysales').limit(0).unionByName(MismatchBiboDailySales_ori.drop('Error')) #สุดท้าย MismatchBiboDailySales โดน truncate ดังนั้นไม่ต้อง save table ก็ได้
        factBiboDailySales, MismatchBiboDailySales = self.factBiboFlow(stagingBiboDailySales)
        self.updLoadStatus1MisMatch(factBiboDailySales, MismatchBiboDailySales)
        factBiboDailySales.write.mode('append').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/factbibodailysales')
        MismatchBiboDailySales.write.mode('overwrite').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.SilverLH_ID}/Tables/mismatchbibodailysales')
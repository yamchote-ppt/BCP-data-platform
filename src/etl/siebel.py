import numpy as np
from pyspark.sql import Row
from pyspark.sql.functions import udf, col, when, concat, lit, format_string, upper, substring, substring_index, expr, current_date, current_timestamp,dense_rank, regexp_extract, length,input_file_name, to_timestamp,concat_ws, isnull, date_format, asc, trim, trunc, date_sub, year,coalesce, row_number, sum, lpad, max, regexp_replace, floor, instr, to_date, count
from pyspark.sql.types import IntegerType, DecimalType, StringType, LongType, TimestampType, StructType, StructField, DoubleType, ShortType
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
from pyspark.sql.window import Window

from typing import Callable, List, Dict, Any
# from tabulate import tabulate

spark = SparkSession.builder.config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

currentDate = current_date()
current_time = current_timestamp()
current_thai_time = expr("current_timestamp() + INTERVAL 7 HOURS")

class Siebel:

    def __init__(self,WS_ID,BronzeLH_name,SilverLH_name):
        self.WS_ID = WS_ID
        self.BronzeLH_name = BronzeLH_name
        self.SilverLH_name = SilverLH_name
        self.BronzeLH_ID = notebookutils.lakehouse.get(BronzeLH_name,WS_ID)['id']
        self.SilverLH_ID = notebookutils.lakehouse.get(SilverLH_name,WS_ID)['id']

    def DimSiebelMerchandise(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("DimSiebelMerchandise")
        print("=======================================================")

        spark.sql(
            """
            MERGE INTO SilverLH.dimsiebelmerchandise AS TARGET
            USING stagingsiebelmerchandise AS SOURCE 
            ON (trim(TARGET.rowid) = trim(SOURCE.ROW_ID)) 
            --When records are matched, update 
            --the records if there is any change
            WHEN MATCHED AND
            (TARGET.Loc <> SOURCE.LOC
            OR TARGET.OuTypeCd <> SOURCE.OU_TYPE_CD 
            OR COALESCE(TARGET.Mid,'NA') <> SOURCE.MID
            OR COALESCE(TARGET.CustomerCode,'NA') <> COALESCE(SOURCE.CUSTOMER_NUMBER,'NA')
            OR COALESCE(TARGET.MidNameEn,'NA') <> COALESCE(SOURCE.MID_NAME_EN,'NA')
            OR COALESCE(TARGET.MidNameTh, 'NA') <> COALESCE(SOURCE.MID_NAME_TH,'NA')
            OR COALESCE(TARGET.BusinessUnit,'NA') <> COALESCE(SOURCE.BUSINESS_UNIT,'NA')
            OR COALESCE(TARGET.MidStatus,'NA') <> COALESCE(SOURCE.MID_STATUS,'NA')
            OR TARGET.LastUpdDateKey <> date_format(SOURCE.LAST_UPD, 'yyyyMMdd')
            OR TARGET.LastUpdTimeKey <> date_format(SOURCE.LAST_UPD, 'HHmmss')
            OR COALESCE(TARGET.Merchant_Type,'NA') <> COALESCE(SOURCE.Merchant_Type,'NA')
            OR TARGET.MID_CreateDate <> date_format(SOURCE.MID_CreateDate,'yyyyMMdd')
            OR TARGET.MID_CreateTime <> date_format(SOURCE.MID_CreateDate, 'HHmmss')
            OR COALESCE(TARGET.House_No,'NA') <> COALESCE(SOURCE.House_No,'NA')
            OR COALESCE(TARGET.MOO,'NA') <> COALESCE(SOURCE.MOO,'NA')
            OR COALESCE(TARGET.Street,'NA') <> COALESCE(SOURCE.Street,'NA')
            OR COALESCE(TARGET.Tumbon,'NA') <> COALESCE(SOURCE.Tumbon,'NA')
            OR COALESCE(TARGET.Amphur,'NA') <> COALESCE(SOURCE.Amphur,'NA')
            OR COALESCE(TARGET.Country,'NA') <> COALESCE(SOURCE.Country,'NA')
            OR COALESCE(TARGET.Province,'NA') <> COALESCE(SOURCE.Province,'NA')
            OR COALESCE(TARGET.Zipcode,'NA') <> COALESCE(SOURCE.Zipcode,'NA')
            OR COALESCE(TARGET.Region,'NA') <> COALESCE(SOURCE.Region,'NA')
            OR COALESCE(TARGET.Attribute01,'NA') <> COALESCE(SOURCE.Attribute01,'NA')
            OR COALESCE(TARGET.Attribute02,'NA') <> COALESCE(SOURCE.Attribute02,'NA')
            OR COALESCE(TARGET.Attribute03,'NA') <> COALESCE(SOURCE.Attribute03,'NA')
            OR COALESCE(TARGET.Attribute04,'NA') <> COALESCE(SOURCE.Attribute04,'NA')
            OR COALESCE(TARGET.Attribute05,'NA') <> COALESCE(SOURCE.Attribute05,'NA')
            OR COALESCE(TARGET.Attribute06,'NA') <> COALESCE(SOURCE.Attribute06,'NA')
            OR COALESCE(TARGET.Attribute07,'NA') <> COALESCE(SOURCE.Attribute07,'NA')
            OR COALESCE(TARGET.Attribute08,'NA') <> COALESCE(SOURCE.Attribute08,'NA')
            OR COALESCE(TARGET.Attribute09,'NA') <> COALESCE(SOURCE.Attribute09,'NA')
            OR COALESCE(TARGET.Attribute10,'NA') <> COALESCE(SOURCE.Attribute10,'NA')
            OR COALESCE(TARGET.SOR_N,'NA') <> COALESCE(SOURCE.SOR_N,'NA')--1
            OR COALESCE(TARGET.ShopCode_Shipto,'NA') <> COALESCE(SOURCE.ShopCode_Shipto,'NA')--2
            OR COALESCE(TARGET.Information_1,'NA') <> COALESCE(SOURCE.Information_1,'NA')--3
            OR COALESCE(TARGET.Information_2,'NA') <> COALESCE(SOURCE.Information_2,'NA')--4
            OR COALESCE(TARGET.Information_3,'NA') <> COALESCE(SOURCE.Information_2,'NA')--5
            )
            THEN 
            UPDATE SET 
            TARGET.Loc = SOURCE.LOC
            ,TARGET.OuTypeCd = SOURCE.OU_TYPE_CD 
            ,TARGET.Mid = SOURCE.MID 
            ,TARGET.CustomerCode = SOURCE.CUSTOMER_NUMBER 
            ,TARGET.MidNameEn = SOURCE.MID_NAME_EN 
            ,TARGET.MidNameTh = SOURCE.MID_NAME_TH 
            ,TARGET.BusinessUnit = SOURCE.BUSINESS_UNIT 
            ,TARGET.MidStatus = SOURCE.MID_STATUS 
            ,TARGET.LastUpdDateKey = date_format(SOURCE.LAST_UPD, 'yyyyMMdd')
            ,TARGET.LastUpdTimeKey = date_format(SOURCE.LAST_UPD, 'HHmmss')
            ,TARGET.LoadDateKey = date_format(current_date(), 'yyyyMMdd')
            ,TARGET.LoadTimeKey = date_format(current_date(), 'HHmmss')
            ,TARGET.Merchant_Type = SOURCE.MERCHANT_TYPE
            ,TARGET.MID_CreateDate = date_format(SOURCE.MID_CreateDate,'yyyyMMdd')
            ,TARGET.MID_CreateTime = date_format(SOURCE.MID_CreateDate, 'HHmmss')
            ,TARGET.House_No = SOURCE.House_No
            ,TARGET.MOO = SOURCE.MOO
            ,TARGET.Street = SOURCE.Street
            ,TARGET.Tumbon = SOURCE.Tumbon
            ,TARGET.Amphur = SOURCE.Amphur
            ,TARGET.Country = SOURCE.Country
            ,TARGET.Province = SOURCE.Province
            ,TARGET.Zipcode = SOURCE.Zipcode
            ,TARGET.Region = SOURCE.Region
            ,TARGET.Attribute01 = SOURCE.Attribute01
            ,TARGET.Attribute02 = SOURCE.Attribute02
            ,TARGET.Attribute03 = SOURCE.Attribute03
            ,TARGET.Attribute04 = SOURCE.Attribute04
            ,TARGET.Attribute05 = SOURCE.Attribute05
            ,TARGET.Attribute06 = SOURCE.Attribute06
            ,TARGET.Attribute07 = SOURCE.Attribute07
            ,TARGET.Attribute08 = SOURCE.Attribute08
            ,TARGET.Attribute09 = SOURCE.Attribute09
            ,TARGET.Attribute10 = SOURCE.Attribute10
            ,TARGET.SOR_N = SOURCE.SOR_N --1
            ,TARGET.ShopCode_Shipto = SOURCE.ShopCode_Shipto --2
            ,TARGET.Information_1 = SOURCE.Information_1 --3
            ,TARGET.Information_2 = SOURCE.Information_2 --4
            ,TARGET.Information_3 = SOURCE.Information_3 --5
            --When no records are matched, insert
            --the incoming records from source
            --table to target table
            WHEN NOT MATCHED BY TARGET THEN 
            INSERT (RowId
            ,Loc
            ,OuTypeCd
            ,Mid
            ,CustomerCode
            ,MidNameEn
            ,MidNameTh
            ,BusinessUnit
            ,MidStatus
            ,LastUpdDateKey
            ,LastUpdTimeKey
            ,LoadDateKey
            ,LoadTimeKey
            ,Merchant_Type
            ,MID_CreateDate
            ,MID_CreateTime
            ,House_No
            ,MOO
            ,Street
            ,Tumbon
            ,Amphur
            ,Country
            ,Province
            ,Zipcode
            ,Region
            ,Attribute01
            ,Attribute02
            ,Attribute03
            ,Attribute04
            ,Attribute05
            ,Attribute06
            ,Attribute07
            ,Attribute08
            ,Attribute09
            ,Attribute10
            ,SOR_N
            ,ShopCode_Shipto
            ,Information_1
            ,Information_2
            ,Information_3)
            VALUES (
            SOURCE.ROW_ID
            ,SOURCE.LOC
            ,SOURCE.OU_TYPE_CD
            ,SOURCE.MID
            ,SOURCE.CUSTOMER_NUMBER
            ,SOURCE.MID_NAME_EN
            ,SOURCE.MID_NAME_TH
            ,SOURCE.BUSINESS_UNIT
            ,SOURCE.MID_STATUS
            ,date_format(LAST_UPD, 'yyyyMMdd')
            ,date_format(LAST_UPD, 'HHmmss')
            ,date_format(current_date(), 'yyyyMMdd')
            ,date_format(current_date(), 'HHmmss')
            ,SOURCE.Merchant_Type
            ,date_format(SOURCE.MID_CreateDate,'yyyyMMdd')
            ,date_format(SOURCE.MID_CreateDate, 'HHmmss')
            ,SOURCE.House_No
            ,SOURCE.MOO
            ,SOURCE.Street
            ,SOURCE.Tumbon
            ,SOURCE.Amphur
            ,SOURCE.Country
            ,SOURCE.Province
            ,SOURCE.Zipcode
            ,SOURCE.Region
            ,SOURCE.Attribute01
            ,SOURCE.Attribute02
            ,SOURCE.Attribute03
            ,SOURCE.Attribute04
            ,SOURCE.Attribute05
            ,SOURCE.Attribute06
            ,SOURCE.Attribute07
            ,SOURCE.Attribute08
            ,SOURCE.Attribute09
            ,SOURCE.Attribute10
            ,SOURCE.SOR_N --1
            ,SOURCE.ShopCode_Shipto --2
            ,SOURCE.Information_1 --3
            ,SOURCE.Information_2 --4
            ,SOURCE.Information_3 --5
            )
            """
        ).show()

        # update Primary key (MerchandiseKey) of new records
        dimsiebelmerchandise = spark.table('SilverLH.dimsiebelmerchandise')

        newRecord = dimsiebelmerchandise.filter(col('MerchandiseKey').isNull())
        oldRecord = dimsiebelmerchandise.filter(col('MerchandiseKey').isNotNull())
        maxOldKey = oldRecord.select('MerchandiseKey').agg(max("MerchandiseKey")).collect()[0][0]
        window_spec = Window.orderBy("RowId")
        newRecord = newRecord.withColumn("MerchandiseKey", row_number().over(window_spec) + maxOldKey)

        dimsiebelmerchandise = oldRecord.unionByName(newRecord)

        dimsiebelmerchandise.write.mode('overwrite').saveAsTable('SilverLH.dimsiebelmerchandise')

    def DimSiebelProduct(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("DimSiebelProduct")
        print("=======================================================")
        dimsiebelproduct = spark.sql("SELECT * FROM SilverLH.dimSiebelProduct")
        stagingSiebelProduct = spark.sql("SELECT * FROM BronzeLH_Siebel.stagingSiebelProduct")
        dimsiebelproduct = utils.fillNaAll(dimsiebelproduct)
        dimsiebelproduct = dimsiebelproduct.withColumn("rowid", trim(col("rowid")))

        renameCol = {
            'ROW_ID':'rowid',
            'CREATED_BY':'CreatedBy',
            'LAST_UPD_BY':'LastUpdBy',
            'NAME':'Name',
            'ALIAS_NAME':'AliasName',
            'ASSOC_LEVEL':'AssocLevel',
            'CATEGORY_CD':'CategoryCd',
            'PROD_CD':'ProdCd',
            'STATUS_CD':'StatusCd',
            'PART_NUM':'PartNum',
            'TYPE':'Type',
        }

        stagingSiebelProduct = stagingSiebelProduct.withColumnsRenamed(renameCol)\
                                                .withColumn('CreatedDateKey',date_format(col("CREATED"), 'yyyyMMdd'))\
                                                .withColumn('CREATED',date_format(col("CREATED"), 'HHmmss')).withColumnRenamed('CREATED','CreatedTimeKey')\
                                                .withColumn('LastUpdDateKey',date_format(col("LAST_UPD"), 'yyyyMMdd'))\
                                                .withColumn('LAST_UPD',date_format(col("LAST_UPD"), 'HHmmss')).withColumnRenamed('LAST_UPD','LastUpdTimeKey')\
                                                .withColumn('LoadDateKey',date_format(col("LoadDateTime"), 'yyyyMMdd'))\
                                                .withColumn('LoadDateTime',date_format(col("LoadDateTime"), 'HHmmss')).withColumnRenamed('LoadDateTime','LoadTimeKey')

        stagingSiebelProduct = stagingSiebelProduct.withColumn("rowid", trim(col("rowid"))).select(*[column for column in dimsiebelproduct.columns if column!= 'ProductKey'])
        stagingSiebelProduct = utils.copySchemaByName(stagingSiebelProduct,dimsiebelproduct)

        source = stagingSiebelProduct
        target = dimsiebelproduct
        utils.trackSizeTable(target,detail='dimsiebelproduct')
        utils.trackSizeTable(source,detail='stagingSiebelProduct')

        old_columns = source.columns
        mapper = {column + "_source":column for column in old_columns}
        new_columns = [column + "_source" for column in old_columns]      # columns of stagingbcpss with '_source'
        source = source.toDF(*new_columns)

        intersection = target.join(source,on = target.rowid == source.rowid_source, how='inner').select(*(['ProductKey']+new_columns))

        newcoming = source.join(target, on = target.rowid == source.rowid_source, how='left_anti').select(new_columns)
        newcoming = newcoming.withColumn('ProductKey', lit(None))
        newcoming = newcoming.select(*intersection.columns)

        newdimsiebelproduct = intersection.unionByName(newcoming)
        newdimsiebelproduct = newdimsiebelproduct.withColumnsRenamed(mapper)
        newdimsiebelproduct = utils.copySchemaByName(newdimsiebelproduct,dimsiebelproduct)

        # update Primary key (CardProfileKey) of new records
        originalTalble = newdimsiebelproduct
        pkey = 'ProductKey'
        order = 'RowId'

        newRecord = originalTalble.filter(col(pkey).isNull())
        oldRecord = originalTalble.filter(col(pkey).isNotNull())
        maxOldKey = oldRecord.select(pkey).agg(max(pkey)).collect()[0][0]
        window_spec = Window.orderBy(order)
        newRecord = newRecord.withColumn(pkey, row_number().over(window_spec) + maxOldKey)

        newdimsiebelproduct = oldRecord.unionByName(newRecord)

        # Save Data
        newdimsiebelproduct.write.mode('overwrite').saveAsTable('SilverLH.dimSiebelProduct')

    def StagingCardProfile2(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("StagingCardProfile2")
        print("=======================================================")

        stagingSiebelCardProfile2 = spark.sql("""
        SELECT a.*,
        case when b.cardnum is not null then 1 else 0 end as is_test
        FROM BronzeLH_Siebel.stagingSiebelCardProfile a
        left join SilverLH.factTestCard b on a.card_num = b.cardnum
        """)

        stagingSiebelCardProfile2 = stagingSiebelCardProfile2.withColumnRenamed('CreatedDateTime','CREATEDDATETIME').withColumn("CREATEDDATETIME", expr("CREATEDDATETIME + INTERVAL 7 HOURS"))\
                                                            .withColumnRenamed('CARD_USSUED_DATE','CARD_ISSUED_DATE')\
                                                            .withColumn('CARD_ISSUED_DATE', to_timestamp('CARD_ISSUED_DATE',"dd/MM/yyyy HH:mm:ss")).withColumn("CARD_ISSUED_DATE", expr("CARD_ISSUED_DATE + INTERVAL 7 HOURS"))\
                                                            .withColumn('CARD_EXPIRE_DATE', to_timestamp('CARD_EXPIRE_DATE',"dd/MM/yyyy HH:mm:ss")).withColumn("CARD_EXPIRE_DATE", expr("CARD_EXPIRE_DATE + INTERVAL 7 HOURS"))\
                                                            .withColumn('JoinDate', to_timestamp('JoinDate',"dd/MM/yyyy HH:mm:ss")).withColumn("JoinDate", expr("JoinDate + INTERVAL 7 HOURS"))\

        stagingSiebelCardProfile2 = stagingSiebelCardProfile2.withColumn("INT_LOAD_DATE", col("CREATEDDATETIME")).withColumn("INT_LOAD_DATE", date_format("INT_LOAD_DATE", "yyyyMMdd").cast(IntegerType()))\
                                                            .withColumn("INT_LOAD_TIME", col("CREATEDDATETIME")).withColumn("INT_LOAD_TIME", date_format("INT_LOAD_TIME", "HHmmss").cast(IntegerType()))\
                                                            .withColumn("INT_CARD_ISSUED_DATE", col("CARD_ISSUED_DATE")).withColumn("INT_CARD_ISSUED_DATE", date_format("INT_CARD_ISSUED_DATE", "yyyyMMdd").cast(IntegerType()))\
                                                            .withColumn("INT_CARD_ISSUED_TIME", col("CARD_ISSUED_DATE")).withColumn("INT_CARD_ISSUED_TIME", date_format("INT_CARD_ISSUED_TIME", "HHmmss").cast(IntegerType()))\
                                                            .withColumn("INT_CARD_EXPIRE_DATE", col("CARD_EXPIRE_DATE")).withColumn("INT_CARD_EXPIRE_DATE", date_format("INT_CARD_EXPIRE_DATE", "yyyyMMdd").cast(IntegerType()))\
                                                            .withColumn("INT_CARD_EXPIRE_TIME", col("CARD_EXPIRE_DATE")).withColumn("INT_CARD_EXPIRE_TIME", date_format("INT_CARD_EXPIRE_TIME", "HHmmss").cast(IntegerType()))\
                                                            .withColumn("INT_CURRENT_DATE", lit(current_time)).withColumn("INT_CURRENT_DATE", expr("INT_CURRENT_DATE + INTERVAL 7 HOURS")).withColumn("INT_CURRENT_DATE", date_format("INT_CURRENT_DATE", "yyyyMMdd").cast(IntegerType()))\
                                                            .withColumn("INT_CURRENT_TIME", lit(current_time)).withColumn("INT_CURRENT_TIME", expr("INT_CURRENT_TIME + INTERVAL 7 HOURS")).withColumn("INT_CURRENT_TIME", date_format("INT_CURRENT_TIME", "yyyyMMdd").cast(IntegerType()))\
                                                            .withColumn("INT_JOINDATE_DATE", col("JoinDate")).withColumn("INT_JOINDATE_DATE", date_format("INT_JOINDATE_DATE", "yyyyMMdd").cast(IntegerType()))\
                                                            .withColumn("INT_JOINDATE_TIME", col("JoinDate")).withColumn("INT_JOINDATE_TIME", date_format("INT_JOINDATE_TIME", "HHmmss").cast(IntegerType()))

        column_mapping = {
            "CARD_STATUS": "CardStatus",
            "CARD_TYPE": "CardType",
            "CARD_SUB_TYPE": "CardSubType",
            "INT_CARD_ISSUED_DATE": "CardIssuedDateKey",
            "INT_CARD_ISSUED_TIME": "CardIssuedTimeKey",
            "INT_CARD_EXPIRE_DATE": "CardExpireDateKey",
            "INT_CARD_EXPIRE_TIME": "CardExpireTimeKey",
            "CARD_NUM": "CardNum",
            "MOBILE_NUMBER": "MobileNumber",
            "FIRST_NAME_ENG": "FirstNameEng",
            "LAST_NAME_ENG": "LastNameEng",
            "BIRTH_DATE": "BirthDate",
            "FIRST_NAME_THA": "FirstNameTha",
            "LAST_NAME_THA": "LastNameTha",
            "IDENTIFICATION_NUM": "IdentificationNum",
            "CAR_MODEL": "CarModel",
            "PROVINCE": "Province",
            "COUNTRY": "Country",
            "DISTRICT": "District",
            "POSTAL_CODE": "PostalCode",
            "SUB_DISTRICT": "SubDistrict",
            "MOO_SOI": "MooSoi",
            "ROAD_STREET": "RoadStreet",
            "HOUSE_NO": "HouseNo",
            "CONTACT_STATUS": "ContactStatus",
            "APPROVAL_STATUS": "ApprovalStatus",
            "INT_LOAD_DATE": "LoadDateKey",
            "INT_LOAD_TIME": "LoadTimeKey",
            "is_test": "IsTest",
            "SAME_PR_FLG": "SamePriceSubmit",
            "MEM_CLASS": "MemberClass",
            "EMAIL_ADDR": "EmailAddr",
            "MEMBER_ID": "MemberID",
            "DONATION_FLAG": "DonationFlag",
            "DONATION_CODE": "DonationCode",
            "DONATION_NAME": "DonationName",
            "Chanel": "Chanel",
            "UpdateBy": "UpdateBy",
            "MOBILE_NUMBEROriginal": "MobileNumberOriginal",
            "EMAIL_ADDROriginal": "EmailAddrOriginal",
            "MEMBER_NUM": "MEMBER_NUM",
            "Data Conversion.JoinDate": "JoinDate",
            "INT_JOINDATE_DATE": "JoinDateKey",
            "INT_JOINDATE_TIME": "JoinTimeKey",
            "MemberGroup": "MemberGroup",
            "RegisChannel": "RegisChannel"
        }
        stagingSiebelCardProfile2 = stagingSiebelCardProfile2.withColumnsRenamed(column_mapping)
        stagingSiebelCardProfile2 = stagingSiebelCardProfile2.select(*column_mapping.values())
        stagingSiebelCardProfile2.write.mode('overwrite').saveAsTable('SilverLH.stagingSiebelCardProfile2')

    def DimSiebelCardProfile(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("DimSiebelCardProfile")
        print("=======================================================")
        print('\tLoad table')
        dimsiebelcardprofile  = spark.sql("SELECT * FROM SilverLH.dimsiebelcardprofile")
        stagingSiebelCardProfile2 = spark.sql("SELECT * FROM SilverLH.stagingSiebelCardProfile2")

        # dimsiebelcardprofile.count(),stagingSiebelCardProfile2.count()
        print('\tjoining SCD')
        source = stagingSiebelCardProfile2
        old_columns = source.columns
        reverseMapper = {column+'_source':column for column in old_columns}
        columnMapper = {column:column+'_source' for column in old_columns}
        source = source.withColumnsRenamed(columnMapper)  

        target = dimsiebelcardprofile
        join_key = (target.CardNum == source.CardNum_source) & (target.StartDateKey.isNotNull()) & (target.EndDateKey.isNull())

        left_anti = target.join(source, on = join_key, how='left_anti').withColumn('action',lit('NOT MATCHED'))

        inner = target.join(source, on = join_key, how='inner') # MATCHED: which has both column from target and from source (<name>_source)
        # split the same part and thew upsert part
        inner_same = inner.filter(col('CardStatus')==col('CardStatus_source')).select(target.columns).withColumn('action',lit('SAME')) #card status is not changed
        inner_upsert = inner.filter((col('CardStatus')!=col('CardStatus_source'))|(col('CardStatus').isNull())|(col('CardStatus_source').isNull())) #card status is changed

        # copy into 2 part: update (change only EndDate and EndTime; others remain the same) and insert (use value in source column)
        inner_update = inner_upsert.select([column for column in inner_upsert.columns if '_source' not in column])
        inner_insert = inner_upsert.select([column for column in inner_upsert.columns if '_source' in column])

        inner_update = inner_update.withColumn("EndDateKey", lit(current_time)).withColumn("EndDateKey", expr("EndDateKey + INTERVAL 7 HOURS")).withColumn("EndDateKey", date_format("EndDateKey", "yyyyMMdd").cast(IntegerType()))\
                                .withColumn("EndTimeKey", lit(current_time)).withColumn("EndTimeKey", expr("EndTimeKey + INTERVAL 7 HOURS")).withColumn("EndTimeKey", date_format("EndTimeKey", "HHmmss").cast(IntegerType()))\
                                .withColumn('action',lit('UPDATED'))

        inner_insert = inner_insert.withColumnsRenamed({x:x.replace('_source','') for x in inner_insert.columns})\
                                .withColumn('EndDateKey', lit(None).cast(IntegerType()))\
                                .withColumn('EndTimeKey', lit(None).cast(IntegerType()))\
                                .withColumn("StartDateKey", lit(current_time)).withColumn("StartDateKey", expr("StartDateKey + INTERVAL 7 HOURS")).withColumn("StartDateKey", date_format("StartDateKey", "yyyyMMdd").cast(IntegerType()))\
                                .withColumn("StartTimeKey", lit(current_time)).withColumn("StartTimeKey", expr("StartTimeKey + INTERVAL 7 HOURS")).withColumn("StartTimeKey", date_format("StartTimeKey", "HHmmss").cast(IntegerType()))\
                                .withColumn('action',lit('INSERT'))                           

        print('\tunion result from SCD')
        dimsiebelcardprofileSCD2 = left_anti.unionByName(inner_same,allowMissingColumns=True).unionByName(inner_update,allowMissingColumns=True).unionByName(inner_insert,allowMissingColumns=True)
        dimsiebelcardprofileSCD2.write.mode('overwrite').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/tmpdimsiebelcardprofileSCD2')
        print('\tnum rows of result from SCD')
        dimsiebelcardprofileSCD2 = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/tmpdimsiebelcardprofileSCD2')
        dimsiebelcardprofileSCD2.groupBy('action').count().show()
        # dimsiebelcardprofileSCD2 = dimsiebelcardprofileSCD2.drop('action')

        print('\tjoining SCD type 2')
        column_names = [
            "CardStatus",
            "CardType",
            "CardSubType",
            "CardNum",
            "MemberID",
            "MobileNumber",
            "FirstNameEng",
            "LastNameEng",
            "BirthDate",
            "FirstNameTha",
            "LastNameTha",
            "IdentificationNum",
            "CarModel",
            "Province",
            "Country",
            "District",
            "PostalCode",
            "SubDistrict",
            "MooSoi",
            "RoadStreet",
            "HouseNo",
            "ContactStatus",
            "ApprovalStatus",
            "IsTest",
            "LoadDateKey",
            "LoadTimeKey",
            "CardIssuedDateKey",
            "CardIssuedTimeKey",
            "CardExpireDateKey",
            "CardExpireTimeKey",
            "SamePriceSubmit",
            "MemberClass",
            "EmailAddr",
            "DonationFlag",
            "DonationCode",
            "DonationName",
            "Chanel",
            "UpdateBy",
            "MobileNumberOriginal",
            "EmailAddrOriginal",
            "MEMBER_NUM",
            "JoinDate",
            "JoinDateKey",
            "JoinTimeKey",
            "MemberGroup",
            "RegisChannel"
        ]
        source = stagingSiebelCardProfile2.select(column_names)
        source = source.withColumn("CardType", coalesce(col("CardType"), lit("NA")))\
                    .withColumn("CardSubType", coalesce(col("CardSubType"), lit("NA")))\
                    .withColumn("CardNum", coalesce(col("CardNum"), lit("NA")))\
                    .withColumn("MemberID", coalesce(col("MemberID"), lit("NA")))\
                    .withColumn("MobileNumber", coalesce(col("MobileNumber"), lit("NA")))\
                    .withColumn("FirstNameEng", coalesce(col("FirstNameEng"), lit("NA")))\
                    .withColumn("LastNameEng", coalesce(col("LastNameEng"), lit("NA")))\
                    .withColumn("BirthDate", coalesce(col("BirthDate"), lit("NA")))\
                    .withColumn("FirstNameTha", coalesce(col("FirstNameTha"), lit("NA")))\
                    .withColumn("LastNameTha", coalesce(col("LastNameTha"), lit("NA")))\
                    .withColumn("IdentificationNum", coalesce(col("IdentificationNum"), lit("NA")))\
                    .withColumn("CarModel", coalesce(col("CarModel"), lit("NA")))\
                    .withColumn("Province", coalesce(col("Province"), lit("NA")))\
                    .withColumn("Country", coalesce(col("Country"), lit("NA")))\
                    .withColumn("District", coalesce(col("District"), lit("NA")))\
                    .withColumn("PostalCode", coalesce(col("PostalCode"), lit("NA")))\
                    .withColumn("SubDistrict", coalesce(col("SubDistrict"), lit("NA")))\
                    .withColumn("MooSoi", coalesce(col("MooSoi"), lit("NA")))\
                    .withColumn("RoadStreet", coalesce(col("RoadStreet"), lit("NA")))\
                    .withColumn("HouseNo", coalesce(col("HouseNo"), lit("NA")))\
                    .withColumn("ContactStatus", coalesce(col("ContactStatus"), lit("NA")))\
                    .withColumn("ApprovalStatus", coalesce(col("ApprovalStatus"), lit("NA")))\
                    .withColumn("SamePriceSubmit", coalesce(col("SamePriceSubmit"), lit("NA")))\
                    .withColumn("MemberClass", coalesce(col("MemberClass"), lit("NA")))\
                    .withColumn("EmailAddr", coalesce(col("EmailAddr"), lit("NA")))\
                    .withColumn("DonationCode", coalesce(col("DonationCode"), lit("NA")))\
                    .withColumn("DonationName", coalesce(col("DonationName"), lit("NA")))\
                    .withColumn("Chanel", coalesce(col("Chanel"), lit("NA")))\
                    .withColumn("UpdateBy", coalesce(col("UpdateBy"), lit("NA")))\
                    .withColumn("MobileNumberOriginal", coalesce(col("MobileNumberOriginal"), lit("NA")))\
                    .withColumn("EmailAddrOriginal", coalesce(col("EmailAddrOriginal"), lit("NA")))\
                    .withColumn("MEMBER_NUM", coalesce(col("MEMBER_NUM"), lit("NA")))\
                    .withColumn("JoinDate", coalesce(col("JoinDate"), lit("NA")))\
                    .withColumn("MemberGroup", coalesce(col("MemberGroup"), lit("NA")))\
                    .withColumn("RegisChannel", coalesce(col("RegisChannel"), lit("NA")))

        old_columns = source.columns
        columnMapper = {column:column+'_source' for column in old_columns}
        source = source.withColumnsRenamed(columnMapper)

        target = dimsiebelcardprofileSCD2
        join_key = (
            (target.CardNum == source.CardNum_source) &
            (target.StartDateKey.isNotNull()) & # เปิดบัตรปกติ
            (target.EndDateKey.isNull()) &      # รายการล่าสุดของบัตรใบนั้น
            (
                (coalesce(target.MobileNumber, lit('N/A')) != coalesce(source.MobileNumber_source, lit('NA'))) |
                (coalesce(target.MobileNumberOriginal, lit('N/A')) != coalesce(source.MobileNumberOriginal_source, lit('NA'))) |
                (coalesce(target.FirstNameEng, lit('N/A')) != coalesce(source.FirstNameEng_source, lit('NA'))) |
                (coalesce(target.LastNameEng, lit('N/A')) != coalesce(source.LastNameEng_source, lit('NA'))) |
                (coalesce(target.BirthDate, lit('N/A')) != coalesce(source.BirthDate_source, lit('NA'))) |
                (coalesce(target.FirstNameTha, lit('N/A')) != coalesce(source.FirstNameTha_source, lit('NA'))) |
                (coalesce(target.LastNameTha, lit('N/A')) != coalesce(source.LastNameTha_source, lit('NA'))) |
                (coalesce(target.IdentificationNum, lit('N/A')) != coalesce(source.IdentificationNum_source, lit('NA'))) |
                (coalesce(target.CarModel, lit('N/A')) != coalesce(source.CarModel_source, lit('NA'))) |
                (coalesce(target.Province, lit('N/A')) != coalesce(source.Province_source, lit('NA'))) |
                (coalesce(target.Country, lit('N/A')) != coalesce(source.Country_source, lit('NA'))) |
                (coalesce(target.District, lit('N/A')) != coalesce(source.District_source, lit('NA'))) |
                (coalesce(target.PostalCode, lit('N/A')) != coalesce(source.PostalCode_source, lit('NA'))) |
                (coalesce(target.SubDistrict, lit('N/A')) != coalesce(source.SubDistrict_source, lit('NA'))) |
                (coalesce(target.MooSoi, lit('N/A')) != coalesce(source.MooSoi_source, lit('NA'))) |
                (coalesce(target.RoadStreet, lit('N/A')) != coalesce(source.RoadStreet_source, lit('NA'))) |
                (coalesce(target.HouseNo, lit('N/A')) != coalesce(source.HouseNo_source, lit('NA'))) |
                (coalesce(target.ContactStatus, lit('N/A')) != coalesce(source.ContactStatus_source, lit('NA'))) |
                (coalesce(target.ApprovalStatus, lit('N/A')) != coalesce(source.ApprovalStatus_source, lit('NA'))) |
                (target.IsTest != source.IsTest_source) |
                (coalesce(target.SamePriceSubmit, lit('N/A')) != coalesce(source.SamePriceSubmit_source, lit('NA'))) |
                (coalesce(target.MemberClass, lit('N/A')) != coalesce(source.MemberClass_source, lit('NA'))) |
                (coalesce(target.EmailAddr, lit('N/A')) != coalesce(source.EmailAddr_source, lit('NA'))) |
                (coalesce(target.EmailAddrOriginal, lit('N/A')) != coalesce(source.EmailAddrOriginal_source, lit('NA'))) |
                (coalesce(target.MemberID, lit('N/A')) != coalesce(source.MemberID_source, lit('NA'))) |
                (coalesce(target.DonationFlag, lit('N/A')) != coalesce(source.DonationFlag_source, lit('NA'))) |
                (coalesce(target.DonationCode, lit('N/A')) != coalesce(source.DonationCode_source, lit('NA'))) |
                (coalesce(target.DonationName, lit('N/A')) != coalesce(source.DonationName_source, lit('NA'))) |
                (coalesce(target.Chanel, lit('N/A')) != coalesce(source.Chanel_source, lit('NA'))) |
                (coalesce(target.UpdateBy, lit('N/A')) != coalesce(source.UpdateBy_source, lit('NA'))) |
                (coalesce(target.MEMBER_NUM, lit('N/A')) != coalesce(source.MEMBER_NUM_source, lit('NA'))) |
                (coalesce(target.JoinDate, lit('N/A')) != coalesce(source.JoinDate_source, lit('NA'))) |
                (target.JoinDateKey != source.JoinDateKey_source) |
                (target.JoinTimeKey != source.JoinTimeKey_source) |
                (target.CardExpireDateKey != source.CardExpireDateKey_source) |
                (coalesce(target.MemberGroup, lit('N/A')) != coalesce(source.MemberGroup_source, lit('NA'))) |
                (coalesce(target.RegisChannel, lit('N/A')) != coalesce(source.RegisChannel_source, lit('NA')))
            )
        )

        target_notMatched = target.join(source, on = join_key, how='left_anti').withColumn('action2',lit('NOT MATCHED'))

        update = target.join(source, on = join_key, how='inner')
        update = update.select([column for column in update.columns if '_source' in column]+['StartDateKey','StartTimeKey','EndDateKey','EndTimeKey','action','CardProfileKey'])
        update = update.withColumnsRenamed({column: column.replace('_source','') for column in update.columns}).withColumn('action2',lit('UPDATED'))
        update = utils.fillNaAllStringType(update)

        insert = source.join(target, on = (target.CardNum == source.CardNum_source), how='left_anti')
        insert = insert.withColumnsRenamed({column: column.replace('_source','') for column in insert.columns})

        thai_time = expr("current_timestamp() + INTERVAL 7 HOURS")
        insert = insert.withColumns({
            'LoadDateKey': date_format(thai_time, 'yyyyMMdd').cast(IntegerType()),
            'LoadTimeKey': date_format(thai_time, 'HHmmss').cast(IntegerType()),
            'StartDateKey': date_format(thai_time, 'yyyyMMdd').cast(IntegerType()),
            'StartTimeKey': date_format(thai_time, 'HHmmss').cast(IntegerType())
        }).withColumn('action2', lit('INSERT'))
        insert = utils.fillNaAllStringType(insert)

        semifinaldimsiebelcardprofile = target_notMatched.unionByName(update,allowMissingColumns=True).unionByName(insert,allowMissingColumns=True)
        semifinaldimsiebelcardprofile.write.mode('overwrite').save(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/semifinaldimsiebelcardprofile')

        semifinaldimsiebelcardprofile = spark.read.load(f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.BronzeLH_ID}/Tables/semifinaldimsiebelcardprofile')
        semifinaldimsiebelcardprofile.groupBy('action','action2').count().show()

        # update Primary key (CardProfileKey) of new records
        originalTalble = semifinaldimsiebelcardprofile
        pkey = 'CardProfileKey'
        order = 'CardNum'

        newRecord = originalTalble.filter(col(pkey).isNull())
        oldRecord = originalTalble.filter(col(pkey).isNotNull())
        maxOldKey = oldRecord.select(pkey).agg(max(pkey)).collect()[0][0]
        window_spec = Window.orderBy(order)
        newRecord = newRecord.withColumn(pkey, row_number().over(window_spec) + maxOldKey)

        finaldimsiebelcardprofile = oldRecord.unionByName(newRecord)
        df = finaldimsiebelcardprofile.drop('action','action2')

        df.agg(max('CardProfileKey'), count(col('CardProfileKey').isNotNull()), sum('LastStatusMember')).show()

        window_spec = Window.partitionBy("MemberID").orderBy(col("StartDateKey").desc(),col("CardIssuedDateKey").desc())
        df_with_row_num = df.withColumn(
            "row_num",
            when(col("EndDateKey").isNull(), row_number().over(window_spec)).otherwise(None)
        )
        df_with_row_num = df_with_row_num.withColumn('LastStatusMember',when(col('row_num')==1,1).otherwise(0))
        df_with_row_num.drop('row_num').write.mode('overwrite').saveAsTable('SilverLH.dimsiebelcardprofile')

    def factCardTransaction_Part1(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("factCardTransaction_Part1")
        print("=======================================================")

        stagingsiebelcardtransaction = spark.table('BronzeLH_Siebel.stagingsiebelcardtransaction')



        stagingsiebelcardtransaction = stagingsiebelcardtransaction.withColumn('CAST_CREATED',col('CREATED'))\
                                                                    .withColumn('CAST_LAST_UPD',col('LAST_UPD'))\
                                                                    .withColumn('CAST_LoadDateTime',col('LoadDateTime'))\
                                                                    .withColumn('CAST_PROCESS_DATE',col('PROCESS_DATE'))\
                                                                    .withColumn('CAST_TRANS_DATE',col('TRANS_DATE'))

        someTxnId = stagingsiebelcardtransaction.select('TXN_ID').limit(1).collect()[0].TXN_ID
        stagingsiebelcardtransaction.filter(col('TXN_ID')==someTxnId).select(['TXN_ID','CREATED','LAST_UPD','PROCESS_DATE','TRANS_DATE','LoadDateTime']).show()

        stagingsiebelcardtransaction = stagingsiebelcardtransaction.withColumnRenamed('X_CO2_TXN','TotalCO2')

        stagingsiebelcardtransaction = stagingsiebelcardtransaction\
                                            .withColumn('PRODUCT_ID_NotNull', when(col('PRODUCT_ID').isNull(),'0').otherwise(col('PRODUCT_ID')))
        dimsiebelcardprofile = spark.sql('SELECT cardnum,cardprofilekey FROM SilverLH.dimsiebelcardprofile WHERE enddatekey IS NULL')


        joinStagingAndDim = stagingsiebelcardtransaction.alias('left').join(dimsiebelcardprofile.alias('right'),
                                                        on = col("left.CARD_NUM") == col("right.cardnum"),
                                                        how = 'left')

        #Look up matched and no match
        matchedCardNum = joinStagingAndDim.filter(col("cardnum").isNotNull()).drop('cardnum')
        noMatchedCardNum = joinStagingAndDim.filter(col("cardnum").isNull()).drop('cardnum').withColumn('Error',lit("Mismatch MembershipKey")).withColumnRenamed('TotalCO2','X_CO2_TOTAL')


        dimProductWithMappingProduct = spark.sql("""
                                                    select a.SourceKey AS PRODUCT_ID,
                                                        a.MappingKey,
                                                        b.ProductGroup
                                                    from masterLH.mappingProduct a
                                                    left join SilverLH.dimProduct b on a.MappingKey = b.ProductKey
                                                    where a.SourceName = 'SIEBEL' and a.SourceFile = 'TRANSACTION'
                                                """).drop_duplicates()


        joinTalbleFOrMatched = matchedCardNum.alias('left').join(dimProductWithMappingProduct.withColumnRenamed('PRODUCT_ID','PRODUCT_ID_right').alias('right'),\
                                                        on = col("left.PRODUCT_ID_NotNull") == col("right.PRODUCT_ID_right"),\
                                                        how = 'left')


        # #Look up matched and no match
        noMatchedProdID = joinTalbleFOrMatched.filter(col("PRODUCT_ID_right").isNull()).drop('PRODUCT_ID_right').withColumn('Error',lit("Mismatch ProductKey")).withColumnRenamed('TotalCO2','X_CO2_TOTAL')
        matchedProdID = joinTalbleFOrMatched.filter(col("PRODUCT_ID_right").isNotNull()).drop('PRODUCT_ID_right')


        # (ProductGroup == "POINT" && MappingKey != 38 ? "NA" : (ProductGroup == "POINT" && MappingKey == 38 && ISNULL(MERCHANT_ROW_ID) ? "NA" : MERCHANT_ROW_ID))
        matchedProdID = matchedProdID.withColumn(
            "CAST_MERCHANT_ROW_ID",
            when(
                (col("ProductGroup") == "POINT") & (col("MappingKey") != 38), "NA"
            ).otherwise(
                when(
                    (col("ProductGroup") == "POINT") & (col("MappingKey") == 38) & col("MERCHANT_ROW_ID").isNull(), "NA"
                ).otherwise(
                    col("MERCHANT_ROW_ID")
                )
            )
        )

        # Look up mechandise key

        dimsiebelmerchandise = spark.sql("""
                                            select merchandisekey AS merchandisekey,
                                                rowid AS rowid_source
                                            from SilverLH.dimsiebelmerchandise
                                        """)


        joinTalbleFOrMatched = matchedProdID.alias('left').join(dimsiebelmerchandise.alias('right'),
                                                                on = col("left.CAST_MERCHANT_ROW_ID") == col("right.rowid_source"),
                                                                how = 'left')

        # Look up matched and no match
        noMatchedMerchan = joinTalbleFOrMatched.filter(col("rowid_source").isNull()).drop('rowid_source').withColumn('Error',lit("Mismatch MerchandiseKey")).withColumnRenamed('TotalCO2','X_CO2_TOTAL')
        matchedMerchan = joinTalbleFOrMatched.filter(col("rowid_source").isNotNull()).drop('rowid_source')


        matchedMerchan = matchedMerchan\
                            .withColumn('INT_CREATED_DATE',expr("CREATED + INTERVAL 7 HOURS"))\
                            .withColumn("INT_CREATED_DATE",date_format("INT_CREATED_DATE", "yyyyMMdd").cast(IntegerType()))\
                            .withColumn('INT_LAST_UPD_DATE',expr("LAST_UPD + INTERVAL 7 HOURS"))\
                            .withColumn("INT_LAST_UPD_DATE",date_format("INT_LAST_UPD_DATE", "yyyyMMdd").cast(IntegerType()))\
                            .withColumn('INT_LOAD_DATE',expr("LoadDateTime + INTERVAL 7 HOURS"))\
                            .withColumn("INT_LOAD_DATE",date_format("INT_LOAD_DATE", "yyyyMMdd").cast(IntegerType()))\
                            .withColumn('INT_TRANS_DATE',expr("TRANS_DATE + INTERVAL 7 HOURS"))\
                            .withColumn("INT_TRANS_DATE",date_format("INT_TRANS_DATE", "yyyyMMdd").cast(IntegerType()))\
                            .withColumn('INT_PROCESS_DATE',expr("PROCESS_DATE + INTERVAL 7 HOURS"))\
                            .withColumn("INT_PROCESS_DATE",date_format("INT_PROCESS_DATE", "yyyyMMdd").cast(IntegerType()))\
                            .withColumn('INT_CREATED_TIME',expr('CREATED + INTERVAL 7 HOURS'))\
                            .withColumn('INT_CREATED_TIME',date_format('INT_CREATED_TIME', "HHmmss").cast(IntegerType()))\
                            .withColumn('INT_LAST_UPD_TIME',expr('LAST_UPD + INTERVAL 7 HOURS'))\
                            .withColumn('INT_LAST_UPD_TIME',date_format('INT_LAST_UPD_TIME', "HHmmss").cast(IntegerType()))\
                            .withColumn('INT_LOAD_TIME',expr('LoadDateTime + INTERVAL 7 HOURS'))\
                            .withColumn('INT_LOAD_TIME',date_format('INT_LOAD_TIME', "HHmmss").cast(IntegerType()))\
                            .withColumn('INT_TRANS_TIME',expr('TRANS_DATE + INTERVAL 7 HOURS'))\
                            .withColumn('INT_TRANS_TIME',date_format('INT_TRANS_TIME', "HHmmss").cast(IntegerType()))\
                            .withColumn('INT_PROCESS_TIME',expr('PROCESS_DATE + INTERVAL 7 HOURS'))\
                            .withColumn('INT_PROCESS_TIME',date_format('INT_PROCESS_TIME', "HHmmss").cast(IntegerType()))

        column_mapping = {
            "AMOUNT": "Amount",
            "AMOUNT_IN_CAP": "AmountInCap",
            "AWARD_POINT": "AwardPoint",
            "BATCH_NUM": "BATCH_NUM",
            "BCP_CAP_DAY": "BcpCapDay",
            "BCP_CAP_TIME": "BcpCapTime",
            "CANCELLED_TXN_ID": "CancelledTxnId",
            "CAP_MONTH_BALANCE": "CapMonthBalance",
            "cardprofilekey": "CardProfileKey",
            "Channel": "Channel",
            "CHANNEL_TYPE": "CHANNEL_TYPE",
            "DescriptionComment": "DescriptionComment",
            "DONATION_TYPE": "DonationType",
            "EXPIRE_IN_DAYS": "ExpireInDays",
            "INT_CREATED_DATE": "CreatedDateKey",
            "INT_CREATED_TIME": "CreatedTimeKey",
            "INT_LAST_UPD_DATE": "LastUpdDateKey",
            "INT_LAST_UPD_TIME": "LastUpdTimeKey",
            "INT_LOAD_DATE": "LoadDateKey",
            "INT_LOAD_TIME": "LoadTimeKey",
            "INT_PROCESS_DATE": "ProcessDateKey",
            "INT_PROCESS_TIME": "ProcessTimeKey",
            "INT_TRANS_DATE": "TransDateKey",
            "INT_TRANS_TIME": "TransTimeKey",
            "MappingKey": "ProductKey",
            "MEMBER_CAP_MONTH_AVA": "MemberCapMonthAva",
            "MEMBER_ID": "MemberID",
            "merchandisekey": "MerchandiseKey",
            "MODIFICATION_NUM": "ModificationNum",
            "PAR_ROW_ID": "ParRowId",
            "POINT_BEFORE_AWARD": "PointBeforeAward",
            "QUALIFY_POINT": "QualifyPoint",
            "QUANTITY": "Quantity",
            "REDEEM_POINT": "RedeemPoint",
            "REMAINING_POINT": "RemainingPoint",
            "ROW_ID": "RowId",
            "SETTLE_FLG": "SettleFlg",
            "SYS_TRACE": "SYS_TRACE",
            "TERMINAL_ID": "TerminalId",
            "TRACE_NUM": "TRACE_NUM",
            "TRANS_NUM": "TransNum",
            "TRANS_STATUS": "TransStatus",
            "TRANS_SUB_TYPE": "TransSubType",
            "TRANS_TYPE": "TransType",
            "TXN_ID": "TxnId",
            "X_CO2_TOTAL": "TotalCO2"
        }

        tmpPrefact = matchedMerchan.withColumnsRenamed(column_mapping)

        cannotload = matchedMerchan.limit(0).withColumn('Error',lit("Cannot load to fact"))

        tmpPrefact = tmpPrefact.select(*spark.sql("SELECT * FROM SilverLH.tmpPrefactSiebelCardTransaction LIMIT 0").columns)

        try:
            tmpPrefact.write.mode('overwrite').saveAsTable('SilverLH.tmpPrefactSiebelCardTransaction')
            print('load to fact success')
        except:
            cannotload = matchedMerchan.withColumn('Error',lit("Cannot load to fact"))
            print('load to fact fail')

        cannotload = cannotload.withColumnRenamed('TotalCO2','X_CO2_TOTAL')

        noMatchedCardNumColumns = [
            "TRANS_DATE",
            "PROCESS_DATE",
            "TXN_ID",
            "CANCELLED_TXN_ID",
            "TRANS_NUM",
            "MEMBER_ID",
            "MEM_NUM",
            "CARD_NUM",
            "MEMBER_NAME",
            "TRANS_TYPE",
            "TRANS_SUB_TYPE",
            "TRANS_STATUS",
            "PRODUCT_ID",
            "PROD_CODE",
            "PROD_CATE",
            "PRODUCT_NAME",
            "PRODUCT_TYPE",
            "AMOUNT",
            "QUANTITY",
            "POINT_BEFORE_AWARD",
            "AWARD_POINT",
            "REDEEM_POINT",
            "REMAINING_POINT",
            "MERCHANT_ID",
            "MERCHANT_ROW_ID",
            "MERCHANT_NAME",
            "MERCHANT_NAME_THAI",
            "MERCHANT_BU",
            "TERMINAL_ID",
            "SETTLE_FLG",
            "QUALIFY_POINT",
            "DONATION_TYPE",
            "AMOUNT_IN_CAP",
            "BCP_CAP_DAY",
            "BCP_CAP_TIME",
            "MEMBER_CAP_MONTH_AVA",
            "CAP_MONTH_BALANCE",
            "EXPIRE_IN_DAYS",
            "ROW_ID",
            "PAR_ROW_ID",
            "MODIFICATION_NUM",
            "CREATED",
            "LAST_UPD",
            "LoadDateTime",
            "X_CO2_TOTAL",
            "DescriptionComment",
            "Channel",
            "BATCH_NUM",
            "TRACE_NUM",
            "CHANNEL_TYPE",
            "SYS_TRACE",
            "CAST_TRANS_DATE",
            "CAST_PROCESS_DATE",
            "CAST_CREATED",
            "CAST_LAST_UPD",
            "CAST_LoadDateTime",
            "Product_ID_NotNull",
            'Error'
        ]
        noMatchedCardNum = noMatchedCardNum.select(*noMatchedCardNumColumns)

        noMatchedProdIDcolumns = [
            "TRANS_DATE",
            "PROCESS_DATE",
            "TXN_ID",
            "CANCELLED_TXN_ID",
            "TRANS_NUM",
            "MEMBER_ID",
            "MEM_NUM",
            "CARD_NUM",
            "MEMBER_NAME",
            "TRANS_TYPE",
            "TRANS_SUB_TYPE",
            "TRANS_STATUS",
            "PRODUCT_ID",
            "PROD_CODE",
            "PROD_CATE",
            "PRODUCT_NAME",
            "PRODUCT_TYPE",
            "AMOUNT",
            "QUANTITY",
            "POINT_BEFORE_AWARD",
            "AWARD_POINT",
            "REDEEM_POINT",
            "REMAINING_POINT",
            "MERCHANT_ID",
            "MERCHANT_ROW_ID",
            "MERCHANT_NAME",
            "MERCHANT_NAME_THAI",
            "MERCHANT_BU",
            "TERMINAL_ID",
            "SETTLE_FLG",
            "QUALIFY_POINT",
            "DONATION_TYPE",
            "AMOUNT_IN_CAP",
            "BCP_CAP_DAY",
            "BCP_CAP_TIME",
            "MEMBER_CAP_MONTH_AVA",
            "CAP_MONTH_BALANCE",
            "EXPIRE_IN_DAYS",
            "ROW_ID",
            "PAR_ROW_ID",
            "MODIFICATION_NUM",
            "CREATED",
            "LAST_UPD",
            "LoadDateTime",
            "X_CO2_TOTAL",
            "DescriptionComment",
            "Channel",
            "BATCH_NUM",
            "TRACE_NUM",
            "CHANNEL_TYPE",
            "SYS_TRACE",
            "CAST_TRANS_DATE",
            "CAST_PROCESS_DATE",
            "CAST_CREATED",
            "CAST_LAST_UPD",
            "CAST_LoadDateTime",
            "Product_ID_NotNull",
            "cardprofilekey",
            'Error'
        ]
        noMatchedProdID = noMatchedProdID.select(*noMatchedProdIDcolumns)

        noMatchedMerchancolumns = [
            "TRANS_DATE",
            "PROCESS_DATE",
            "TXN_ID",
            "CANCELLED_TXN_ID",
            "TRANS_NUM",
            "MEMBER_ID",
            "MEM_NUM",
            "CARD_NUM",
            "MEMBER_NAME",
            "TRANS_TYPE",
            "TRANS_SUB_TYPE",
            "TRANS_STATUS",
            "PRODUCT_ID",
            "PROD_CODE",
            "PROD_CATE",
            "PRODUCT_NAME",
            "PRODUCT_TYPE",
            "AMOUNT",
            "QUANTITY",
            "POINT_BEFORE_AWARD",
            "AWARD_POINT",
            "REDEEM_POINT",
            "REMAINING_POINT",
            "MERCHANT_ID",
            "MERCHANT_ROW_ID",
            "MERCHANT_NAME",
            "MERCHANT_NAME_THAI",
            "MERCHANT_BU",
            "TERMINAL_ID",
            "SETTLE_FLG",
            "QUALIFY_POINT",
            "DONATION_TYPE",
            "AMOUNT_IN_CAP",
            "BCP_CAP_DAY",
            "BCP_CAP_TIME",
            "MEMBER_CAP_MONTH_AVA",
            "CAP_MONTH_BALANCE",
            "EXPIRE_IN_DAYS",
            "ROW_ID",
            "PAR_ROW_ID",
            "MODIFICATION_NUM",
            "CREATED",
            "LAST_UPD",
            "LoadDateTime",
            "X_CO2_TOTAL",
            "DescriptionComment",
            "Channel",
            "BATCH_NUM",
            "TRACE_NUM",
            "CHANNEL_TYPE",
            "SYS_TRACE",
            "CAST_TRANS_DATE",
            "CAST_PROCESS_DATE",
            "CAST_CREATED",
            "CAST_LAST_UPD",
            "CAST_LoadDateTime",
            "Product_ID_NotNull",
            "cardprofilekey",
            "MappingKey",
            "ProductGroup",
            "CAST_MERCHANT_ROW_ID",
            'Error'
        ]

        noMatchedMerchan = noMatchedMerchan.select(*noMatchedMerchancolumns)

        cannotloadcolumns = [
            "TRANS_DATE",
            "PROCESS_DATE",
            "TXN_ID",
            "CANCELLED_TXN_ID",
            "TRANS_NUM",
            "MEMBER_ID",
            "MEM_NUM",
            "CARD_NUM",
            "MEMBER_NAME",
            "TRANS_TYPE",
            "TRANS_SUB_TYPE",
            "TRANS_STATUS",
            "PRODUCT_ID",
            "PROD_CODE",
            "PROD_CATE",
            "PRODUCT_NAME",
            "PRODUCT_TYPE",
            "AMOUNT",
            "QUANTITY",
            "POINT_BEFORE_AWARD",
            "AWARD_POINT",
            "REDEEM_POINT",
            "REMAINING_POINT",
            "MERCHANT_ID",
            "MERCHANT_ROW_ID",
            "MERCHANT_NAME",
            "MERCHANT_NAME_THAI",
            "MERCHANT_BU",
            "TERMINAL_ID",
            "SETTLE_FLG",
            "QUALIFY_POINT",
            "DONATION_TYPE",
            "AMOUNT_IN_CAP",
            "BCP_CAP_DAY",
            "BCP_CAP_TIME",
            "MEMBER_CAP_MONTH_AVA",
            "CAP_MONTH_BALANCE",
            "EXPIRE_IN_DAYS",
            "ROW_ID",
            "PAR_ROW_ID",
            "MODIFICATION_NUM",
            "CREATED",
            "LAST_UPD",
            "LoadDateTime",
            "X_CO2_TOTAL",
            "DescriptionComment",
            "Channel",
            "BATCH_NUM",
            "TRACE_NUM",
            "CHANNEL_TYPE",
            "SYS_TRACE",
            "CAST_TRANS_DATE",
            "CAST_PROCESS_DATE",
            "CAST_CREATED",
            "CAST_LAST_UPD",
            "CAST_LoadDateTime",
            "Product_ID_NotNull",
            "cardprofilekey",
            "MappingKey",
            "ProductGroup",
            "CAST_MERCHANT_ROW_ID",
            "merchandisekey",
            "INT_CREATED_DATE",
            "INT_LAST_UPD_DATE",
            "INT_LOAD_DATE",
            "INT_TRANS_DATE",
            "INT_PROCESS_DATE",
            "INT_CREATED_TIME",
            "INT_LAST_UPD_TIME",
            "INT_LOAD_TIME",
            "INT_TRANS_TIME",
            "INT_PROCESS_TIME",
            "Error"
        ]
        cannotload = cannotload.select(*cannotloadcolumns)

        tmpPreMismatchSiebelCardTransaction = cannotload.unionByName(noMatchedCardNum,allowMissingColumns=True)\
                                                        .unionByName(noMatchedProdID,allowMissingColumns=True)\
                                                        .unionByName(noMatchedMerchan,allowMissingColumns=True)


        tmpPreMismatchSiebelCardTransaction = tmpPreMismatchSiebelCardTransaction.withColumn('INT_CREATED_DATE',expr("CAST_CREATED + INTERVAL 7 HOURS"))\
                            .withColumn("INT_CREATED_DATE",date_format("INT_CREATED_DATE", "yyyyMMdd").cast(IntegerType()))\
                            .withColumn('INT_LAST_UPD_DATE',expr("CAST_LAST_UPD + INTERVAL 7 HOURS"))\
                            .withColumn("INT_LAST_UPD_DATE",date_format("INT_LAST_UPD_DATE", "yyyyMMdd").cast(IntegerType()))\
                            .withColumn('INT_LOAD_DATE',expr("CAST_LoadDateTime + INTERVAL 7 HOURS"))\
                            .withColumn("INT_LOAD_DATE",date_format("INT_LOAD_DATE", "yyyyMMdd").cast(IntegerType()))\
                            .withColumn('INT_TRANS_DATE',expr("CAST_TRANS_DATE + INTERVAL 7 HOURS"))\
                            .withColumn("INT_TRANS_DATE",date_format("INT_TRANS_DATE", "yyyyMMdd").cast(IntegerType()))\
                            .withColumn('INT_PROCESS_DATE',expr("CAST_PROCESS_DATE + INTERVAL 7 HOURS"))\
                            .withColumn("INT_PROCESS_DATE",date_format("INT_PROCESS_DATE", "yyyyMMdd").cast(IntegerType()))\
                            .withColumn('INT_CREATED_TIME',expr('CAST_CREATED + INTERVAL 7 HOURS'))\
                            .withColumn('INT_CREATED_TIME',date_format('INT_CREATED_TIME', "HHmmss").cast(IntegerType()))\
                            .withColumn('INT_LAST_UPD_TIME',expr('CAST_LAST_UPD + INTERVAL 7 HOURS'))\
                            .withColumn('INT_LAST_UPD_TIME',date_format('INT_LAST_UPD_TIME', "HHmmss").cast(IntegerType()))\
                            .withColumn('INT_LOAD_TIME',expr('CAST_LoadDateTime + INTERVAL 7 HOURS'))\
                            .withColumn('INT_LOAD_TIME',date_format('INT_LOAD_TIME', "HHmmss").cast(IntegerType()))\
                            .withColumn('INT_TRANS_TIME',expr('CAST_TRANS_DATE + INTERVAL 7 HOURS'))\
                            .withColumn('INT_TRANS_TIME',date_format('INT_TRANS_TIME', "HHmmss").cast(IntegerType()))\
                            .withColumn('INT_PROCESS_TIME',expr('CAST_PROCESS_DATE + INTERVAL 7 HOURS'))\
                            .withColumn('INT_PROCESS_TIME',date_format('INT_PROCESS_TIME', "HHmmss").cast(IntegerType()))


        column_mapping_mismatch = {
            "AMOUNT": "AMOUNT",
            "AMOUNT_IN_CAP": "AMOUNT_IN_CAP",
            "AWARD_POINT": "AWARD_POINT",
            "BATCH_NUM": "BATCH_NUM",
            "BCP_CAP_DAY": "BCP_CAP_DAY",
            "BCP_CAP_TIME": "BCP_CAP_TIME",
            "CANCELLED_TXN_ID": "CANCELLED_TXN_ID",
            "CAP_MONTH_BALANCE": "CAP_MONTH_BALANCE",
            "CARD_NUM": "CARD_NUM",
            "CAST_CREATED": "CAST_CREATED",
            "CAST_LAST_UPD": "CAST_LAST_UPD",
            "CAST_LoadDateTime": "CAST_LoadDateTime",
            "CAST_PROCESS_DATE": "CAST_PROCESS_DATE",
            "CAST_TRANS_DATE": "CAST_TRANS_DATE",
            "Channel": "Channel",
            "CHANNEL_TYPE": "CHANNEL_TYPE",
            "DescriptionComment": "DescriptionComment",
            "DONATION_TYPE": "DONATION_TYPE",
            "Error": "Error",
            "EXPIRE_IN_DAYS": "EXPIRE_IN_DAYS",
            "INT_CREATED_DATE": "CreatedDateKey",
            "INT_CREATED_TIME": "CreatedTimeKey",
            "INT_LAST_UPD_DATE": "LastUpdDateKey",
            "INT_LAST_UPD_TIME": "LastUpdTimeKey",
            "INT_LOAD_DATE": "LoadDateKey",
            "INT_LOAD_TIME": "LoadTimeKey",
            "INT_PROCESS_DATE": "ProcessDateKey",
            "INT_PROCESS_TIME": "ProcessTimeKey",
            "INT_TRANS_DATE": "TransDateKey",
            "INT_TRANS_TIME": "TransTimeKey",
            "MEM_NUM": "MEM_NUM",
            "MEMBER_CAP_MONTH_AVA": "MEMBER_CAP_MONTH_AVA",
            "MEMBER_ID": "MEMBER_ID",
            "MEMBER_NAME": "MEMBER_NAME",
            "MERCHANT_BU": "MERCHANT_BU",
            "MERCHANT_ID": "MERCHANT_ID",
            "MERCHANT_NAME": "MERCHANT_NAME",
            "MERCHANT_NAME_THAI": "MERCHANT_NAME_THAI",
            "MERCHANT_ROW_ID": "MERCHANT_ROW_ID",
            "MODIFICATION_NUM": "MODIFICATION_NUM",
            "PAR_ROW_ID": "PAR_ROW_ID",
            "POINT_BEFORE_AWARD": "POINT_BEFORE_AWARD",
            "PROD_CATE": "PROD_CATE",
            "PROD_CODE": "PROD_CODE",
            "PRODUCT_ID": "PRODUCT_ID",
            "PRODUCT_NAME": "PRODUCT_NAME",
            "PRODUCT_TYPE": "PRODUCT_TYPE",
            "QUALIFY_POINT": "QUALIFY_POINT",
            "QUANTITY": "QUANTITY",
            "REDEEM_POINT": "REDEEM_POINT",
            "REMAINING_POINT": "REMAINING_POINT",
            "ROW_ID": "ROW_ID",
            "SETTLE_FLG": "SETTLE_FLG",
            "SYS_TRACE": "SYS_TRACE",
            "TERMINAL_ID": "TERMINAL_ID",
            "TRACE_NUM": "TRACE_NUM",
            "TRANS_NUM": "TRANS_NUM",
            "TRANS_STATUS": "TRANS_STATUS",
            "TRANS_SUB_TYPE": "TRANS_SUB_TYPE",
            "TRANS_TYPE": "TRANS_TYPE",
            "TXN_ID": "TXN_ID",
            "X_CO2_TOTAL": "TotalCO2"
        }


        tmpPreMismatchSiebelCardTransaction = tmpPreMismatchSiebelCardTransaction.withColumnsRenamed(column_mapping_mismatch).select(*column_mapping_mismatch.values())

        tmpPreMismatchSiebelCardTransaction.write.mode('overwrite').saveAsTable('SilverLH.tmpPreMismatchSiebelCardTransaction')

    def factCardTransaction_Part2(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("factCardTransaction_Part2")
        print("=======================================================")

        tmpPrefactSiebelCardTransaction = spark.table("SilverLH.tmpPrefactSiebelCardTransaction")
        factSiebelCardTransactionYesterday = spark.table('SilverLH.factSiebelCardTransaction_v2')
        factSiebelCardTransactionToday = spark.table('SilverLH.factsiebelcardtransaction_v2_today')
        factSiebelCardTransaction = factSiebelCardTransactionYesterday.unionByName(factSiebelCardTransactionToday,allowMissingColumns=True)

        df = tmpPrefactSiebelCardTransaction
        source = df.select(
                    coalesce(df.TransDateKey, lit(20180101)).alias('TransDateKey'),
                    coalesce(df.TransTimeKey, lit(20180101)).alias('TransTimeKey'),
                    coalesce(df.ProcessDateKey, lit(20180101)).alias('ProcessDateKey'),
                    coalesce(df.ProcessTimeKey, lit(20180101)).alias('ProcessTimeKey'),
                    coalesce(df.TxnId, lit('NA')).alias('TxnId'),
                    coalesce(df.CancelledTxnId, lit('NA')).alias('CancelledTxnId'),
                    coalesce(df.TransNum, lit('NA')).alias('TransNum'),
                    col('CardProfileKey').alias('CardProfileKey'),
                    coalesce(df.TransType, lit('NA')).alias('TransType'),
                    coalesce(df.TransSubType, lit('NA')).alias('TransSubType'),
                    coalesce(df.TransStatus, lit('NA')).alias('TransStatus'),
                    coalesce(df.Amount, lit(0)).alias('Amount'),
                    coalesce(df.Quantity, lit(0)).alias('Quantity'),
                    coalesce(df.PointBeforeAward, lit(0)).alias('PointBeforeAward'),
                    coalesce(df.AwardPoint, lit(0)).alias('AwardPoint'),
                    coalesce(df.RedeemPoint, lit(0)).alias('RedeemPoint'),
                    (df.PointBeforeAward + df.AwardPoint - coalesce(df.RedeemPoint, lit(0))).alias('RemainingPoint'),
                    df.MerchandiseKey.alias('MerchandiseKey'),
                    coalesce(df.TerminalId, lit('NA')).alias('TerminalId'),
                    coalesce(df.SettleFlg, lit('NA')).alias('SettleFlg'),
                    coalesce(df.QualifyPoint, lit('NA')).alias('QualifyPoint'),
                    coalesce(df.DonationType, lit('NA')).alias('DonationType'),
                    coalesce(df.AmountInCap, lit(0)).alias('AmountInCap'),
                    coalesce(df.BcpCapDay, lit(0)).alias('BcpCapDay'),
                    coalesce(df.BcpCapTime, lit(0)).alias('BcpCapTime'),
                    coalesce(df.MemberCapMonthAva, lit(0)).alias('MemberCapMonthAva'),
                    coalesce(df.CapMonthBalance, lit(0)).alias('CapMonthBalance'),
                    coalesce(df.ExpireInDays, lit(0)).alias('ExpireInDays'),
                    coalesce(df.RowId, lit('NA')).alias('RowId'),
                    coalesce(df.ParRowId, lit('NA')).alias('ParRowId'),
                    coalesce(df.ModificationNum, lit(0)).alias('ModificationNum'),
                    coalesce(df.CreatedDateKey, lit(20180101)).alias('CreatedDateKey'),
                    coalesce(df.CreatedTimeKey, lit(20180101)).alias('CreatedTimeKey'),
                    coalesce(df.LastUpdDateKey, lit(20180101)).alias('LastUpdDateKey'),
                    coalesce(df.LastUpdTimeKey, lit(20180101)).alias('LastUpdTimeKey'),
                    coalesce(df.LoadDateKey, lit(20180101)).alias('LoadDateKey'),
                    coalesce(df.LoadTimeKey, lit(20180101)).alias('LoadTimeKey'),
                    df.ProductKey.alias('ProductKey'),
                    coalesce(df.TotalCO2, lit(0)).alias('TotalCO2'),
                    coalesce(df.DescriptionComment, lit('NA')).alias('DescriptionComment'),
                    coalesce(df.Channel, lit('NA')).alias('Channel'),
                    coalesce(df.MemberID, lit('NA')).alias('MemberID'),
                    coalesce(df.BATCH_NUM, lit('NA')).alias('BATCH_NUM'),
                    coalesce(df.TRACE_NUM, lit('NA')).alias('TRACE_NUM'),
                    coalesce(df.SYS_TRACE, lit('NA')).alias('SYS_TRACE'),
                    coalesce(df.CHANNEL_TYPE, lit('NA')).alias('CHANNEL_TYPE')
                )
        source = source.withColumn('TransYearKey', (col('TransDateKey')/10000).cast(IntegerType())).withColumn('TransMonthKey', (col('TransDateKey')/100).cast(IntegerType())%100)
        old_columns = source.columns
        reverseMapper = {column+'_source':column for column in old_columns}
        columnMapper = {column:column+'_source' for column in old_columns}
        source = source.withColumnsRenamed(columnMapper)  

        target = factSiebelCardTransaction
        join_key = (target.TxnId == source.TxnId_source)

        left_anti = target.join(source, on = join_key, how='left_anti').withColumn('action',lit('NOT MATCHED'))
        # utils.trackSizeTable(left_anti,detail='NOT MATCHED')

        inner = target.join(source, on = join_key, how='inner') # MATCHED: which has both column from target and from source (<name>_source)
        inner = inner.select([column for column in inner.columns if '_source' in column]).withColumnsRenamed(reverseMapper).withColumn('action',lit('UPDATED'))
        # utils.trackSizeTable(inner,detail='UPDATED')

        insert = source.join(target, on = join_key, how='left_anti').withColumnsRenamed(reverseMapper).withColumn('action',lit('INSERT'))
        # utils.trackSizeTable(insert,detail='INSERT')

        newfactSiebelCardTransaction = left_anti.unionByName(inner,allowMissingColumns=True).unionByName(insert,allowMissingColumns=True)
        # utils.trackSizeTable(newfactSiebelCardTransaction,detail='after merge')
        updateResult = newfactSiebelCardTransaction.groupBy('action').count()
        updateResult.show()

        newfactSiebelCardTransaction = utils.copySchemaByName(newfactSiebelCardTransaction,factSiebelCardTransaction)

        # # special condition for wait to edit
        # extra = newfactSiebelCardTransaction.filter(col('TxnId')=='1-S1V4Y13')
        # try:
        #     extra.drop('action').withColumn('date',current_timestamp()).write.mode('append').saveAsTable('SilverLH.ExtrafactSiebelCardTransaction_V2')
        # except:
        #     extra.drop('action').withColumn('date',current_timestamp()).write.mode('overwrite').saveAsTable('SilverLH.ExtrafactSiebelCardTransaction_V2')

        # newfactSiebelCardTransaction = newfactSiebelCardTransaction.filter(col('TxnId') !='1-S1V4Y13')

        newfactSiebelCardTransaction.drop('action').write\
            .partitionBy("TransDateKey")\
            .mode('overwrite').partitionBy(['TransYearKey','TransMonthKey','TransType','TransStatus'])\
            .option("overwriteSchema", "true").saveAsTable('SilverLH.factSiebelCardTransaction_V2')
        
        return updateResult

    def factCardTransaction_Part3(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("factCardTransaction_Part3")
        print("=======================================================")

        spark.sql(
        """
            -- mismatch
            -- script for new mismatchsiebel
            MERGE INTO SilverLH.mismatchSiebelCardTransaction_V2 AS TARGET
            USING (SELECT a.TransDateKey as TransDateKey
                    ,a.TransTimeKey as TransTimeKey
                    ,a.ProcessDateKey as ProcessDateKey
                    ,a.ProcessTimeKey as ProcessTimeKey
                    ,a.TXN_ID as TXN_ID
                    ,coalesce(a.CANCELLED_TXN_ID,'NA') as CANCELLED_TXN_ID
                    ,a.TRANS_NUM as TRANS_NUM
                    ,a.MEMBER_ID as MEMBER_ID
                    ,coalesce(a.MEM_NUM,'NA') as MEM_NUM
                    ,coalesce(a.CARD_NUM,'NA') as CARD_NUM
                    ,coalesce(a.MEMBER_NAME,'NA') as MEMBER_NAME
                    ,a.TRANS_TYPE as TRANS_TYPE
                    ,a.TRANS_SUB_TYPE as TRANS_SUB_TYPE
                    ,a.TRANS_STATUS as TRANS_STATUS
                    ,a.PRODUCT_ID as PRODUCT_ID
                    ,a.PROD_CODE as PROD_CODE
                    ,a.PROD_CATE as PROD_CATE
                    ,a.PRODUCT_NAME as PRODUCT_NAME
                    ,a.PRODUCT_TYPE as PRODUCT_TYPE
                    ,coalesce(a.AMOUNT,0) as AMOUNT
                    ,coalesce(a.POINT_BEFORE_AWARD,0) as POINT_BEFORE_AWARD
                    ,coalesce(a.AWARD_POINT,0) as AWARD_POINT
                    ,coalesce(a.REDEEM_POINT,0) as REDEEM_POINT
                    ,(a.POINT_BEFORE_AWARD + a.AWARD_POINT) - coalesce(a.REDEEM_POINT, 0) as REMAINING_POINT
                    --a.REMAINING_POINT as REMAINING_POINT
                    ,coalesce(a.MERCHANT_ID,'NA') as MERCHANT_ID
                    ,coalesce(a.MERCHANT_ROW_ID,'NA') as MERCHANT_ROW_ID
                    ,coalesce(a.MERCHANT_NAME,'NA') as MERCHANT_NAME
                    ,coalesce(a.MERCHANT_NAME_THAI,'NA') as MERCHANT_NAME_THAI
                    ,coalesce(a.MERCHANT_BU,'NA') as MERCHANT_BU
                    ,coalesce(a.TERMINAL_ID,'NA') as TERMINAL_ID
                    ,coalesce(a.SETTLE_FLG,'NA') as SETTLE_FLG
                    ,a.QUALIFY_POINT as QUALIFY_POINT
                    ,coalesce(a.DONATION_TYPE,'NA') as DONATION_TYPE
                    ,coalesce(a.AMOUNT_IN_CAP,0) as AMOUNT_IN_CAP
                    ,coalesce(a.BCP_CAP_DAY,0) as BCP_CAP_DAY
                    ,coalesce(a.BCP_CAP_TIME,0) as BCP_CAP_TIME
                    ,coalesce(a.MEMBER_CAP_MONTH_AVA,0) as MEMBER_CAP_MONTH_AVA
                    ,coalesce(a.CAP_MONTH_BALANCE,0) as CAP_MONTH_BALANCE
                    ,coalesce(a.EXPIRE_IN_DAYS,0) as EXPIRE_IN_DAYS
                    ,coalesce(a.ROW_ID,'NA') as ROW_ID
                    ,coalesce(a.PAR_ROW_ID,'NA') as PAR_ROW_ID
                    ,a.MODIFICATION_NUM as MODIFICATION_NUM
                    ,coalesce(a.CreatedDateKey, 20180101) as CreatedDateKey
                    ,coalesce(a.CreatedTimeKey, 20180101) as CreatedTimeKey
                    ,a.LastUpdDateKey as LastUpdDateKey
                    ,a.LastUpdTimeKey as LastUpdTimeKey
                    ,a.LoadDateKey as LoadDateKey
                    ,a.LoadTimeKey as LoadTimeKey
                    ,coalesce(a.QUANTITY,0) as QUANTITY
                    ,a.CAST_TRANS_DATE as CAST_TRANS_DATE
                    ,a.CAST_PROCESS_DATE as CAST_PROCESS_DATE
                    ,a.CAST_CREATED as CAST_CREATED
                    ,a.CAST_LAST_UPD as CAST_LAST_UPD
                    ,a.CAST_LoadDateTime as CAST_LoadDateTime
                    ,a.Error as Error
                    ,a.TotalCO2 as TotalCO2
                    ,coalesce(a.DescriptionComment,'NA') as DescriptionComment
                    ,a.Channel as Channel	
                    ,coalesce(a.BATCH_NUM, 'NA') as BATCH_NUM --09/08/2022
                    ,coalesce(a.TRACE_NUM, 'NA') as TRACE_NUM --09/08/2022
                    ,coalesce(a.SYS_TRACE, 'NA') as SYS_TRACE --09/08/2022
                    ,coalesce(a.CHANNEL_TYPE, 'NA') as CHANNEL_TYPE --09/08/2022
            FROM SilverLH.tmppremismatchsiebelcardtransaction a
            ) AS SOURCE
            ON (TARGET.TXN_ID = SOURCE.TXN_ID)
            -- When records are matched, update
            -- the records if there is any change
            WHEN MATCHED AND
                TARGET.TransDateKey <> SOURCE.TransDateKey
                OR TARGET.TransTimeKey<>SOURCE.TransTimeKey
                OR TARGET.ProcessDateKey<>SOURCE.ProcessDateKey
                OR TARGET.ProcessTimeKey<>SOURCE.ProcessTimeKey
                OR TARGET.TXN_ID<>SOURCE.TXN_ID
                OR coalesce(TARGET.CANCELLED_TXN_ID,'NA')<>SOURCE.CANCELLED_TXN_ID
                OR TARGET.TRANS_NUM<>SOURCE.TRANS_NUM
                OR TARGET.MEMBER_ID<>SOURCE.MEMBER_ID
                OR coalesce(TARGET.MEM_NUM,'NA')<>SOURCE.MEM_NUM
                OR coalesce(TARGET.CARD_NUM,'NA')<>SOURCE.CARD_NUM
                OR coalesce(TARGET.MEMBER_NAME,'NA')<>SOURCE.MEMBER_NAME
                OR coalesce(TARGET.TRANS_TYPE,'NA')<>SOURCE.TRANS_TYPE
                OR coalesce(TARGET.TRANS_SUB_TYPE,'NA')<>SOURCE.TRANS_SUB_TYPE
                OR coalesce(TARGET.TRANS_STATUS,'NA')<>SOURCE.TRANS_STATUS
                OR coalesce(TARGET.PRODUCT_ID,'NA')<>SOURCE.PRODUCT_ID
                OR coalesce(TARGET.PROD_CODE,'NA')<>SOURCE.PROD_CODE
                or coalesce(TARGET.PROD_CATE,'NA')<>SOURCE.PROD_CATE
                or coalesce(TARGET.PRODUCT_NAME,'NA')<>SOURCE.PRODUCT_NAME
                or coalesce(TARGET.PRODUCT_TYPE,'NA')<>SOURCE.PRODUCT_TYPE
                or coalesce(TARGET.AMOUNT,0)<>SOURCE.AMOUNT
                or coalesce(TARGET.POINT_BEFORE_AWARD,0)<>SOURCE.POINT_BEFORE_AWARD
                or coalesce(TARGET.AWARD_POINT,0)<>SOURCE.AWARD_POINT
                or coalesce(TARGET.REDEEM_POINT,0)<>SOURCE.REDEEM_POINT
                or coalesce(TARGET.REMAINING_POINT,0)<>SOURCE.REMAINING_POINT
                or coalesce(TARGET.MERCHANT_ID,'NA')<>SOURCE.MERCHANT_ID
                or coalesce(TARGET.MERCHANT_ROW_ID,'NA')<>SOURCE.MERCHANT_ROW_ID
                or coalesce(TARGET.MERCHANT_NAME,'NA')<>SOURCE.MERCHANT_NAME
                or coalesce(TARGET.MERCHANT_NAME_THAI,'NA')<>SOURCE.MERCHANT_NAME_THAI
                or coalesce(TARGET.MERCHANT_BU,'NA')<>SOURCE.MERCHANT_BU
                or coalesce(TARGET.TERMINAL_ID,'NA')<>SOURCE.TERMINAL_ID
                or coalesce(TARGET.SETTLE_FLG,'NA')<>SOURCE.SETTLE_FLG
                or coalesce(TARGET.QUALIFY_POINT,'NA')<>SOURCE.QUALIFY_POINT
                or coalesce(TARGET.DONATION_TYPE,'NA')<>SOURCE.DONATION_TYPE
                or coalesce(TARGET.AMOUNT_IN_CAP,0)<>SOURCE.AMOUNT_IN_CAP
                or coalesce(TARGET.BCP_CAP_DAY,0)<>SOURCE.BCP_CAP_DAY
                or coalesce(TARGET.BCP_CAP_TIME,0)<>SOURCE.BCP_CAP_TIME
                or coalesce(TARGET.MEMBER_CAP_MONTH_AVA,0)<>SOURCE.MEMBER_CAP_MONTH_AVA
                or coalesce(TARGET.CAP_MONTH_BALANCE,0)<>SOURCE.CAP_MONTH_BALANCE
                or coalesce(TARGET.EXPIRE_IN_DAYS,0)<>SOURCE.EXPIRE_IN_DAYS
                or coalesce(TARGET.ROW_ID,'NA')<>SOURCE.ROW_ID
                or coalesce(TARGET.PAR_ROW_ID,'NA')<>SOURCE.PAR_ROW_ID
                or coalesce(TARGET.MODIFICATION_NUM,0)<>SOURCE.MODIFICATION_NUM
                or coalesce(TARGET.CreatedDateKey,20180101)<>SOURCE.CreatedDateKey
                or coalesce(TARGET.CreatedTimeKey,20180101)<>SOURCE.CreatedTimeKey
                or coalesce(TARGET.LastUpdDateKey,20180101)<>SOURCE.LastUpdDateKey
                or coalesce(TARGET.LastUpdTimeKey,20180101)<>SOURCE.LastUpdTimeKey
                or TARGET.LoadDateKey<>SOURCE.LoadDateKey
                or TARGET.LoadTimeKey<>SOURCE.LoadTimeKey
                or coalesce(TARGET.QUANTITY,0)<>SOURCE.QUANTITY
                or TARGET.CAST_TRANS_DATE<>SOURCE.CAST_TRANS_DATE
                or TARGET.CAST_PROCESS_DATE<>SOURCE.CAST_PROCESS_DATE
                or TARGET.CAST_CREATED<>SOURCE.CAST_CREATED
                or TARGET.CAST_LAST_UPD<>SOURCE.CAST_LAST_UPD
                or TARGET.CAST_LoadDateTime<>SOURCE.CAST_LoadDateTime
                or coalesce(TARGET.Error,'NA')<>SOURCE.Error
                or coalesce(TARGET.TotalCO2,'NA')<>SOURCE.TotalCO2
                or coalesce(TARGET.DescriptionComment,'NA') <> SOURCE.DescriptionComment
                or coalesce(TARGET.Channel,'NA')<>SOURCE.Channel
                or coalesce(TARGET.BATCH_NUM,'NA')<>SOURCE.BATCH_NUM --09/08/2022
                or coalesce(TARGET.TRACE_NUM,'NA')<>SOURCE.TRACE_NUM --09/08/2022
                or coalesce(TARGET.SYS_TRACE,'NA')<>SOURCE.SYS_TRACE --09/08/2022
                or coalesce(TARGET.CHANNEL_TYPE,'NA')<>SOURCE.CHANNEL_TYPE --09/08/2022
            THEN 
            UPDATE SET 
                TARGET.TransDateKey=SOURCE.TransDateKey
                ,TARGET.TransTimeKey=SOURCE.TransTimeKey
                ,TARGET.ProcessDateKey=SOURCE.ProcessDateKey
                ,TARGET.ProcessTimeKey=SOURCE.ProcessTimeKey
                ,TARGET.TXN_ID=SOURCE.TXN_ID
                ,TARGET.CANCELLED_TXN_ID=coalesce(SOURCE.CANCELLED_TXN_ID,'NA')
                ,TARGET.TRANS_NUM=SOURCE.TRANS_NUM
                ,TARGET.MEMBER_ID=SOURCE.MEMBER_ID
                ,TARGET.MEM_NUM=coalesce(SOURCE.MEM_NUM,'NA')
                ,TARGET.CARD_NUM=coalesce(SOURCE.CARD_NUM,'NA')
                ,TARGET.MEMBER_NAME=coalesce(SOURCE.MEMBER_NAME,'NA')
                ,TARGET.TRANS_TYPE=coalesce(SOURCE.TRANS_TYPE,'NA')
                ,TARGET.TRANS_SUB_TYPE=coalesce(SOURCE.TRANS_SUB_TYPE,'NA')
                ,TARGET.TRANS_STATUS=coalesce(SOURCE.TRANS_STATUS,'NA')
                ,TARGET.PRODUCT_ID=coalesce(SOURCE.PRODUCT_ID,'NA')
                ,TARGET.PROD_CODE=coalesce(SOURCE.PROD_CODE,'NA')
                ,TARGET.PROD_CATE=coalesce(SOURCE.PROD_CATE,'NA')
                ,TARGET.PRODUCT_NAME=coalesce(SOURCE.PRODUCT_NAME,'NA')
                ,TARGET.PRODUCT_TYPE=coalesce(SOURCE.PRODUCT_TYPE,'NA')
                ,TARGET.AMOUNT=coalesce(SOURCE.AMOUNT,0)
                ,TARGET.POINT_BEFORE_AWARD=coalesce(SOURCE.POINT_BEFORE_AWARD,0)
                ,TARGET.AWARD_POINT=coalesce(SOURCE.AWARD_POINT,0)
                ,TARGET.REDEEM_POINT=coalesce(SOURCE.REDEEM_POINT,0)
                ,TARGET.REMAINING_POINT=coalesce(SOURCE.REMAINING_POINT,0)
                ,TARGET.MERCHANT_ID=coalesce(SOURCE.MERCHANT_ID,'NA')
                ,TARGET.MERCHANT_ROW_ID=coalesce(SOURCE.MERCHANT_ROW_ID,'NA')
                ,TARGET.MERCHANT_NAME=coalesce(SOURCE.MERCHANT_NAME,'NA')
                ,TARGET.MERCHANT_NAME_THAI=coalesce(SOURCE.MERCHANT_NAME_THAI,'NA')
                ,TARGET.MERCHANT_BU=coalesce(SOURCE.MERCHANT_BU,'NA')
                ,TARGET.TERMINAL_ID=coalesce(SOURCE.TERMINAL_ID,'NA')
                ,TARGET.SETTLE_FLG=coalesce(SOURCE.SETTLE_FLG,'NA')
                ,TARGET.QUALIFY_POINT=coalesce(SOURCE.QUALIFY_POINT,'NA')
                ,TARGET.DONATION_TYPE=coalesce(SOURCE.DONATION_TYPE,'NA')
                ,TARGET.AMOUNT_IN_CAP=coalesce(SOURCE.AMOUNT_IN_CAP,0)
                ,TARGET.BCP_CAP_DAY=coalesce(SOURCE.BCP_CAP_DAY,0)
                ,TARGET.BCP_CAP_TIME=coalesce(SOURCE.BCP_CAP_TIME,0)
                ,TARGET.MEMBER_CAP_MONTH_AVA=coalesce(SOURCE.MEMBER_CAP_MONTH_AVA,0)
                ,TARGET.CAP_MONTH_BALANCE=coalesce(SOURCE.CAP_MONTH_BALANCE,0)
                ,TARGET.EXPIRE_IN_DAYS=coalesce(SOURCE.EXPIRE_IN_DAYS,0)
                ,TARGET.ROW_ID=coalesce(SOURCE.ROW_ID,'NA')
                ,TARGET.PAR_ROW_ID=coalesce(SOURCE.PAR_ROW_ID,'NA')
                ,TARGET.MODIFICATION_NUM=coalesce(SOURCE.MODIFICATION_NUM,'NA')
                ,TARGET.CreatedDateKey=coalesce(SOURCE.CreatedDateKey,20180101)
                ,TARGET.CreatedTimeKey=coalesce(SOURCE.CreatedTimeKey,20180101)
                ,TARGET.LastUpdDateKey=coalesce(SOURCE.LastUpdDateKey,20180101)
                ,TARGET.LastUpdTimeKey=coalesce(SOURCE.LastUpdTimeKey,20180101)
                ,TARGET.LoadDateKey=SOURCE.LoadDateKey
                ,TARGET.LoadTimeKey=SOURCE.LoadTimeKey
                ,TARGET.QUANTITY=coalesce(SOURCE.QUANTITY,0)
                ,TARGET.CAST_TRANS_DATE=SOURCE.CAST_TRANS_DATE
                ,TARGET.CAST_PROCESS_DATE=SOURCE.CAST_PROCESS_DATE
                ,TARGET.CAST_CREATED=SOURCE.CAST_CREATED
                ,TARGET.CAST_LAST_UPD=SOURCE.CAST_LAST_UPD
                ,TARGET.CAST_LoadDateTime=SOURCE.CAST_LoadDateTime
                ,TARGET.Error=coalesce(SOURCE.Error,'NA')
                ,TARGET.TotalCO2=coalesce(SOURCE.TotalCO2,'NA')
                ,TARGET.DescriptionComment = coalesce(SOURCE.DescriptionComment,'NA')
                ,TARGET.Channel=coalesce(SOURCE.Channel,'NA')
                ,TARGET.BATCH_NUM=coalesce(SOURCE.BATCH_NUM,'NA') --09/08/2022
                ,TARGET.TRACE_NUM=coalesce(SOURCE.TRACE_NUM,'NA') --09/08/2022
                ,TARGET.SYS_TRACE=coalesce(SOURCE.SYS_TRACE,'NA') --09/08/2022
                ,TARGET.CHANNEL_TYPE=coalesce(SOURCE.CHANNEL_TYPE,'NA') --09/08/2022
            --When no records are matched, insert ไม่มีได้
            --the incoming records from source
            --table to target table
            WHEN NOT MATCHED BY TARGET THEN 
            INSERT (
                TransDateKey
                ,TransTimeKey
                ,ProcessDateKey
                ,ProcessTimeKey
                ,TXN_ID
                ,CANCELLED_TXN_ID
                ,TRANS_NUM
                ,MEMBER_ID
                ,MEM_NUM
                ,CARD_NUM
                ,MEMBER_NAME
                ,TRANS_TYPE
                ,TRANS_SUB_TYPE
                ,TRANS_STATUS
                ,PRODUCT_ID
                ,PROD_CODE
                ,PROD_CATE
                ,PRODUCT_NAME
                ,PRODUCT_TYPE
                ,AMOUNT
                ,POINT_BEFORE_AWARD
                ,AWARD_POINT
                ,REDEEM_POINT
                ,REMAINING_POINT
                ,MERCHANT_ID
                ,MERCHANT_ROW_ID
                ,MERCHANT_NAME
                ,MERCHANT_NAME_THAI
                ,MERCHANT_BU
                ,TERMINAL_ID
                ,SETTLE_FLG
                ,QUALIFY_POINT
                ,DONATION_TYPE
                ,AMOUNT_IN_CAP
                ,BCP_CAP_DAY
                ,BCP_CAP_TIME
                ,MEMBER_CAP_MONTH_AVA
                ,CAP_MONTH_BALANCE
                ,EXPIRE_IN_DAYS
                ,ROW_ID
                ,PAR_ROW_ID
                ,MODIFICATION_NUM
                ,CreatedDateKey
                ,CreatedTimeKey
                ,LastUpdDateKey
                ,LastUpdTimeKey
                ,LoadDateKey
                ,LoadTimeKey
                ,QUANTITY
                ,CAST_TRANS_DATE
                ,CAST_PROCESS_DATE
                ,CAST_CREATED
                ,CAST_LAST_UPD
                ,CAST_LoadDateTime
                ,Error
                ,TotalCO2
                ,DescriptionComment
                ,Channel
                ,BATCH_NUM --09/08/2022
                ,TRACE_NUM --09/08/2022
                ,SYS_TRACE --09/08/2022
                ,CHANNEL_TYPE --09/08/2022
            )
            VALUES (
                SOURCE.TransDateKey
                ,SOURCE.TransTimeKey
                ,SOURCE.ProcessDateKey
                ,SOURCE.ProcessTimeKey
                ,SOURCE.TXN_ID
                ,SOURCE.CANCELLED_TXN_ID
                ,SOURCE.TRANS_NUM
                ,SOURCE.MEMBER_ID
                ,SOURCE.MEM_NUM
                ,SOURCE.CARD_NUM
                ,SOURCE.MEMBER_NAME
                ,SOURCE.TRANS_TYPE
                ,SOURCE.TRANS_SUB_TYPE
                ,SOURCE.TRANS_STATUS
                ,SOURCE.PRODUCT_ID
                ,SOURCE.PROD_CODE
                ,SOURCE.PROD_CATE
                ,SOURCE.PRODUCT_NAME
                ,SOURCE.PRODUCT_TYPE
                ,SOURCE.AMOUNT
                ,SOURCE.POINT_BEFORE_AWARD
                ,SOURCE.AWARD_POINT
                ,SOURCE.REDEEM_POINT
                ,SOURCE.REMAINING_POINT
                ,SOURCE.MERCHANT_ID
                ,SOURCE.MERCHANT_ROW_ID
                ,SOURCE.MERCHANT_NAME
                ,SOURCE.MERCHANT_NAME_THAI
                ,SOURCE.MERCHANT_BU
                ,SOURCE.TERMINAL_ID
                ,SOURCE.SETTLE_FLG
                ,SOURCE.QUALIFY_POINT
                ,SOURCE.DONATION_TYPE
                ,SOURCE.AMOUNT_IN_CAP
                ,SOURCE.BCP_CAP_DAY
                ,SOURCE.BCP_CAP_TIME
                ,SOURCE.MEMBER_CAP_MONTH_AVA
                ,SOURCE.CAP_MONTH_BALANCE
                ,SOURCE.EXPIRE_IN_DAYS
                ,SOURCE.ROW_ID
                ,SOURCE.PAR_ROW_ID
                ,SOURCE.MODIFICATION_NUM
                ,SOURCE.CreatedDateKey
                ,SOURCE.CreatedTimeKey
                ,SOURCE.LastUpdDateKey
                ,SOURCE.LastUpdTimeKey
                ,SOURCE.LoadDateKey
                ,SOURCE.LoadTimeKey
                ,SOURCE.QUANTITY
                ,SOURCE.CAST_TRANS_DATE
                ,SOURCE.CAST_PROCESS_DATE
                ,SOURCE.CAST_CREATED
                ,SOURCE.CAST_LAST_UPD
                ,SOURCE.CAST_LoadDateTime
                ,SOURCE.Error
                ,SOURCE.TotalCO2
                ,SOURCE.DescriptionComment
                ,SOURCE.Channel
                ,SOURCE.BATCH_NUM --09/08/2022
                ,SOURCE.TRACE_NUM --09/08/2022
                ,SOURCE.SYS_TRACE --09/08/2022
                ,SOURCE.CHANNEL_TYPE --09/08/2022
            );
        """).show()
        # utils.trackSizeOnLake("SilverLH.mismatchSiebelCardTransaction_V2")

        mismatch_df = spark.table("SilverLH.mismatchSiebelCardTransaction_V2")
        # utils.trackSizeTable(mismatch_df,detail='mismatch_df')

        fact_df = spark.table("SilverLH.factSiebelCardTransaction_V2")
        # utils.trackSizeTable(fact_df,detail='fact_df')

        filtered_df = mismatch_df.join(fact_df, mismatch_df.TXN_ID == fact_df.TxnId, "left_anti") #DELETE by left_anti
        # utils.trackSizeTable(filtered_df,detail='filtered_df')

        filtered_df.write.mode("overwrite").saveAsTable("SilverLH.mismatchSiebelCardTransaction_V2")

    def TmpCardRegister(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("TmpCardRegister")
        print("=======================================================")

        spark.sql(
        """
        MERGE INTO SilverLH.tmpCardRegister AS target
        USING (
            SELECT
                a.cardnum,
                b.txnid AS RegisterationTxnId,
                b.RegistrationProcessDateKey,
                b.RegistrationProcessTimeKey,
                b.MerchandiseKey,
                b.TerminalId
            FROM (
                SELECT DISTINCT cardnum
                FROM SilverLH.dimSiebelCardProfile
            ) AS a
            LEFT JOIN (
                SELECT
                    cardnum,
                    a.TxnId,
                    a.TransDatekey AS RegistrationProcessDateKey,
                    a.TransTimeKey AS RegistrationProcessTimeKey,
                    a.MerchandiseKey,
                    a.TerminalId,
                    ROW_NUMBER() OVER (PARTITION BY cardnum ORDER BY a.TransDatekey, a.TransTimeKey) AS occurence
                FROM SilverLH.factSiebelCardTransaction_V2 AS a
                INNER JOIN SilverLH.dimSiebelCardProfile AS b ON a.CardProfileKey = b.CardProfileKey
                WHERE ProductKey = 38 AND AwardPoint >= 0
            ) AS b ON a.cardnum = b.cardnum AND b.occurence = 1
        ) AS source
        ON source.cardnum = target.cardnum
        WHEN MATCHED AND target.RegisterationTxnId IS NULL THEN
            UPDATE SET
                target.RegisterationTxnId = source.RegisterationTxnId,
                target.RegistrationProcessDateKey = source.RegistrationProcessDateKey,
                target.RegistrationProcessTimeKey = source.RegistrationProcessTimeKey,
                target.LastUpdDateKey = date_format(current_timestamp() + INTERVAL 7 HOURS, 'yyyyMMdd'),
                target.LastUpdTimeKey = date_format(current_timestamp() + INTERVAL 7 HOURS, 'HHmmss'),
                target.RegisMerchandiseKey = source.MerchandiseKey,
                target.RegisTerminalID = source.TerminalId
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                cardnum,
                RegisterationTxnId,
                RegistrationProcessDateKey,
                RegistrationProcessTimeKey,
                LastUpdDateKey,
                LastUpdTimeKey,
                RegisMerchandiseKey,
                RegisTerminalID
            )
            VALUES (
                source.cardnum,
                source.RegisterationTxnId,
                source.RegistrationProcessDateKey,
                source.RegistrationProcessTimeKey,
                date_format(current_timestamp() + INTERVAL 7 HOURS, 'yyyyMMdd'),
                date_format(current_timestamp() + INTERVAL 7 HOURS, 'HHmmss'),
                source.MerchandiseKey,
                source.TerminalId
            );
        """
        ).show()


        spark.sql("""
        MERGE INTO SilverLH.tmpCardRegister AS target
        USING (
            SELECT *
            FROM (
                SELECT
                    a.cardnum,
                    b.TxnId,
                    b.firsttxndatetimekey,
                    b.TransDatekey,
                    b.TransTimeKey,
                    b.merchandisekey,
                    ROW_NUMBER() OVER (PARTITION BY a.CardNum ORDER BY b.firsttxndatetimekey) AS rownum
                FROM (
                    SELECT
                        cardnum,
                        CAST(RegistrationProcessDateKey AS STRING) || LPAD(CAST(RegistrationProcessTimeKey AS STRING), 6, '0') AS registerdatetimekey
                    FROM SilverLH.tmpCardRegister
                    WHERE RegistrationProcessDateKey IS NOT NULL
                ) a
                INNER JOIN (
                    SELECT
                        cardnum,
                        b.TxnId,
                        b.TransDatekey ,
                        b.TransTimeKey,
                        b.merchandisekey,
                        CAST(TransDatekey  AS STRING) || LPAD(CAST(b.TransTimeKey AS STRING), 6, '0') AS firsttxndatetimekey
                    FROM SilverLH.dimSiebelCardProfile a
                    INNER JOIN SilverLH.factSiebelCardTransaction_V2 b ON a.CardProfileKey = b.CardProfileKey
                    WHERE TransType = 'ACCRUAL'
                    AND b.TransSubType = 'Product'
                    AND TransStatus = 'Processed'
                    AND b.ProductKey IN (
                        SELECT ProductKey
                        FROM SilverLH.dimProduct
                        WHERE UsedSIEBEL = '1'
                            AND ProductGroup IN ('LUBE', 'MOGAS', 'HSD', 'NONOIL', 'Mart')
                            OR ProductKey IN ('41')
                    )
                ) b ON a.cardnum = b.CardNum AND a.registerdatetimekey < b.firsttxndatetimekey
            ) tmp
            WHERE rownum = 1
        ) source
        ON target.cardnum = source.Cardnum
        WHEN MATCHED AND target.FirstTransTxnId IS NULL
        THEN UPDATE SET
            target.FirstTransTxnId = source.txnid,
            target.FirstTransProcessDateKey = source.TransDatekey,
            target.FirstTransProcessTimeKey = source.TransTimeKey,
            target.MerchandiseKey = source.MerchandiseKey,
            target.LastUpdDateKey = DATE_FORMAT(current_date(), 'yyyyMMdd'),
            target.LastUpdTimeKey = DATE_FORMAT(current_timestamp(), 'HHmmss')
        """).show()

    def UpdateMemberIDCardRegister(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("UpdateMemberIDCardRegister")
        print("=======================================================")

        mismat = spark.table('SilverLH.mismatchsiebelcardtransaction_v2')

        # utils.trackSizeOnLake('SilverLH.tmpCardRegister')
        tmpCardRegister = spark.table('SilverLH.tmpCardRegister')

        utils.trackSizeTable(tmpCardRegister,detail='tmpCardRegister')

        currentDate = tmpCardRegister.filter(col('RegisterationTxnId').isNotNull() & col('memberid').isNull() & (col('LastUpdDateKey')==date_format(current_thai_time, "yyyyMMdd").cast("int")) )
        NotCurrentDate = tmpCardRegister.filter(~(col('RegisterationTxnId').isNotNull() & col('memberid').isNull() & (col('LastUpdDateKey')==date_format(current_thai_time, "yyyyMMdd").cast("int")) ))

        utils.trackSizeTable(currentDate,detail='current date')
        utils.trackSizeTable(NotCurrentDate,detail='not current date')

        dimSiebelCardProfileForLookUp = spark.sql("SELECT CardNum, MemberID FROM SilverLH.dimSiebelCardProfile WHERE EndDateKey IS NULL and MemberID <> '1-LK5M'")
        utils.trackSizeTable(dimSiebelCardProfileForLookUp,detail='dimSiebelCardProfile with select and filter')

        updated_tmpCardRegister  = currentDate.alias('left').join(dimSiebelCardProfileForLookUp.withColumnsRenamed({'CardNum':'CardNum_r','MemberID':'MemberID_r'}).alias('right'),
                                                                        on= col('left.cardnum')==col('right.CardNum_r'),
                                                                        how='left').select(
                                                                            'left.cardnum', 'left.RegisterationTxnId', 'left.RegistrationProcessDateKey', 'left.RegistrationProcessTimeKey',
                                                                            'left.FirstTransTxnId', 'left.FirstTransProcessDateKey', 'left.FirstTransProcessTimeKey', 'left.MerchandiseKey',
                                                                            'left.LastUpdDateKey', 'left.LastUpdTimeKey',
                                                                            coalesce('left.MemberID', 'right.MemberID_r').alias('MemberID'),'left.RegisMerchandiseKey','left.RegisTerminalId')

        utils.trackSizeTable(updated_tmpCardRegister,detail='updated_tmpCardRegister')

        updated_tmpCardRegister = updated_tmpCardRegister.unionByName(NotCurrentDate)
        utils.trackSizeTable(updated_tmpCardRegister,detail='after union back')

        updated_tmpCardRegister.write.mode('overwrite').saveAsTable('SilverLH.tmpCardRegister')

    def DimSiebelMerchandiseTID(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("DimSiebelMerchandiseTID")
        print("=======================================================")

        dimSiebelMerchandiseTID = spark.table('SilverLH.dimSiebelMerchandiseTID')
        dimSiebelMerchandiseTID = dimSiebelMerchandiseTID.withColumns({'TID':upper(col('TID')),'MID':upper(col('MID'))}).withColumn('loadDateTime', regexp_replace('loadDateTime','T',' ')) # only initial
        stagingSiebelMerchandiseTID = spark.table('BronzeLH_Siebel.stagingSiebelMerchandiseTID')
        stagingSiebelMerchandiseTID = stagingSiebelMerchandiseTID.withColumns({'TID':upper(col('TID')),'MID':upper(col('MID'))}).withColumn('LoadDate',col('loadDateTime')).withColumn('loadDateTime', col('loadDateTime').cast(StringType()))

        source = stagingSiebelMerchandiseTID
        old_columns = source.columns
        reverseMapper = {column+'_source':column for column in old_columns}
        columnMapper = {column:column+'_source' for column in old_columns}
        source = source.withColumnsRenamed(columnMapper)
        source = source.fillna({'TID_source':'NA','MID_source':'NA', 'TID_Status_source':'NA'})

        target = dimSiebelMerchandiseTID

        target = target.fillna({'TID':'NA','MID':'NA', 'TID_Status':'NA'})
        join_key = (target.TID == source.TID_source)

        left_anti = target.join(source, on = join_key, how='left_anti').withColumn('action',lit('SAME IN TARGET'))
        inner = target.join(source, on = join_key, how='inner')
        insert = source.join(target, on = join_key, how='left_anti').withColumnsRenamed(reverseMapper).withColumn('action',lit('INSERT'))
        insert = insert.withColumn('LoadDate',col('LoadDate') + expr("INTERVAL 7 HOURS")).withColumn('UpdateDate',col('UpdateDate') + expr("INTERVAL 7 HOURS"))

        inner_update = inner.filter((col('MID')!=col('MID_source'))|(col('TID_Status')!=col('TID_Status_source'))).select(*source.columns).withColumnsRenamed(reverseMapper).withColumn('action',lit('UPDATE'))
        inner_update = inner_update.withColumn('LoadDate',col('LoadDate') + expr("INTERVAL 7 HOURS")).withColumn('UpdateDate',col('UpdateDate') + expr("INTERVAL 7 HOURS"))
        inner_same = inner.filter(~((col('MID')!=col('MID_source'))|(col('TID_Status')!=col('TID_Status_source')))).select(*target.columns).withColumn('action',lit('SAME IN MATCHED'))

        result = left_anti.unionByName(inner_same,allowMissingColumns=True).unionByName(inner_update,allowMissingColumns=True).unionByName(insert,allowMissingColumns=True)

        result = result.withColumns({
            'MID': when(col('MID')=='NA', lit(None)).otherwise(col('MID')),
            'TID': when(col('TID')=='NA', lit(None)).otherwise(col('TID')),
            'TID_Status': when(col('TID_Status')=='NA', lit(None)).otherwise(col('TID_Status'))
        })
        result.groupBy('action').count().show()
        result.drop('action').write.mode('overwrite').saveAsTable('SilverLH.dimSiebelMerchandiseTID')

    def UpdateRegisSS(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("UpdateRegisSS")
        print("=======================================================")

        tmpPerUpdateRegisSS = spark.sql(
        """
        with T1 as (
        select  a.cardnum AS CardNum,
                coalesce(a.RegisMerchandiseKey,0) as RegisMerchandiseKey ,
                a.RegistrationProcessDateKey--,b.CardStatus,b.JoinDateKey
                ,b.MemberID
        from SilverLH.tmpCardRegister a
        inner join SilverLH.dimSiebelCardProfile b on b.EndDateKey is null and b.MemberID <> '1-LK5M' --and b.RegisSS is null
        and a.CardNum = b.CardNum

        where a.LastUpdDateKey = DATE_FORMAT(current_date(), 'yyyyMMdd') and b.CardStatus <> 'New'
        --and a.cardnum = '7896610000063956'
        )
        , t2 as (
        SELECT
            CASE
            WHEN Customercode = '1814100' THEN 'BSRC'
            WHEN substr(trim(MidNameEN), 1, 4) = 'BSRC' THEN 'BSRC'
            ELSE 'BCP'
            END AS RegisSS,
            MerchandiseKey
        FROM
            SilverLH.dimSiebelMerchandise
        )

        select *, ROW_NUMBER() OVER(ORDER BY MemberID ASC) AS RowNum from T1

        left join T2 on T1.RegisMerchandiseKey = T2.MerchandiseKey
        """
        ).withColumn('RegisSS',col('RegisSS').cast(StringType()))\
        .withColumnRenamed('RegistrationProcessDateKey','RegisDateKey')\
        .select('CardNum','RegisMerchandiseKey','MemberID','RegisSS','RowNum','RegisDateKey')
        tmpPerUpdateRegisSS.createOrReplaceTempView('tmpPerUpdateRegisSS')


        df = spark.sql(
        """
        select count(DISTINCT(RegisSS))as CountRegisSS,MemberID 

        from SilverLH.dimSiebelCardProfile
        where  MemberID in (select MemberID from tmpPerUpdateRegisSS)
        and CardStatus <> 'New' 

        group by MemberID
        """
        )

        RegisSS0 = df.filter(col('CountRegisSS')==0)
        RegisSS1 = df.filter(col('CountRegisSS')==1)

        defaultOutput = df.filter((col('CountRegisSS')!=0) & (col('CountRegisSS')!=1))\
                        .withColumn('RegisSS',lit('ERR'))

        dimCardProfileWithRegisSS = spark.sql(
        """
        select MemberID , RegisSS 
        from SilverLH.dimSiebelCardProfile
        where  RegisSS is not null and MemberID in(select Distinct(MemberID)  from tmpPerUpdateRegisSS)
        group by MemberID , RegisSS
        """).sort('MemberID',ascending=True)

        utils.trackSizeTable(dimCardProfileWithRegisSS,detail='dimCardProfileWithRegisSS')

        mergeJoin1 = RegisSS1.join(dimCardProfileWithRegisSS,on = 'MemberID',how='left')

        tmpPerRegisSS = spark.sql(
        """
        WITH MinRowNums AS (
            SELECT DISTINCT
                MIN(RowNum) OVER (PARTITION BY MemberID) AS DISRow,
                MemberID
            FROM tmpPerUpdateRegisSS
            GROUP BY
                CardNum,
                RegisMerchandiseKey,
                MemberID,
                RegisSS,
                RegisDateKey,
                RowNum
        )
        SELECT
            t.MemberID,
            t.RegisSS
        FROM
            tmpPerUpdateRegisSS t
        JOIN
            MinRowNums m
        ON
            t.RowNum = m.DISRow
        -- Uncomment the following line if you want to filter by a specific MemberID
        -- AND t.MemberID = '1-FN43A-14'
        GROUP BY
            t.MemberID,
            t.RegisSS
        """
        ).sort('MemberID',ascending=True)


        mergeJoin2 = RegisSS0.join(tmpPerRegisSS,on = 'MemberID',how='left')

        tmpPerUpdateRegisSS2 = defaultOutput.unionByName(mergeJoin1).unionByName(mergeJoin2)
        tmpPerUpdateRegisSS2.createOrReplaceTempView('tmpPerUpdateRegisSS2')

        # tmpPerUpdateRegisSS2.write.mode('overwrite').saveAsTable('SilverLH.tmpPerUpdateRegisSS2')
        # utils.trackSize(spark.table('SilverLH.tmpPerUpdateRegisSS2'),tableName='tmpPerUpdateRegisSS2 in destination')


        '''
        update RegisSS in dimSiebelCardProfile for the existing MemberID
        '''

        spark.sql(
        """
        MERGE INTO SilverLH.dimSiebelCardProfile AS TARGET
        USING tmpPerUpdateRegisSS2 AS SOURCE 
        ON trim(TARGET.MemberID) = trim(SOURCE.MemberID)-- and trim(TARGET.CardNum) = trim(SOURCE.CardNum))
        --When records are matched, update 
        --the records if there is any change
        WHEN MATCHED
        THEN 
        UPDATE SET 
        TARGET.RegisSS = SOURCE.RegisSS;
        """
        ).show()

        tmpPerUpdateRegisSS3 = spark.sql(
        """
        WITH T1 AS (
        SELECT 
            r.CardNum,
            COALESCE(r.RegisMerchandiseKey, 0) AS RegisMerchandiseKey,
            r.RegistrationProcessDateKey,
            f.MemberID
        FROM SilverLH.dimSiebelCardProfile f
        INNER JOIN SilverLH.tmpCardRegister r ON f.CardNum = r.cardnum
        WHERE RegisSS IS NULL
            AND JoinDateKey IS NOT NULL
            AND EndDateKey IS NULL
            AND CardStatus != 'New'
            AND f.memberid != '1-LK5M'
        ),
        T2 AS (
        SELECT 
            CASE 
            WHEN Customercode = '1814100' THEN 'ETL'
            WHEN SUBSTRING(TRIM(MidNameEN), 1, 4) = 'ETL' THEN 'ETL'
            ELSE 'BCP'
            END AS RegisSS,
            MerchandiseKey
        FROM SilverLH.dimSiebelMerchandise 
        )
        SELECT *, ROW_NUMBER() OVER(ORDER BY MemberID ASC) AS RowNum 
        FROM T1
        LEFT JOIN T2 ON T1.RegisMerchandiseKey = T2.MerchandiseKey
        """
        ).withColumn('RegisSS',col('RegisSS').cast(StringType()))\
        .withColumnRenamed('RegistrationProcessDateKey','RegisDateKey')\
        .select('CardNum','RegisMerchandiseKey','MemberID','RegisSS','RowNum','RegisDateKey')

        tmpPerUpdateRegisSS3.createOrReplaceTempView('tmpPerUpdateRegisSS3')

        df = spark.sql(
        """
        select count(DISTINCT(RegisSS))as CountRegisSS,MemberID 

        from SilverLH.dimSiebelCardProfile
        where  MemberID in (select MemberID from tmpPerUpdateRegisSS3)
        and CardStatus <> 'New' 

        group by MemberID
        """
        )

        RegisSS0 = df.filter(col('CountRegisSS')==0)
        RegisSS1 = df.filter(col('CountRegisSS')==1)

        defaultOutput = df.filter((col('CountRegisSS')!=0) & (col('CountRegisSS')!=1))\
                        .withColumn('RegisSS',lit('ERR'))

        dimCardProfileWithRegisSS = spark.sql(
        """
        select MemberID , RegisSS 
        from SilverLH.dimSiebelCardProfile
        where  RegisSS is not null and MemberID in(select Distinct(MemberID)  from tmpPerUpdateRegisSS3)
        group by MemberID , RegisSS
        """).sort('MemberID',ascending=True)

        mergeJoin1 = RegisSS1.join(dimCardProfileWithRegisSS,on = 'MemberID',how='left')

        tmpPerRegisSS = spark.sql(
        """
        WITH MinRowNums AS (
            SELECT DISTINCT
                MIN(RowNum) OVER (PARTITION BY MemberID) AS DISRow,
                MemberID
            FROM tmpPerUpdateRegisSS3
            GROUP BY
                CardNum,
                RegisMerchandiseKey,
                MemberID,
                RegisSS,
                RegisDateKey,
                RowNum
        )
        SELECT
            t.MemberID,
            t.RegisSS
        FROM
            tmpPerUpdateRegisSS3 t
        JOIN
            MinRowNums m
        ON
            t.RowNum = m.DISRow
        -- Uncomment the following line if you want to filter by a specific MemberID
        -- AND t.MemberID = '1-FN43A-14'
        GROUP BY
            t.MemberID,
            t.RegisSS
        """
        ).sort('MemberID',ascending=True)

        mergeJoin2 = RegisSS0.join(tmpPerRegisSS,on = 'MemberID',how='left')

        tmpPerUpdateRegisSS4 = defaultOutput.unionByName(mergeJoin1).unionByName(mergeJoin2)
        tmpPerUpdateRegisSS4.createOrReplaceTempView('tmpPerUpdateRegisSS4')

        '''
        update RegisSS in dimSiebelCardProfile for the existing MemberID
        '''

        spark.sql(
        """
        MERGE INTO SilverLH.dimSiebelCardProfile AS TARGET
        USING tmpPerUpdateRegisSS4 AS SOURCE 
        ON trim(TARGET.MemberID) = trim(SOURCE.MemberID)-- and trim(TARGET.CardNum) = trim(SOURCE.CardNum))
        --When records are matched, update 
        --the records if there is any change
        WHEN MATCHED
        THEN 
        UPDATE SET 
        TARGET.RegisSS = SOURCE.RegisSS;
        """
        ).show()

    def UpdateSiebelPTS_SMILES(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("UpdateSiebelPTS_SMILES")
        print("=======================================================")

        stagingSiebelPTS_SMILES = spark.sql(
        """
        select
            a.TransDateKey
            ,a.CardProfileKey
            ,b.CardNum
            ,a.MemberID
            ,'Y' as TRF_ETL
        from   
            SilverLH.factSiebelCardTransaction_V2  a
            ,SilverLH.dimSiebelCardProfile b
            ,SilverLH.dimProduct c
        where 
        a.ProductKey = 796
        and c.UsedSIEBEL = 1
        and a.TransType = 'ACCRUAL'
        and a.TransSubType = 'Product'
        and a.TransStatus = 'Processed'
        and a.CancelledTxnId = 'NA'
        and a.CardProfileKey = b.CardProfileKey
        and a.ProductKey = c.ProductKey
        """
        ).withColumn('LoadDate',lit(current_timestamp()))\
        .withColumn('TRF_ETL',col('TRF_ETL').cast(StringType()))

        stagingSiebelPTS_SMILES.createOrReplaceTempView('stagingSiebelPTS_SMILES')

        spark.sql(
        """
        MERGE INTO SilverLH.dimSiebelCardProfile AS TARGET
        USING (
        SELECT DISTINCT(MemberID),TRF_ETL
        FROM stagingSiebelPTS_SMILES
        ) AS SOURCE 
        ON trim(TARGET.MemberID) = trim(SOURCE.MemberID)
        WHEN MATCHED
        THEN 
        UPDATE SET 
        TARGET.TRF_ETL= SOURCE.TRF_ETL;
        """
        ).show()

    def factSiebelTxnSplit(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("factSiebelTxnSplit")
        print("=======================================================")

        current_thai_time = expr("current_timestamp() + INTERVAL 7 HOURS")

        source = f'SilverLH.factsiebelcardtransaction_v2'
        
        table = spark.table('SilverLH.factsiebelcardtransaction_v2')
        tableYesterday = table.filter(col('TransDateKey') < date_format(current_thai_time,format = 'yyyyMMdd').cast(IntegerType()))
        tableToday = table.filter(col('TransDateKey') >= date_format(current_thai_time,format = 'yyyyMMdd').cast(IntegerType()))

        tableToday.write.mode('overwrite').partitionBy(['TransYearKey','TransMonthKey','TransType','TransStatus']).saveAsTable('SilverLH.factsiebelcardtransaction_v2_today')
        tableYesterday.write\
            .partitionBy("TransDateKey")\
            .mode('overwrite').partitionBy(['TransYearKey','TransMonthKey','TransType','TransStatus']).saveAsTable('SilverLH.factSiebelCardTransaction_V2')

    def factSiebelAccrualItem(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("factSiebelAccrualItem")
        print("=======================================================")

        factSiebelItemAccrual  = spark.sql("SELECT * FROM SilverLH.factsiebelitem_accrual").drop_duplicates()
        stagingSiebelItemAccrual = utils.fillNaAll(spark.sql("SELECT * FROM BronzeLH_Siebel.stagingSiebelAccrualItem"))
        stagingSiebelItemAccrual = stagingSiebelItemAccrual.withColumnsRenamed({'EXPIRATION_DT':'Expiration_Date'})
        stagingSiebelItemAccrual = stagingSiebelItemAccrual.select(*factSiebelItemAccrual.columns)
        stagingSiebelItemAccrual = utils.copySchemaByName(stagingSiebelItemAccrual,factSiebelItemAccrual)

        source = stagingSiebelItemAccrual
        target = factSiebelItemAccrual
        source = source.withColumn('PointItem_RowID', trim(source.PointItem_RowID))
        target = target.withColumn('PointItem_RowID', trim(target.PointItem_RowID))

        old_columns = source.columns
        mapper = {column + "_source":column for column in old_columns}
        new_columns = [column + "_source" for column in old_columns]      # columns of stagingbcpss with '_source'
        source = source.toDF(*new_columns)

        old_records = target.join(source, on=target.PointItem_RowID == source.PointItem_RowID_source, how='left_anti').select(*old_columns).withColumn('action', lit('SAME'))
        intersection = target.join(source, on=target.PointItem_RowID == source.PointItem_RowID_source, how='inner').select(*new_columns).withColumnsRenamed(mapper).withColumn('action', lit('UPDATE'))
        newcoming = source.join(target, on=target.PointItem_RowID == source.PointItem_RowID_source, how='left_anti').select(*new_columns).withColumnsRenamed(mapper).withColumn('action', lit('INSERT'))

        newfactSiebelItemAccrual = intersection.unionByName(old_records).unionByName(newcoming).withColumnsRenamed(mapper)

        df = newfactSiebelItemAccrual.drop('action')
        df.write.mode('overwrite').saveAsTable('SilverLH.factsiebelitem_accrual')

    def factSiebelItem_Redemption(self,WS_ID=None, BronzeLH_ID=None, SilverLH_ID=None):
        print("=======================================================")
        print("factSiebelItem_Redemption")
        print("=======================================================")

        factSiebelItemRedemption  = spark.sql("SELECT * FROM SilverLH.factsiebelitem_redemption")
        stagingSiebelItemRedemption = utils.fillNaAll(spark.sql("SELECT * FROM BronzeLH_Siebel.stagingSiebelItem_Redemption"))
        stagingSiebelItemRedemption = stagingSiebelItemRedemption.withColumnRenamed('TXN_DT','TRANSACTION_DATE')\
                                                                .withColumn("YearKey", lpad(date_format(col("TRANSACTION_DATE"), "yyyy"), 4, "0"))

        stagingSiebelItemRedemption = stagingSiebelItemRedemption.select(*factSiebelItemRedemption.columns)
        stagingSiebelItemRedemption = utils.copySchemaByName(stagingSiebelItemRedemption,factSiebelItemRedemption)

        source = stagingSiebelItemRedemption
        target = factSiebelItemRedemption

        old_columns = source.columns
        mapper = {column + "_source":column for column in old_columns}
        new_columns = [column + "_source" for column in old_columns]      # columns of stagingbcpss with '_source'
        source = source.toDF(*new_columns)

        old_records = target.join(source, on=target.REDEEM_ROW_ID == source.REDEEM_ROW_ID_source, how='left_anti').select(*old_columns)
        intersection = target.join(source, on=target.REDEEM_ROW_ID == source.REDEEM_ROW_ID_source, how='inner').select(*new_columns).withColumnsRenamed(mapper)
        newcoming = source.join(target, on=target.REDEEM_ROW_ID == source.REDEEM_ROW_ID_source, how='left_anti').select(*new_columns).withColumnsRenamed(mapper)

        newUpsert = intersection.unionByName(old_records)
        newUpsert = newUpsert.unionByName(newcoming)
        newUpsert = newUpsert.withColumnsRenamed(mapper)

        newUpsert.write.mode('overwrite').saveAsTable('SilverLH.factsiebelitem_redemption')

    def run_siebel(self):
        self.DimSiebelMerchandise(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.DimSiebelProduct(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.StagingCardProfile2(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.DimSiebelCardProfile(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.factCardTransaction_Part1(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.factCardTransaction_Part2(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.factCardTransaction_Part3(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.TmpCardRegister(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.UpdateMemberIDCardRegister(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.DimSiebelMerchandiseTID(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.UpdateRegisSS(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.UpdateSiebelPTS_SMILES(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.factSiebelTxnSplit(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.factSiebelAccrualItem(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
        self.factSiebelItem_Redemption(self.WS_ID, self.BronzeLH_ID, self.SilverLH_ID)
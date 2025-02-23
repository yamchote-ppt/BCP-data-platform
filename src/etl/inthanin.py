# Welcome to your new notebook
# Type here in the cell editor to add code!
import numpy as np
from pyspark.sql import Row
from pyspark.sql.functions import col, when, concat, lit, format_string, upper, substring, expr, current_date, current_timestamp,to_timestamp,concat_ws, isnull, date_format, asc, trim, trunc, date_sub, year,coalesce, row_number, sum, lpad, expr, add_months
from pyspark.sql.types import IntegerType, DecimalType, StringType, LongType, TimestampType, StructType, StructField, DoubleType
import pandas as pd
import re
import os
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from datetime import datetime
import env.utils as utils
from pyspark.sql.window import Window
import shutil

spark = SparkSession.builder.getOrCreate()

def update_table_inthanin(tableName,WS_ID,LH_staging_ID,SilverLH_ID):
    # TODO: partition transaction data by SaleDate 
    target = spark.read.load(f'abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{SilverLH_ID}/Tables/{tableName}')

    target_active = target
    
    source = spark.read.load(f'abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{LH_staging_ID}/Tables/staging_{tableName}')
    # saledate_list = source.select('SaleDate').distinct()
    target_same = target_active.join(source, on='SaleDate', how='left_anti') # a table with the saledate not in saledate_list
    
    target = target_same.unionByName(source)
    target.write.mode('overwrite').save(f'abfss://{WS_ID}@onelake.dfs.fabric.microsoft.com/{SilverLH_ID}/Tables/{tableName}')
    # target.write.mode('overwrite').saveAsTable(f'SilverLH.{tableName}')









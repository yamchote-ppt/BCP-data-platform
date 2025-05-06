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

from typing import Callable, List, Dict, Any

spark = SparkSession.builder.config("spark.sql.extensions", "delta.sql.DeltaSparkSessionExtensions").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").getOrCreate()

current_date_list = [int(x) for x in (datetime.now() + timedelta(hours=7)).strftime('%Y%m%d %H%M%S').split(' ')]

class tableManage:
    def __init__(self, WS_ID):
        self.WS_ID = WS_ID
        self.MAPPER = {
            'stringtype': StringType,
            'integertype': IntegerType,
            'longtype': LongType,
            'floattype': FloatType,
            'doubletype': DoubleType,
            'booleantype': BooleanType,
            'Datetype': DateType,
            'timestamptype': TimestampType,
            'decimaltype': DecimalType,
            'binarytype': BinaryType,
            'str': StringType,
            'int': IntegerType,
            'long': LongType,
            'float': DecimalType,
            'double': DecimalType,
            'boolean': BooleanType,
            'decimal': DecimalType,
            'short': ShortType,
        }

    def create_table(self, table_name: str, lh_name: str, column_datatype_map: dict[str, DataType], precision_scale_map: dict[str, tuple], table_partition_columns: List[str] =None):
        '''
        column_datatype_map should be a dict with column name as key and datatype function from `pyspark.sql.types` as value.
        precision_scale_map should be a dict with column name as key and a tuple of (precision, scale) as value of decimal type; in case of other type it can be `(None, None)` or omitted.
        '''
        LH_ID = utils.get_lh_id(self.WS_ID, lh_name)
        table_path = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Tables/{table_name}'

        # Create schema for the table
        schema_fields = []
        for column, datatype in column_datatype_map.items():
            precision, scale = precision_scale_map.get(column, (None, None))
            if datatype == DecimalType: 
                if precision is None:
                    precision = 38
                if scale is None:
                    scale = 18
                schema_fields.append(StructField(column, datatype(precision, scale), True))
            else:
                schema_fields.append(StructField(column, datatype(), True))
        schema = StructType(schema_fields)

        empty_df = spark.createDataFrame([], schema)

        writer = empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if table_partition_columns:
            writer = writer.partitionBy(*table_partition_columns)
        writer.save(table_path)

    def edit_table(self, table_name: str, lh_name: str, column_datatype_map: dict[str, DataType], precision_scale_map: dict[str, tuple], table_partition_columns: List[str] =None):
        LH_ID = utils.get_lh_id(self.WS_ID, lh_name)
        table_path = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{LH_ID}/Tables/{table_name}'
        old_table = spark.read.format("delta").load(table_path)

        # Create schema for the table
        for column, datatype in column_datatype_map.items():
            precision, scale = precision_scale_map.get(column, (None, None))
            if datatype == DecimalType: 
                old_table = old_table.withColumn(column, old_table[column].cast(datatype(precision, scale)))
            else:
                old_table = old_table.withColumn(column, old_table[column].cast(datatype()))
        
        # Save the modified DataFrame back to the same path
        writer = old_table.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if table_partition_columns:
            writer = writer.partitionBy(*table_partition_columns)
        writer.save(table_path)



class CreateBlankTable(tableManage):
    def __init__(self, WS_ID, META_LH_ID, META_FILENAME, format="delta"):
        '''
        METATABLE must have the following columns:
        - TableName: Name of the table to be created
        - LakehouseName: Name of the lakehouse to save the table
        - ColumnName: Name of the column
        - DataType: Data type of the column (e.g., StringType, IntegerType, etc.)
        - Precision: Precision for DecimalType (if applicable)
        - Scale: Scale for DecimalType (if applicable)
        - forPartition: 1 if the column is a partition column, 0 otherwise
        '''
        super().__init__(WS_ID)
        self.META_LH_ID = META_LH_ID
        self.META_FILENAME = META_FILENAME
        assert format in ["delta", "csv"], "format should be either 'delta' or 'csv'"
        self.format = format
        if self.format == 'csv':
            self.META_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.META_LH_ID}/Files/{self.META_FILENAME}'
        elif self.format == 'delta':
            self.META_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.META_LH_ID}/Tables/{self.META_FILENAME}'

    def readMetaFile(self):
        if  self.format == 'csv':
            meta_df = spark.read.option("header", "true").option("delimiter", ",").csv(self.META_PATH).toPandas().set_index(['TableName','LakehouseName'], drop=False)
            return meta_df
        elif self.format == 'delta':
            meta_df = spark.read.format("delta").load(self.META_PATH).toPandas().set_index(['TableName','LakehouseName'], drop=False)
            return meta_df

    def create_all(self, meta_table: pd.DataFrame):
        # raise NotImplementedError("create_all method should be implemented in the subclass.")
        tableNames = meta_table.index.get_level_values(0).unique()
        for table in tableNames:
            table_meta = meta_table.loc[table]
            lakehouse_name = table_meta.index.unique()[0]
            column_datatype_map = {}
            precision_scale_map = {}
            table_partition_columns = []
            for index, row in table_meta.iterrows():
                column_name = row['ColumnName']
                datatype_str = row['DataType']
                datatype = self.MAPPER[datatype_str]

                column_datatype_map[column_name] = datatype
                if datatype == DecimalType:
                    precision_scale_map[column_name] = (row['Precision'], row['Scale'])
                if row['forPartition'] == 1:
                    table_partition_columns.append(column_name)
            self.create_table(table, lakehouse_name, column_datatype_map, precision_scale_map, table_partition_columns)

    def run(self):
        self.meta_table = self.readMetaFile()
        self.create_all(self.meta_table)

# class editTable(tableManage):
#     def __init__(self, WS_ID, META_LH_ID, META_FILENAME, format="delta"):
#         '''
#         METATABLE must have the following columns:
#         - TableName: Name of the table to be created
#         - LakehouseName: Name of the lakehouse to save the table
#         - ColumnName: Name of the column
#         - DataType: Data type of the column (e.g., StringType, IntegerType, etc.)
#         - Precision: Precision for DecimalType (if applicable)
#         - Scale: Scale for DecimalType (if applicable)
#         - forPartition: 1 if the column is a partition column, 0 otherwise
#         '''
#         super().__init__(WS_ID)
#         self.META_LH_ID = META_LH_ID
#         self.META_FILENAME = META_FILENAME
#         assert format in ["delta", "csv"], "format should be either 'delta' or 'csv'"
#         self.format = format
#         if self.format == 'csv':
#             self.META_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.META_LH_ID}/Files/{self.META_FILENAME}'
#         elif self.format == 'delta':
#             self.META_PATH = f'abfss://{self.WS_ID}@onelake.dfs.fabric.microsoft.com/{self.META_LH_ID}/Tables/{self.META_FILENAME}'

#     def readMetaFile(self):
#         if  self.format == 'csv':
#             meta_df = spark.read.option("header", "true").option("delimiter", ",").csv(self.META_PATH).toPandas().set_index(['TableName','LakehouseName'], drop=False)
#             return meta_df
#         elif self.format == 'delta':
#             meta_df = spark.read.format("delta").load(self.META_PATH).toPandas().set_index(['TableName','LakehouseName'], drop=False)
#             return meta_df

#     def create_all(self, meta_table: pd.DataFrame):
#         # raise NotImplementedError("create_all method should be implemented in the subclass.")
#         tableNames = meta_table.index.get_level_values(0).unique()
#         for table in tableNames:
#             table_meta = meta_table.loc[table]
#             lakehouse_name = table_meta.index.unique()[0]
#             column_datatype_map = {}
#             precision_scale_map = {}
#             table_partition_columns = []
#             for index, row in table_meta.iterrows():
#                 column_name = row['ColumnName']
#                 datatype_str = row['DataType']
#                 datatype = self.MAPPER[datatype_str]

#                 column_datatype_map[column_name] = datatype
#                 if datatype == DecimalType:
#                     precision_scale_map[column_name] = (row['Precision'], row['Scale'])
#                 if row['forPartition'] == 1:
#                     table_partition_columns.append(column_name)
#             self.create_table(table, lakehouse_name, column_datatype_map, precision_scale_map, table_partition_columns)

#     def run(self):
#         self.meta_table = self.readMetaFile()
#         self.create_all(self.meta_table)
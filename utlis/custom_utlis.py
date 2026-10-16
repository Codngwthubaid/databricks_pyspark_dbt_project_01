from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import DeltaTable

"""
EXPLAIN FUNCTION -: dedup
dedup function takes self, df, dedup_cols, cdc
- self is default
- df is out actual dataframe
- dedup_cols is a list of columns to dedup on
- cdc is the 'last_updated_timestamp' col for orderBy

step 1: dedupKey col creates a hash col based on all the provided cols and with the help of '*' we unpack all the cols from the list 
step 2: dedupCount col creates a row_number() based on the dedupKey and orderBy the cdc column in desc for getting the latest record
step 3: filter the df based on the dedupCount == 1
step 4: drop the dedupKey and dedupCount cols
step 5: return the df


EXPLAIN FUNCTION -: processing_timestamp
processing_timestamp function takes self, df
- self is default
- df is out actual dataframe

step 1: add a new column 'process_timestamp' with the current timestamp
step 2: return the df


EXPLAIN FUNCTION -: upsert
upsert function takes self, df, key_cols
- self is default
- df is out actual dataframe
- key_cols is a list of columns to merge on

step 1: create a merge condition by list comprehension
step 2: create a delta object using the DeltaTable.forName() method
step 3: merge the df with the delta object using the merge() method with a condition when the cdc is greater than or equal to the target table (only for whenMatchedUpdateAll)
step 4: execute the merge
"""

class transformations:
    
    def dedup(self, df: DataFrame, dedup_cols: List, cdc):

        df = df.withColumn('dedupKey', concat(*dedup_cols))
        df = df.withColumn('dedupCount', row_number().over(Window.partitionBy('dedupKey').orderBy(desc(cdc))))
        df = df.filter(df.dedupCount == 1)
        df = df.drop('dedupKey', 'dedupCount')
        return df


    def processing_timestamp(self, df):
        
        df = df.withColumn('process_timestamp', current_timestamp())        
        return df
    
    def upsert(self, df, key_cols, table, cdc):
        # merge condition will create by list comprehension 
        merge_condition = " AND ".join([f"src.{i} = trg.{i}" for i in key_cols])
        delta_obj = DeltaTable.forName(spark, f'databricks_pyspark_dbt_project_01.silver_schema.{table}')
        delta_obj.alias('trg').merge(df.alias('src'), merge_condition)\
                                .whenMatchedUpdateAll(condition = f"src.{cdc} >= trg.{cdc}")\
                                .whenNotMatchedInsertAll()\
                                .execute()


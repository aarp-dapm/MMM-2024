# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DoubleType, FloatType, DateType, TimestampType
from pyspark.sql.types import *
from pyspark.sql.functions import date_format, col, desc, udf, from_unixtime, unix_timestamp, date_sub, date_add, last_day
from pyspark.sql.functions import round, sum, lit, add_months, coalesce, max, min, monotonically_increasing_id, approx_count_distinct
from pyspark.sql.window import Window

# from datetime import datetime
import datetime as dt
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import time

# COMMAND ----------

# MAGIC %md
# MAGIC # Update 1/30/2024: Appending FY 23 Data

# COMMAND ----------

###################### 2022 ############################
### Linear: 'DRTV_Monitored_Spend', 'Linear_Monitored_Impressions', 'Brand_Spend', 'DRTV_Unmonitored_Spend'
### CTV: 'Roku_CTV_Spend', 'Roku_CTV_Impressions', 'Samsung_CTV_Spend', 'Samsung_CTV_Impressions'
df_2022 = spark.sql("select * from default.drtv_data_2022")
cols = ['Mail_Date', 'DRTV_Monitored_Spend', 'Linear_Monitored_Impressions', 'Brand_Spend', 'DRTV_Unmonitored_Spend', 'Roku_CTV_Spend', 'Roku_CTV_Impressions', 'Samsung_CTV_Spend', 'Samsung_CTV_Impressions']
df_2022 = df_2022.withColumn('Mail_Date', f.to_date(f.col('Date'), "M/d/y")).drop(f.col('Date')).select(cols).dropDuplicates()
df_2022 = df_2022.filter(f.year(f.col('Mail_Date'))==2022)


## Adding Data for last two weeks of 2022 ##
new_row12_19 = spark.createDataFrame( [('2022-12-19', 142699,32833163,0,14648,5044,300527,4952,469278)], cols) 
new_row12_26 = spark.createDataFrame( [('2022-12-26', 102626,27384629,0,17135,5080,325300,5055,495935)], cols)

## Append New Rows ##
df_2022 = df_2022.union(new_row12_19).union(new_row12_26)
df_2022 = df_2022.withColumn('Mail_Date', f.to_date(f.col('Mail_Date')))

df_2022.display()

# COMMAND ----------

###################### 2021 ############################
### Linear: 'DRTV_Monitored_Spend', 'Linear_Monitored_Impressions', 'Brand_Spend', 'DRTV_Unmonitored_Spend'
### CTV: 'CTV_Spend', 'CTV_Impressions'
df_2021 = spark.sql("select * from default.drtv_data_2021")
df_2021 = df_2021.withColumn('Linear_Monitored_Impressions', f.col('Linear_Monitored_Impressions_')).drop(f.col('Linear_Monitored_Impressions_'))

### Creating Roku TV Spend n Impression
df_2021 = df_2021.withColumn('Roku_CTV_Spend', f.col('CTV_Spend')/2)
df_2021 = df_2021.withColumn('Roku_CTV_Impressions', f.col('CTV_Impressions')/2)
### Creating Samsung TV Spend n Impression
df_2021 = df_2021.withColumn('Samsung_CTV_Spend', f.col('CTV_Spend')/2)
df_2021 = df_2021.withColumn('Samsung_CTV_Impressions', f.col('CTV_Impressions')/2)

cols = ['Mail_Date', 'DRTV_Monitored_Spend', 'Linear_Monitored_Impressions', 'Brand_Spend', 'DRTV_Unmonitored_Spend', 'Roku_CTV_Spend', 'Roku_CTV_Impressions', 'Samsung_CTV_Spend', 'Samsung_CTV_Impressions']
df_2021 = df_2021.withColumn('Mail_Date', f.to_date(f.col('Date'), "M/d/y")).drop(f.col('Date')).select(cols).dropDuplicates()
df_2021.display()

# COMMAND ----------

###################### 2020 ############################
### Linear: 'DRTV_Monitored_Spend', 'Linear_Monitored_Impressions', 'Brand_Spend', 'DRTV_Unmonitored_Spend'
### CTV: 'Roku_CTV_Spend', 'Roku_CTV_Impressions', 'Samsung_CTV_Spend', 'Samsung_CTV_Impressions'
df_2020 = spark.sql("select * from default.data_2020")
cols = ['Mail_Date', 'DRTV Monitored Spend', 'Linear Monitored Impressions', 'Brand Spend', 'DRTV Unmonitored Spend', 'Roku CTV Spend', 'Roku CTV Impressions', 'Samsung CTV Spend', 'Samsung CTV Impressions']
df_2020 = df_2020.withColumn('Mail_Date', f.to_date(f.col('Week'), "M/d/y")).drop(f.col('Week')).select(cols).dropDuplicates()
temp_df_2020 = df_2020.filter(f.year(f.col('Mail_Date'))==2020)


### Replacing Spaces with underscore ###
df_2020 = temp_df_2020.select([f.col(col).alias(col.replace(' ', '_')) for col in temp_df_2020.columns])
df_2020.display()

# COMMAND ----------

## Union Dataframe ##
df = df_2022.union(df_2021.union(df_2020))
df.display()

# COMMAND ----------

## Data avaialable till 12/28/2022, need updated data ##
df.agg({"Mail_Date": "max"}).collect()[0], df.agg({"Mail_Date": "min"}).collect()[0]

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Append Data FY 2023

# COMMAND ----------

append_df_2023 = spark.sql("select * from default.drtv_fy_2023_upload__csv")
append_df_2023 = append_df_2023.withColumn('Mail_Date', f.to_date(f.col('Columns'), "M/d/y")).drop(f.col('Columns'))


map_dict = { 'DRTV_Monitored_Spend':'DRTV_Monitored_Est._Spend',
 'Linear_Monitored_Impressions':'Linear_DRTV_Monitored_Impressions_',
 'Brand_Spend':'Brand_Est._Spend',
 'DRTV_Unmonitored_Spend':'DRTV_Unmonitored_Est._Spend',
 'Roku_CTV_Spend':'Roku_CTV_Estimated_Spend',
 'Roku_CTV_Impressions':'Roku_CTV_Impressions',
 'Samsung_CTV_Spend':'Samsung_CTV_Estimated_Spend',
 'Samsung_CTV_Impressions':'Samsung_CTV_Impressions'}

for key,vals in map_dict.items():
  append_df_2023 = append_df_2023.withColumnRenamed(vals, key)


## Rename more cols ##
rename_more_cols = {
 'Roku_CTV_Est._CPIA_-_Direct_Mail_Campaign':'Roku_CTV_Est_CPIA_Direct_Mail_Campaign',
 'Roku_CTV_Estimated_Spend_-_Direct_Mail_Campaign':'Roku_CTV_Estimated_Spend_Direct_Mail_Campaign',
 'Roku_CTV_Impressions_-_Direct_Mail_Campaign':'Roku_CTV_Impressions_Direct_Mail_Campaign',
 'Total_Est._DRTV_Linear_Spend_':'Total_Est_DRTV_Linear_Spend'
 }

for key,vals in rename_more_cols.items():
  append_df_2023 = append_df_2023.withColumnRenamed(key, vals)

append_df_2023 = append_df_2023.filter(f.col('Mail_Date')<=f.to_date(f.lit('2023-12-31')))
append_df_2023.columns

# COMMAND ----------

"""
{'Roku_CTV_Estimated_Spend_Direct_Mail_Campaign','Roku_CTV_Impressions_Direct_Mail_Campaign'}
Removing above two channels from DRTV, asthey are not considered in main sheet shared by Joe
"""
## Sleecting relevant columns for FY 2023 ##
cols_2_keep = ['Mail_Date', 'DRTV_Monitored_Spend',
'Linear_Monitored_Impressions',
'Brand_Spend',
'DRTV_Unmonitored_Spend',
'Roku_CTV_Spend',
'Roku_CTV_Impressions',
'Roku_CTV_Estimated_Spend_Direct_Mail_Campaign',
'Roku_CTV_Impressions_Direct_Mail_Campaign',
'Samsung_CTV_Spend',
'Samsung_CTV_Impressions',
'Linear_Brand_Monitored_Impressions',
'Publisher_Direct_CTV_Estimated_Spend',
'Publisher_Direct_CTV_Impressions',
'Yahoo_CTV_Estimated_Spend',
'Yahoo_CTV_Impressions']
append_df_2023 = append_df_2023.select(cols_2_keep)

## Adding New Cols to Main Data Frame ##
cols_2_add = [
  'Linear_Brand_Monitored_Impressions',
  'Publisher_Direct_CTV_Estimated_Spend',
  'Publisher_Direct_CTV_Impressions',
  'Yahoo_CTV_Estimated_Spend',
  'Yahoo_CTV_Impressions',
  'Roku_CTV_Estimated_Spend_Direct_Mail_Campaign',
  'Roku_CTV_Impressions_Direct_Mail_Campaign',
 ]
for cols in cols_2_add:
  df = df.withColumn(cols, f.lit(0))

# COMMAND ----------

# MAGIC %md
# MAGIC # Appending Data to Main Dataframe

# COMMAND ----------


### sorting column in same order ###
cols = df.columns

### Appending ###
df = df.union(append_df_2023.select(cols))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Creation for Model Development ##

# COMMAND ----------

drtv = df.withColumnRenamed('Linear_Monitored_Impressions', 'DRTV_Linear_Monitored_Impressions')\
             .withColumnRenamed('Brand_Spend', 'DRTV_Brand_Spend')\
             .withColumnRenamed('Roku_CTV_Spend', 'DRTV_Roku_CTV_Spend')\
             .withColumnRenamed('Roku_CTV_Impressions', 'DRTV_Roku_CTV_Impressions')\
             .withColumnRenamed('Samsung_CTV_Spend', 'DRTV_Samsung_CTV_Spend')\
             .withColumnRenamed('Samsung_CTV_Impressions', 'DRTV_Samsung_CTV_Impressions')\
             .withColumnRenamed('Linear_Brand_Monitored_Impressions', 'DRTV_Linear_Brand_Monitored_Impressions')\
             .withColumnRenamed('Publisher_Direct_CTV_Estimated_Spend', 'DRTV_Publisher_Direct_CTV_Estimated_Spend',)\
             .withColumnRenamed('Publisher_Direct_CTV_Impressions', 'DRTV_Publisher_Direct_CTV_Impressions')\
             .withColumnRenamed('Yahoo_CTV_Impressions', 'DRTV_Yahoo_CTV_Impressions')\
             .withColumnRenamed('Yahoo_CTV_Estimated_Spend', 'DRTV_Yahoo_CTV_Estimated_Spend')\
             .withColumnRenamed('Roku_CTV_Estimated_Spend_Direct_Mail_Campaign', 'DRTV_Roku_CTV_Estimated_Spend_Direct_Mail_Campaign')\
             .withColumnRenamed('Roku_CTV_Impressions_Direct_Mail_Campaign', 'DRTV_Roku_CTV_Impressions_Direct_Mail_Campaign')
               

drtv.display()

# COMMAND ----------

## Saving Dataframe ##

table_name = "temp.ja_blend_mmm_DRTV" 
spark.sql('DROP TABLE IF EXISTS temp.ja_blend_mmm_DRTV')  
drtv.write.mode("overwrite").saveAsTable(table_name)

model_df = spark.sql("select * from temp.ja_blend_mmm_DRTV")
model_df.display()

# COMMAND ----------



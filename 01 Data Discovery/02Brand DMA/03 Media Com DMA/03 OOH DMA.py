# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update 3/11/2024: Added DMA information to 2020-2022 Data
# MAGIC ## Update 3/11/2024: Adding 2023 Data, it has spend only

# COMMAND ----------

df_master = spark.sql("select * from default.aarp_2020_2022_ooh_data_csv_2")
df_master = df_master.fillna(0, subset=["A18+_IMPS"])


## CHnaging Date format ##
df_master = df_master.withColumn('BOOKING_START_DATE_', f.to_date(f.col('BOOKING_START_DATE'), "M/d/yyyy"))
df_master = df_master.withColumn('BOOKING_END_DATE_', f.to_date(f.col('BOOKING_END_DATE'), "M/d/yyyy"))

data_collect = df_master.collect()

# COMMAND ----------

df_master.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Converting Dataframe into row as date format

# COMMAND ----------

def new_df(LINE_OF_BUSINESS, start_date, end_date, imps, spend, format_type, market):
  
  new_df = spark.sparkContext.parallelize([Row(LINE_OF_BUSINESS=LINE_OF_BUSINESS, BOOKING_START_DATE=start_date, BOOKING_END_DATE=end_date, IMPRESSION=imps, SPEND=spend, FORMAT_TYPE=format_type, MARKET=market)]).toDF()
  new_df = new_df.withColumn('start_date', f.col('BOOKING_START_DATE').cast('date')).withColumn('end_date', f.col('BOOKING_END_DATE').cast('date'))
  new_df = new_df.withColumn('WEEK', f.explode(f.expr('sequence(start_date, end_date, interval 1 day)')))

  ct = new_df.groupBy().count().take(1)[0][0]
  ### Impressions ###
  new_df = new_df.withColumn("Avg_IMPRESSION", f.col("IMPRESSION")/ct)
  
  ### Spend ###
  new_df = new_df.withColumn("Avg_SPEND", f.col("SPEND")/ct)
  
  return new_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Data for 2020-2021-2022

# COMMAND ----------

## Empty Dataframe with Scehma ##
from pyspark.sql.types import StructType,StructField, StringType
schema = StructType([
  StructField('LINE_OF_BUSINESS', StringType(), True),
  StructField('BOOKING_START_DATE', DateType(), True),
  StructField('BOOKING_END_DATE', DateType(), True),
  StructField('IMPRESSION', DoubleType(), True),
  StructField('SPEND', DoubleType(), True),
  StructField('Avg_IMPRESSION', DoubleType(), True),
  StructField('Avg_SPEND', DoubleType(), True),
  StructField('FORMAT_TYPE', DoubleType(), True),
  StructField('WEEK', DateType(), True),
  StructField('MARKET', StringType(), True)
  ])

 #Creates Empty RDD
emptyRDD = spark.sparkContext.emptyRDD()


df = spark.createDataFrame(emptyRDD,schema)
# df.printSchema()



### Creating Union Dataframe ###
for row in data_collect:
#   print(row['CAMPAIGN_NAME'],row['BOOKING_START_DATE_'], row['BOOKING_END_DATE_'], row['A18+_IMPS'], row['SPEND'])
  temp_df = new_df(row['LINE_OF_BUSINESS'],row['BOOKING_START_DATE_'], row['BOOKING_END_DATE_'], row['A18+_IMPS'], row['SPEND'], row['FORMAT_TYPE'], row['MARKET'])
#   print("OKAY")
  cols = ['LINE_OF_BUSINESS', 'BOOKING_START_DATE', 'BOOKING_END_DATE', 'IMPRESSION', 'SPEND', 'Avg_IMPRESSION', 'Avg_SPEND', 'FORMAT_TYPE', 'WEEK', 'MARKET']
#   print(temp_df.select(cols).dtypes)
  df = df.union(temp_df.select(cols))
#   break
  
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Data for 2023

# COMMAND ----------

df_2023 = spark.sql("select * from default.aarp_2023_ooh_data_csv")
df_2023 = df_2023.fillna(0, subset=['A18+_IMPS'])

df_2023 = df_2023.withColumn('BOOKING_START_DATE_', f.to_date(f.col('BOOKING_START_DATE'), "M/d/yyyy"))
df_2023 = df_2023.withColumn('BOOKING_END_DATE_', f.to_date(f.col('BOOKING_END_DATE'), "M/d/yyyy"))
df_collect_2023 = df_2023.collect()

# COMMAND ----------

### Creating Union Dataframe with 2023 data ###
for row in df_collect_2023:
#   print(row['CAMPAIGN_NAME'],row['BOOKING_START_DATE_'], row['BOOKING_END_DATE_'], row['A18+_IMPS'], row['SPEND'])
  temp_df = new_df(row['LINE_OF_BUSINESS'],row['BOOKING_START_DATE_'], row['BOOKING_END_DATE_'], row['A18+_IMPS'], row['SPEND'], row['FORMAT_TYPE'], row['MARKET'])
#   print("OKAY")
  cols = ['LINE_OF_BUSINESS', 'BOOKING_START_DATE', 'BOOKING_END_DATE', 'IMPRESSION', 'SPEND', 'Avg_IMPRESSION', 'Avg_SPEND', 'FORMAT_TYPE', 'WEEK', 'MARKET']
#   print(temp_df.select(cols).dtypes)
  df = df.union(temp_df.select(cols))
#   break
  
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processing Data as usual 

# COMMAND ----------

### ROlling Week Date to week start date ###
df = df.withColumn('day_of_week', f.dayofweek(f.col('WEEK')))
df = df.selectExpr('*', 'date_sub(WEEK, day_of_week-2) as Date')
df = df.filter(f.year(f.to_date(f.col('Date'))).isin([2020, 2021, 2022,2023]))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

dma_mapping = {
    "Phoenix": 753,
    "Omaha": 652,
    "Laredo": 749,
    "San Antonio": 641,
    "Louisville": 529,
    "WASHINGTON DC": 511,
    "HOUSTON": 618,
    "St. Louis": 609,
    "Birmingham": 630,
    "Dallas-Ft. Worth": 623,
    "Austin": 635,
    "El Paso": 765,
    "SAN ANTONIO": 641,
    "Chicago": 602,
    "Harlingen-Weslaco-Brownsville-San Benito": 636,
    "Des Moines-Ames": 679,
    "Washington DC": 511,
    "Houston": 618,
    "Cincinnati": 515
}


# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  

final_model_df = df.withColumn("DMA_Code", map_dma_name_to_code("MARKET"))
final_model_df = final_model_df.groupBy('Date', 'DMA_Code', 'FORMAT_TYPE', 'LINE_OF_BUSINESS').agg(f.sum(f.col('Avg_SPEND')).alias('Spend'), f.sum(f.col('AVg_IMPRESSION')).alias('Circulation'))

final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

'''
All Data is at DMA level. Not needed to be distributed to National Level

'''

# COMMAND ----------

## Rename columns and Adding dummy cols to streamline same data format ##

### Rename ###
map_dict = {'LINE_OF_BUSINESS': 'Campaign',
            'FORMAT_TYPE': 'SubChannel',
            'Circulation':'Imps'}
final_model_df = rename_cols_func(final_model_df, map_dict) 

### Adding dummy Vars ###
# final_model_df = final_model_df.withColumn("SubChannel",f.lit("OOH"))
final_model_df = final_model_df.withColumn("Clicks",f.lit(0))

### Re-ordering cols ###
cols = ['Date', 'DMA_Code', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.select(cols)
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # QC Block

# COMMAND ----------

# QC_df = unit_test( df, final_model_df, ['Avg_SPEND','Spend'], ['Date','Date'])
# QC_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Some More Updates

# COMMAND ----------

## Adding Channel Name ##
cols = ['Date', 'DMA_Code', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.withColumn('Channel', f.lit('OOH')).select(cols) ## Re-arranging Columns 
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_MediaCom_OOH" 
save_df_func(final_model_df, table_name)

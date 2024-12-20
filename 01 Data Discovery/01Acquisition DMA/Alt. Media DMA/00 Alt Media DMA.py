# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source.

# COMMAND ----------

## Reading Data ##
# df = spark.read.parquet("dbfs:/blend360/prod/alt_media/alt_media_threshold_date")
df = spark.read.parquet("dbfs:/blend360/prod/alt_media/alt_media_mail_date")
cols = ['threshold_date', 'mail_date','order_create_date', 'key_cd', 'join_renew', 'program_type_desc', 'mail_quantity', 'c_production_cost', 'c_media_cost', 'c_premium_cost', 'responses']
df = df.select(cols)

## Removing Duplicate Cost and Qty ##
df = df.select('mail_date', 'key_cd', 'program_type_desc', 'c_media_cost', 'c_production_cost', 'c_premium_cost', 'mail_quantity', 'responses')
df = df.groupBy('mail_date', 'key_cd', 'program_type_desc', 'c_media_cost', 'c_production_cost', 'c_premium_cost', 'mail_quantity').agg(f.sum(f.col('responses')).alias('responses'))

## Creating a weekly level columm 
df = df.withColumn('day_of_week', f.dayofweek(f.col('mail_date'))).filter(f.year(f.col('mail_date')).isin([2022, 2023]))
df = df.selectExpr('*', 'date_sub(mail_date, day_of_week-2) as week_start')


## Creating Total Cost Column ##
df = df.groupBy("week_start", "program_type_desc", "key_cd").agg(
  f.sum(f.col("c_premium_cost")*f.col("responses")).alias("VarCost"), 
  f.sum(f.col("c_media_cost")+f.col("c_production_cost")).alias("FixedCost"), 
  f.sum(f.col("mail_quantity")).alias("mail_quantity"), 
  f.sum(f.col("responses")).alias("responses")
)




###############################################
########### Adding Edge Weeks #################
###############################################


## Adding Edge Weeks to 2022 ##
df = df.withColumn('week_start', when(f.col('week_start')=='2021-12-27', f.to_date(f.lit('2022-01-03'))).otherwise(f.col('week_start')))

## Adding Edge Weeks to 2023 ##
df = df.withColumn('week_start', when(f.col('week_start')=='2024-01-01', f.to_date(f.lit('2023-12-25'))).otherwise(f.col('week_start')))

df.display()

# COMMAND ----------

## Creating New Df ##
final_model_df = df.alias('final_model_df')

## Renaming Columns ##
rename_dict = {'week_start':'Date', 'program_type_desc':'SubChannel', 'mail_quantity':'MailVolume', 'responses':'Response'}
final_model_df = rename_cols_func(final_model_df, rename_dict)

## Creating New Columns ##
final_model_df = final_model_df.withColumn('Spend',f.col('FixedCost')+f.col('VarCost'))
final_model_df = final_model_df.withColumn('Channel',f.lit('AltMedia'))
final_model_df = final_model_df.withColumn('Campaign',f.col('SubChannel'))

## Cols Re-order ##
cols = ['Date', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'MailVolume', 'Response']
final_model_df = final_model_df.select(cols)
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

'''
Data is at National Level
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------


## Creating Key-Value Dataframe ##
data_tuples = [(key,value) for key, value in norm_ch_dma_code_prop.items()]
norm_pop_prop_df = spark.createDataFrame(data_tuples, schema = ['DmaCode', 'Prop'])

## Joining above df with final_model_df ##
final_model_df = final_model_df.crossJoin(norm_pop_prop_df)

## Updating column Values ##
cols = ['Spend', 'MailVolume', 'Response']
for col in cols:
  final_model_df = final_model_df.withColumn(col, f.col(col)*f.col('Prop') )

## Adding MediaName Column ##
final_model_df = final_model_df.withColumn('MediaName', f.lit('AltMedia'))

select_cols = ['Date', 'DmaCode', 'MediaName', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'MailVolume', 'Response']
final_model_df = final_model_df.select(select_cols)
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Dataframe

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_IcmDigital_AltMedia" 
save_df_func(final_model_df, table_name)

# COMMAND ----------

# ## Loading Data for reporting Purpose ##
# df = load_saved_table("temp.ja_blend_mmm2_IcmDigital_AltMedia")
# df.groupBy('Date', 'Channel', 'SubChannel', 'Campaign').agg( f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('MailVolume')).alias('MailVolume'), f.sum(f.col('Response')).alias('Response') ).display()

# COMMAND ----------



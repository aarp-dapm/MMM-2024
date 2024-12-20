# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

## Reading Data ##
df_22 = read_excel('dbfs:/blend360/sandbox/mmm/icm_digital/fb_rewards/Rewards-Jan-1-2022-Dec-31-2022.xlsx')
df_23 = read_excel('dbfs:/blend360/sandbox/mmm/icm_digital/fb_rewards/Rewards-Jan-1-2023-Dec-31-2023.xlsx')
df = df_22.union(df_23)


## Renaming Columns ##
rename_dict = {'Impressions':'Imps', 'Amount spent (USD)':'Spend', 'Link clicks':'Clicks', 'Campaign name':'Campaign'}
df = rename_cols_func(df, rename_dict)

## Adding Columns ##
df = df.withColumn('SubChannel', f.lit('Facebook'))
df = df.withColumn('Channel', f.lit('Social'))

## Moving Data from Monthly to Daily ##
df = df.withColumn('MonthStart', f.to_date(f.split("Month"," - ")[0], 'yyyy-MM-dd') )
df = df.withColumn('MonthEnd', f.to_date(f.split("Month"," - ")[1], 'yyyy-MM-dd') )
df = df.withColumn( 'Date', f.explode(f.expr('sequence(MonthStart, MOnthEnd, interval 1 day)')))


## Normalizing Metrics to Daily Level ##
df = df.withColumn('num_days', f.date_diff(f.col('MonthEnd'), f.col('MonthStart'))+1) ## to normalize metrics later on 
df = df.withColumn('Imps', f.col('Imps')/f.col('num_days')) ## Impressions ##
df = df.withColumn('Clicks', f.col('Clicks')/f.col('num_days')) ## Clicks ##
df = df.withColumn('Spend', f.col('Spend')/f.col('num_days')) ## Spend ##


## Slecting relevant columns and re-ordering them ##
cols = ['Date', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = df.groupBy('Date', 'Channel', 'SubChannel', 'Campaign').agg( f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks') ) 
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Campaign Grouping 

# COMMAND ----------

##

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

'''
No DMA Level Data
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

## Creating Key-Value Dataframe ##
data_tuples = [(key,value) for key, value in norm_ch_dma_code_prop.items()]
norm_pop_prop_df = spark.createDataFrame(data_tuples, schema = ['DmaCode', 'Prop'])

## Joining above df with final_model_df ##
final_model_df = final_model_df.crossJoin(norm_pop_prop_df)

## Updating column Values ##
cols = ['Spend', 'Imps', 'Clicks']
for col in cols:
  final_model_df = final_model_df.withColumn(col, f.col(col)*f.col('Prop') )


select_cols = ['Date', 'DmaCode', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.select(select_cols)
final_model_df.display()

# COMMAND ----------

## Aggregating Data to Week ## 
final_model_df = final_model_df.withColumn('Date', f.date_format( f.date_sub(f.col('Date'), f.dayofweek(f.col('Date'))-2 ) ,'yyyy-MM-dd') )
final_model_df = final_model_df.groupBy('Date', 'DmaCode', 'Channel', 'SubChannel', 'Campaign').agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks'))
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File 

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_IcmDigital_FbRewards" 
save_df_func(final_model_df, table_name)

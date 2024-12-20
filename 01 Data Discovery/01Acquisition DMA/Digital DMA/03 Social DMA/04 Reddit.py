# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

## Reading Campaign Data ##
df = read_table("temp.ja_blend_mmm2_DigitalCampaign").filter( (f.col('media_type')=='Social') & (f.col('Traffic_Source')=='Reddit') ).filter(f.year(f.col('week_start')).isin([2022, 2023]))


## Rename Columns ##
rename_dict = {'week_start':'Date',  'campaign':'Campaign', 'Impressions':'Imps'}
df = rename_cols_func(df, rename_dict)

## Adding New Columns ##
df = df.withColumn( 'Channel', f.lit('Social'))
df = df.withColumn( 'SubChannel', f.lit('Reddit'))


## Selecting Cols ##
cols = ['Date', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
df.select(cols).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

'''  
No DMA Info available
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

## Creating Key-Value Dataframe ##
data_tuples = [(key,value) for key, value in norm_ch_dma_code_prop.items()]
norm_pop_prop_df = spark.createDataFrame(data_tuples, schema = ['DmaCode', 'Prop'])

## Joining above df with final_model_df ##
final_model_df = df.crossJoin(norm_pop_prop_df)

## Updating column Values ##
cols = ['Spend', 'Imps', 'Clicks']
for col in cols:
  final_model_df = final_model_df.withColumn(col, f.col(col)*f.col('Prop') )

select_cols = ['Date', 'DmaCode', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.select(select_cols)
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # QC Block

# COMMAND ----------

# QC_df = unit_test( df, final_model_df, ['Spend','Spend'], ['Date','Date'])
# QC_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_SocialReddit" 
save_df_func(final_model_df, table_name)

# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

### Table Name ###
TABLE_DMA = "temp.ja_blend_mmm2_DigitalDMA"
TABLE_CAMP = "temp.ja_blend_mmm2_DigitalCampaign"
TABLE_DMA_MASTER = "temp.ja_blend_mmm2_DmaMaster"
TABLE_DV360_WEEK_DMA_PROP = "temp.ja_blend_mmm2_DisplayDV360_week_dma_prop"   

# COMMAND ----------

## Reading Campaign Data ##
df = read_table(TABLE_DMA).filter( (f.col('media_type')=='Social') & (f.col('Traffic_Source')=='Facebook') ).filter(f.year(f.col('week_start')).isin([2022, 2023]))
df = df.withColumn('GEO', when(f.col('GEO').isin(['Washington, District of Columbia']), "District of Columbia").otherwise(f.col('GEO')) ) ## Replacing value of washinton DC 

## Filtering for US States ##
df = df.filter(f.col('GEO').isin(us_states)) 

## Renaming Columns ##
rename_dict = { 'GEO':'State_Name', 'Impressions':'Imps', 'week_start':'Date', 'Traffic_Source':'SubChannel'}
df = rename_cols_func(df, rename_dict)

## Adding New Columns ##
df = df.withColumn( 'Channel', f.lit('Social'))  

## Getting DMA Code for each State ##
df_dma_master = read_table(TABLE_DMA_MASTER).select('Dma_code', 'State_Name', 'StateDmaCount').drop_duplicates()
df = df.join(df_dma_master, on='State_Name', how='left')

## Renaming 'Dma_code' ##
df = df.withColumnRenamed('Dma_code', 'DmaCode')


## Dividing Spend, Imps and Clicks numbers to DMA level ##
cols = ['Spend', 'Imps', 'Clicks']
for col in cols:
  df = df.withColumn(col, f.col(col)/f.col('StateDmaCount'))


## Aggregating Numbers ##
final_model_df = df.groupBy('Date', 'DmaCode', 'Channel', 'SubChannel', 'Campaign').agg( f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks'))
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
table_name = "temp.ja_blend_mmm2_Digital_SocialFB" 
save_df_func(final_model_df, table_name)

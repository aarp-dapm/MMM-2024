# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Final DataFrame

# COMMAND ----------

## Tablenames to be Stitched ## 
table_name = ["temp.ja_blend_mmm2_DRTV_Linear", "temp.ja_blend_mmm2_DRTV_CTV"]

## Saving Dataframes to Stitch ##
df_list = []
for name in table_name:
  df_list.append(load_saved_table(name))


## Stitching Dataframe ##
df = stitch_df(df_list)


## Adding Channel Column ##
df = df.withColumn("MediaName", f.lit('DRTV'))

## Rearranging Cols ##
cols = ['Date', 'DMA_Code', "MediaName", 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
df = df.select(cols)
df.display()

# COMMAND ----------

## Filtering for Direct Mail Campaigns Spend ## Date: 05/29/2024
df = df.filter(~f.col('Campaign').isin(['DirectMailCampaign']))

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File 

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_DRTV" 
save_df_func(df, table_name)

# COMMAND ----------



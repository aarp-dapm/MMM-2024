# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Final DataFrame

# COMMAND ----------

## Loading Dataframe ##
df_searchDCM = load_saved_table("temp.ja_blend_mmm2_asi_SearchDcm")
df_Meta = load_saved_table("temp.ja_blend_mmm2_ASI_MetaVo2")

# COMMAND ----------

## Stitch DataFrame ##
df_name_list = [df_searchDCM, df_Meta]
df = stitch_df(df_name_list)

## Adding Channel Column ##
df = df.withColumn("MediaName", f.lit('ASI'))

## Rearranging Cols ##
cols = ['Date', 'DMA_Code', 'MediaName', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
df = df.select(cols)
df.display()

# COMMAND ----------

df.select('Date').dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File 

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_ASI" 
save_df_func(df, table_name)

# COMMAND ----------

temp = read_table(table_name)
temp.groupBy('Date', 'MediaName', 'Channel', 'SubChannel', 'Campaign').agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks')).display()

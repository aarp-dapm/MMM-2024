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
table_name = ["temp.ja_blend_mmm2_SearchBing" , "temp.ja_blend_mmm2_Digital_SearchGoogle", "temp.ja_blend_mmm2_Digital_SearchYT", "temp.ja_blend_mmm2_GooglePerfMax", "temp.ja_blend_mmm2_GoogleDiscovery"]

## Saving Dataframes to Stitch ##
df_list = []
for name in table_name:
  df_list.append(load_saved_table(name))


## Stitching Dataframe ##
df = stitch_df(df_list)

# COMMAND ----------

## Adding Channel Column ##
df = df.withColumn("MediaName", f.lit('MembershipDigital'))

## Rearranging Cols ##
cols = ['Date', 'DMA_Code', "MediaName", 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
df = df.select(cols)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Campaign Grouping

# COMMAND ----------

df = df.withColumn('Campaign', f.col('SubChannel'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File 

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_MemDigitalSearch" 
save_df_func(df, table_name)

# COMMAND ----------

table_name = "temp.ja_blend_mmm2_MemDigitalSearch" 
df = read_table(table_name)
df.display()

# COMMAND ----------

df.groupBy(f.year('Date').alias('Year'),'Channel').agg( f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks') ).display()

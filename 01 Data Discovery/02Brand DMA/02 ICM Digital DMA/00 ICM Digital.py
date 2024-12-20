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
df_msoft = load_saved_table("temp.ja_blend_mmm2_IcmDigital_Microsoft")
df_GoogleSearch = load_saved_table("temp.ja_blend_mmm2_IcmDigital_GoogleSearch" )
df_aff = load_saved_table("temp.ja_blend_mmm2_IcmDigital_Affiliate" )
df_dv360 = load_saved_table("temp.ja_blend_mmm2_IcmDigital_Dv360" )
df_dmp = load_saved_table("temp.ja_blend_mmm2_IcmDigital_Dmp" )
df_fb = load_saved_table("temp.ja_blend_mmm2_IcmDigital_FbRewards" )

## Stitch DataFrame ##
df_name_list = [df_msoft, df_GoogleSearch, df_aff, df_dv360, df_dmp, df_fb]
# df_name_list = [df_msoft, df_GoogleSearch, df_dv360, df_dmp]
df = stitch_df(df_name_list)

## Adding Channel Column ##
df = df.withColumn("MediaName", f.lit('IcmDigital'))

## Rearranging Cols ##
cols = ['Date', 'DMA_Code', "MediaName", 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
df = df.select(cols)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File 

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_IcmDigital" 
save_df_func(df, table_name)

# COMMAND ----------



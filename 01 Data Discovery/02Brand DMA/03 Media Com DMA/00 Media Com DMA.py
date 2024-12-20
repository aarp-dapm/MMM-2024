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
df_tv1 = load_saved_table("temp.ja_blend_mmm2_BrandOfflineTV")
df_tv2 = load_saved_table("temp.ja_blend_mmm2_NonBrandOfflineTV")
df_tv3 = load_saved_table("temp.ja_blend_mmm2_BrandOfflineTV_FromDRTV")
df_radio = load_saved_table("temp.ja_blend_mmm2_MediaCom_Radio")
df_ooh = load_saved_table("temp.ja_blend_mmm2_MediaCom_OOH")
df_print = load_saved_table("temp.ja_blend_mmm2_MediaCom_Print")
df_search = load_saved_table("temp.ja_blend_mmm2_MediaCom_Search")
df_social = load_saved_table("temp.ja_blend_mmm2_MediaCom_Social")
df_otherOnline = load_saved_table("temp.ja_blend_mmm2_MediaCom_OtherOnline")

# COMMAND ----------

## Stitch DataFrame ##
df_name_list = [df_tv1, df_tv2, df_radio, df_ooh, df_print, df_search, df_social, df_otherOnline]
df = stitch_df(df_name_list)

## Adding Channel Column ##
df = df.withColumn("MediaName", f.lit('MediaCom'))

## Rearranging Cols ##
cols = ['Date', 'DMA_Code', 'MediaName', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
df = df.select(cols)
df.display()

# COMMAND ----------

## Last mins edits ##
df = df.replace('Targeted Communities', 'TargetedCommunities', subset=['Campaign'])
# df.groupBy('Date', 'MediaName', 'Channel', 'SubChannel', 'Campaign').agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks')).display()

# COMMAND ----------

## Creating Another Table with Media Com Divided and Brand and Non Brand ##
df2 = df.withColumn("MediaName", when(f.col('MediaName')=='MediaCom', when( (f.col('Campaign') == 'Brand') | (f.col('SubChannel').like("Brand%")) ,'MediaComBrand').otherwise('MediaComOtherEM')) )

cols = ["Date", "DMA_Code", "MediaName", "Channel", "SubChannel", "Campaign", "Spend", "Imps", "Clicks"]
df2 = df2.select(cols)
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File 

# COMMAND ----------

## Saving Dataframe ##
# table_name = "temp.ja_blend_mmm2_MediaCom" 
table_name = "temp.ja_blend_mmm2_MediaCom2" 
save_df_func(df2, table_name)

# COMMAND ----------

## Saving Dataframe for Brand Only ##
df_brand = df.filter( (f.col('Campaign') == 'Brand') | (f.col('SubChannel').like("Brand%")) )

table_name = "temp.ja_blend_mmm2_MediaCom_BrandOnly" 
save_df_func(df_brand, table_name)

# COMMAND ----------

## Reading Table ##
df = read_table("temp.ja_blend_mmm2_MediaCom_BrandOnly")
df = df.groupBy('Date', 'MediaName', 'Channel', 'SubChannel').agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks') )


## Creating Individual Dataframe ##
spend_df = df.withColumn('Channel',f.concat_ws('_',f.col('Channel'), f.lit('Spend'))).groupBy('Date').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df.withColumn('Channel',f.concat_ws('_',f.col('Channel'), f.lit('Imps'))).groupBy('Date').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df.withColumn('Channel',f.concat_ws('_',f.col('Channel'), f.lit('Clicks'))).groupBy('Date').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date'], how='left').join(clicks_df, on=['Date'], how='left').fillna(0)
final_model_df.display()
## Saving DataFrame ##
# table_name = "temp.ja_blend_mmm2_MediaDf_ch"
# save_df_func(final_model_df, table_name)

# COMMAND ----------

table_name = "temp.ja_blend_mmm2_MediaCom_BrandOnly" 
df = read_table(table_name)
df.display()

# COMMAND ----------

df.groupBy(f.year(f.col('Date')).alias('Year'), f.col('Channel')).agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks')).filter(f.col('Year').isin([2023])).display()

# COMMAND ----------



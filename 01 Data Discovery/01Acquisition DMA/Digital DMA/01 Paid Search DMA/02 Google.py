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
TABLE_GS_WEEK_DMA_PROP = "temp.ja_blend_mmm2_GoogleSearch_week_dma_prop"  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calcualting and Saving Weekly DMA Prop for Google Search: START

# COMMAND ----------

# ## Raeding Data ##
# df_dma = read_table(TABLE_DMA).filter(f.col('media_type')=='Search')
# df_dma_master = read_table(TABLE_DMA_MASTER).select('Zip', 'Dma_code').drop_duplicates() ## One ZipCode can be part of multiple DMAs

# ## filtering for 'Search' ##
# df_dma = df_dma.filter(f.col('media_type')=='Search')

# ## Creating DMA Code and Region Name Column ##
# df_dma = df_dma.withColumn('Zip',f.substring(f.col('GEO'), -5,5))
# df_dma = df_dma.withColumn('Region', f.expr("substring(GEO, 1, length(GEO) - 5)"))


# ## Mapping Zip to DMA Code ##
# df_dma = df_dma.join(df_dma_master, on=['Zip'], how='left').filter(~f.col('Dma_code').isNull())  ## filter for NULL DMA_Code ##

# ## Week-DMA Aggregation ##
# week_dma_agg = df_dma.groupBy('week_start', 'Dma_code').agg(f.sum(f.col('Spend')).alias('Spend'))

# ## Window Function ##
# window = Window.partitionBy('week_start')
# week_dma_prop = week_dma_agg.withColumn('SpendProp', f.col('Spend')/f.sum(f.col('Spend')).over(window) ).drop('Spend')
# week_dma_prop.display()

# ## renaming Column ##
# week_dma_prop = rename_cols_func(week_dma_prop, { 'week_start':'Date', 'Dma_code':'DmaCode'})

# ## Saving Week DMA Prop ##
# table_name = "temp.ja_blend_mmm2_GoogleSearch_week_dma_prop" 
# save_df_func(week_dma_prop,table_name, display=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calcualting and Saving Weekly DMA Prop for Google Search: END

# COMMAND ----------

## Reading Campaign Data ##
df = read_table(TABLE_CAMP).filter( (f.col('media_type')=='Search') & (f.col('Traffic_Source')=='Google Paid Search') ).filter(f.year(f.col('week_start')).isin([2022, 2023]))

## Adding New Columns ## 
df = df.withColumn('Campaign', f.lit('PlaceHolderAsOfNow')) ## Place Holder as Of Now
df = df.withColumn('Channel', f.lit('Search')) ## AARP clubs this channel in Search
df = df.withColumn('SubChannel', f.lit('GoogleSearch')) ##

## Renaming Channels ##
df = rename_cols_func(df, {'week_start':'Date', 'Impressions':'Imps'})

## Selecting Channels and Aggregating ##
df =df.groupBy('Date', 'Channel', 'SubChannel', 'Campaign').agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks')).fillna(0, subset=['Spend', 'Imps', 'Clicks'])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Data

# COMMAND ----------

## Reading YT Weekly DMA Proportion Table ## 
week_dma_prop_df = read_table(TABLE_GS_WEEK_DMA_PROP)

## DMA population Prop Table ##
data_tuples = [(key,value) for key, value in norm_ch_dma_code_prop.items()]
norm_pop_prop_df = spark.createDataFrame(data_tuples, schema = ['DmaCode', 'SpendProp'])



## Left Join prop table with weekly data table ##
final_model_df = df.join(week_dma_prop_df, on=['Date'], how='left').select('Date', 'DmaCode', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks', 'SpendProp') ## Left Join Here ##


## Selecting Weeks for Missing Data ##
final_model_df_missing_weeks = final_model_df.filter(f.col('DmaCode').isNull()).drop('DmaCode', 'SpendProp')
final_model_df_missing_weeks = final_model_df_missing_weeks.crossJoin(norm_pop_prop_df).select('Date', 'DmaCode', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks', 'SpendProp') ## Cross Join Here ##

## Selecting Weeks for Non-Missing Data ##
final_model_df_non_missing_weeks = final_model_df.filter(~f.col('DmaCode').isNull())


## Union both data and Distribute metrics proportionaly ##
save_model_df = final_model_df_non_missing_weeks.union(final_model_df_missing_weeks)

cols = ['Spend', 'Imps', 'Clicks']
for col in cols:
  save_model_df = save_model_df.withColumn(col, f.col(col)*f.col('SpendProp'))


cols = ['Date', 'DmaCode', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
save_model_df = save_model_df.select(cols)

save_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # QC Block

# COMMAND ----------

QC_df = unit_test( df, save_model_df, ['Spend','Spend'], ['Date','Date'])
QC_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_Digital_SearchGoogle" 
save_df_func(save_model_df, table_name)

# COMMAND ----------

temp_df = read_table("temp.ja_blend_mmm2_Digital_SearchGoogle")


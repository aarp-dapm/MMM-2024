# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

### Reading data from Joe Harr's File ###
df = read_table("temp.ja_blend_mmm_DRTV").withColumnRenamed('Mail_Date','Date')

### CTV Columns ###
ctv_cols_spend = ['DRTV_Roku_CTV_Spend',  'DRTV_Samsung_CTV_Spend', 'DRTV_Publisher_Direct_CTV_Estimated_Spend', 'DRTV_Yahoo_CTV_Estimated_Spend',  'DRTV_Roku_CTV_Estimated_Spend_Direct_Mail_Campaign']
ctv_cols_imp = ['DRTV_Roku_CTV_Impressions', 'DRTV_Samsung_CTV_Impressions', 'DRTV_Publisher_Direct_CTV_Impressions', 'DRTV_Yahoo_CTV_Impressions', 'DRTV_Roku_CTV_Impressions_Direct_Mail_Campaign']
df = df.select(['Date']+ctv_cols_spend+ctv_cols_imp).fillna(0)
df.display()

# COMMAND ----------

## unpivot above columns for spend ##
df_spend = df.unpivot('Date', ctv_cols_spend, 'cols', 'Spend')

condition = when( f.col('cols').like('%Direct_Mail_Campaign%'), 'DirectMailCampaign').\
            when( f.col('cols').like('%Roku%'), 'Roku').\
            when( f.col('cols').like('%Samsung%'), 'Samsung').\
            when( f.col('cols').like('%Yahoo%'), 'Yahoo').\
            when( f.col('cols').like('%Publisher_Direct%'), 'PublisherDirect').\
            otherwise('Other')
df_spend = df_spend.withColumn('Campaign', condition).drop('cols')

## unpivot above columns for impression ##
df_imps = df.unpivot('Date', ctv_cols_imp, 'cols', 'Imps')

condition = when( f.col('cols').like('%Direct_Mail_Campaign%'), 'DirectMailCampaign').\
            when( f.col('cols').like('%Roku%'), 'Roku').\
            when( f.col('cols').like('%Samsung%'), 'Samsung').\
            when( f.col('cols').like('%Yahoo%'), 'Yahoo').\
            when( f.col('cols').like('%Publisher_Direct%'), 'PublisherDirect').\
            otherwise('Other')
df_imps = df_imps.withColumn('Campaign', condition).drop('cols')


## Combining both Spend and Impressions ##
final_model_df = df_spend.join(df_imps, on=['Date','Campaign'], how='left')

## Adding More Columns ##
final_model_df = final_model_df.withColumn('SubChannel', f.lit('ConnectedTV'))
final_model_df = final_model_df.withColumn('Clicks', f.lit(0))
final_model_df = final_model_df.select('Date', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks')
final_model_df.display()


# COMMAND ----------

final_model_df.select('Campaign').dropDuplicates().display()

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

## Creating Key-Value Dataframe ##
data_tuples = [(key,value) for key, value in norm_ch_dma_code_prop.items()]
norm_pop_prop_df = spark.createDataFrame(data_tuples, schema = ['DmaCode', 'Prop'])

## Joining above df with final_model_df ##
final_model_df = final_model_df.crossJoin(norm_pop_prop_df)


## Updating column Values ##
cols = ['Spend', 'Imps', 'Clicks']
for col in cols:
  final_model_df = final_model_df.withColumn(col, f.col(col)*f.col('Prop') )

select_cols = ['Date', 'DmaCode', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
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
# MAGIC # Some More Updates

# COMMAND ----------

## Adding Channel Name ##
cols = ['Date', 'DmaCode', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.withColumn('Channel', f.lit('TV')).select(cols) ## Re-arranging Columns 
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

'''
Direct Mail Campaign is part of DRTV as of now but it will be filtered out in main DRTV table and added as part of Acq. Mail 

'''

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_DRTV_CTV" 
save_df_func(final_model_df, table_name)

# COMMAND ----------

table_name = "temp.ja_blend_mmm2_DRTV_CTV" 
df = read_table(table_name)
df.groupBy( f.year(f.col('Date')).alias('Year'), f.col('Campaign').alias('Campaign')).agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps')).dropDuplicates().display()

# COMMAND ----------

# df.filter(f.col('Date').isin(['2022-12-19', '2022-12-26'])).groupBy('Date', 'SubChannel', 'Campaign').agg( f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps') ).display()

# COMMAND ----------



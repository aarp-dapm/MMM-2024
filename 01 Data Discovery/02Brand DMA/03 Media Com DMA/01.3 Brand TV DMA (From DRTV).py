# Databricks notebook source
# MAGIC %md
# MAGIC # Utility Files and Function 

# COMMAND ----------

# MAGIC %run "./51 Local Utility Files"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

### Reading data from Joe Harr's File ###
df = read_table("temp.ja_blend_mmm_DRTV").withColumnRenamed('Mail_Date','Date').fillna(0,subset=['DRTV_Monitored_Spend', 'DRTV_Brand_Spend', 'DRTV_Unmonitored_Spend'])

## Selecting Columns ## 
cols = ['Date', 'DRTV_Brand_Spend', 'DRTV_Linear_Brand_Monitored_Impressions']
df = df.select(cols)

## Changing Datatype of Date ##
df = df.withColumn('Date', f.date_format(f.col('Date'),'yyyy-MM-dd'))

## Changing Column Names ##
rename_dict = {'DRTV_Brand_Spend':'Spend', 'DRTV_Linear_Brand_Monitored_Impressions':'Imps'}
df = rename_cols_func(df, rename_dict)

## Imputing for NULL Value ##
df = df.fillna(0,subset=['Spend','Imps'])


## Adding more columns yto standardize data ##
df = df.withColumn('SubChannel', f.lit('BrandTV'))
df = df.withColumn('Campaign', f.lit('FromDRTV'))


## Reorder columns ##
cols = ['Date', 'SubChannel', 'Campaign', 'Spend', 'Imps']
df = df.select(cols)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Campaign (LOBs) Renaming

# COMMAND ----------

'''
No Campaign Grouping
'''

# COMMAND ----------

### Aggregating metrics ###
cols = ["Date", "SubChannel", "Campaign"]

spend_col = 'Spend'
imps_col = 'Imps'
# click_col = 'Clicks' ## No Clix Col

final_model_df = df.groupBy(cols).agg(f.sum(f.col(spend_col)).alias('spend'), f.sum(f.col(imps_col)).alias('Imps'))
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

'''
Data at National Level
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
cols = ['Spend', 'Imps']
for col in cols:
  final_model_df = final_model_df.withColumn(col, f.col(col)*f.col('Prop') )

## Updating Column Name ##
final_model_df = final_model_df.withColumnRenamed('DmaCode', 'DMA_Code')

select_cols = ['Date', 'DMA_Code','SubChannel', 'Campaign', 'Spend', 'Imps']
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

## Adding Col name consistency ##
final_model_df = final_model_df.withColumn("Clicks",f.lit(0))

## Adding Channel Name ##
cols = ['Date', 'DMA_Code', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.withColumn('Channel', f.lit('TV')).select(cols) ## Re-arranging Columns 
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_BrandOfflineTV_FromDRTV" 
save_df_func(final_model_df, table_name)

# COMMAND ----------



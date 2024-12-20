# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

###################################################################################################################################################
################################################################ FROM S3 BUCKET ###################################################################
###################################################################################################################################################

### Reading Data ###
df1 = spark.read.csv("s3://edp-prod-ue1-720291645793-s3-funnel-io/SocialBrandDash/data/", header=True).dropDuplicates()
df1 = df1.filter(f.col('Data_Source_name') != 'AARP Brand Evergreen - 10000.42400.575200')
df1 = df1.filter(f.col('Data_Source_id')!='240cbcbc-a8a8-4ee3-b7aa-9378378862aa')

### Agg. Data and creating new cols ####
temp_df1 = df1.groupBy(['Date', 'Traffic_source', 'Campaign']).agg(f.sum('Media_Spend').alias('Spend'), f.sum('Clicks').alias('Clicks'), f.sum('Impressions').alias('Imps'))


###################################################################################################################################################
################################################################ FROM CSV FILE ####################################################################
###################################################################################################################################################

df2 = spark.sql("select * from default.evergreen_2020_2023_2_csv") # File provided by Justin in Phase 2
df2 = df2.withColumn('Date', f.to_date(f.col('Day'), "M/d/y")).drop(f.col('Day')) ## Changing Date to Datetime ##

### Agg. Data and creating new cols ####
temp_df2 = df2.groupBy(['Date', 'Traffic_source', 'Campaign_name']).agg(f.sum('Spend').alias('Spend'), f.sum('Clicks').alias('Clicks'), f.sum('Impressions').alias('Imps'))
temp_df2 = rename_cols_func(temp_df2, {'Campaign_name':'Campaign'})

###################################################################################################################################################
################################################################ UNION DATA #######################################################################
###################################################################################################################################################


df = temp_df1.union(temp_df2)  ### Union DF ###

### Creating Monday as first day of week ###
df = df.withColumn('Date', f.date_format( f.date_sub(f.col('Date'), f.dayofweek(f.col('Date'))-2 ) ,'yyyy-MM-dd') )
df = df.groupBy(['Date', 'Traffic_source', 'Campaign']).agg(f.sum('Spend').alias('Spend'), f.sum('Imps').alias('Imps'), f.sum('Clicks').alias('Clicks'))

## Adding Columns ##
df = df.withColumn('Channel', f.lit('Social'))

### Rename Columns and re-ordering columns  ###
df = rename_cols_func(df, {'Traffic_source':'SubChannel'})
df =df.select(['Date', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']).fillna(0, subset=['Spend', 'Imps', 'Clicks'])
df.display()

# COMMAND ----------

df.select('Channel', 'SubChannel', 'Campaign').dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Campaign Grouping: to be done

# COMMAND ----------



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
final_model_df = df.crossJoin(norm_pop_prop_df)

## Updating column Values ##
cols = ['Spend', 'Imps', 'Clicks']
for col in cols:
  final_model_df = final_model_df.withColumn(col, f.col(col)*f.col('Prop') )

select_cols = ['Date', 'DmaCode','MediaName', 'Channel', 'SubChannel', 'Campaign','Spend', 'Imps', 'Clicks']
## Adding Columns ##
final_model_df = final_model_df.withColumn('MediaName', f.lit('IcmSocial'))
final_model_df = final_model_df.select(select_cols)
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Save Dataframe

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_IcmSocial" 
save_df_func(final_model_df, table_name)

# COMMAND ----------

## For Reporting ##
table_name = "temp.ja_blend_mmm2_IcmSocial" 
df = read_table(table_name)
df.groupBy('Date', 'Channel', 'SubChannel').agg( f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks') ).display()

# COMMAND ----------

df.select('SubChannel', 'Campaign').dropDuplicates().display()

# COMMAND ----------



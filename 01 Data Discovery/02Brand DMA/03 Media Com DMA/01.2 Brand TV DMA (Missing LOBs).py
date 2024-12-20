# Databricks notebook source
# MAGIC %md
# MAGIC # Utility Files and Function 

# COMMAND ----------

# MAGIC %run "./51 Local Utility Files"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

path_list = ['dbfs:/blend360/sandbox/mmm/brand_offline/tv/2023 Missing TV Data.xlsx']

## Appending Data for 2022 and 2023 ##
df = read_excel(path_list[0])

## Filtering rows ##
df = df.filter(~f.col('WEEK').isin(['***']))
df.display()

# COMMAND ----------

## Reading CSV File List ##
path_list = ['dbfs:/blend360/sandbox/mmm/brand_offline/tv/2023 Missing TV Data.xlsx']

## Appending Data for 2022 and 2023 ##
df = read_excel(path_list[0])

## Filtering rows ##
df = df.filter(~f.col('WEEK').isin(['***']))

## Create column with Monday as starting Date  ##
df = df.withColumn('Date', f.date_format(f.date_sub(f.to_date(f.col('WEEK'),'MM/dd/yy'), f.dayofweek(f.to_date(f.col('WEEK'),'MM/dd/yy'))-2), 'yyyy-MM-dd'))

## Rename Column Name ##
rename_dict = {'MARKET':'DmaName', 'LOB':'Campaign', 'AD3564 IMP':'Imps', 'SPEND (GROSS)':'Spend'}
df = rename_cols_func(df,rename_dict)

## Adding Columns ##
df = df.withColumn('SubChannel', f.lit('NonBrandTV'))

## Columns ##
cols = ['Date', 'SubChannel', 'DmaName', 'Campaign', 'Spend', 'Imps']
df = df.select(cols).dropDuplicates()
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Campaign (LOBs) Renaming

# COMMAND ----------

'''
To Keep consistent Nomenclature, rewriting names as TARGETED COMMUNITIES --> TargetedCommunities  PROGRAMS --> Programs
'''

df = df.withColumn('Campaign', when(f.col('Campaign')=='TARGETED COMMUNITIES', 'TargetedCommunities')\
                              .when(f.col('Campaign')=='PROGRAMS', 'Programs')\
                              .otherwise('ERROR'))

df.display()

# COMMAND ----------

### Aggregating metrics ###
cols = ["Date", "DmaName", "SubChannel", "Campaign"]

spend_col = 'Spend'
imps_col = 'Imps'
# click_col = 'Clicks' ## No Clix Col

final_model_df = df.groupBy(cols).agg(f.sum(f.col(spend_col)).alias('spend'), f.sum(f.col(imps_col)).alias('Imps'))
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

dma_mapping = {
    "WASHINGTON DC": 511,
    "HOUSTON TX": 618,
    "CHICAGO IL": 602,
    "MIAMI/FT LAUDERDALE FL": 528,
    "LOS ANGELES CA": 803,
    "ATLANTA GA": 524,
    "DALLAS/FT WORTH TX": 623,
    "PHILADELPHIA PA": 504,
    "NEW YORK NY": 501

}

# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  
dma_col = "DmaName"
cols = ["Date", "DMA_Code", "SubChannel", "Campaign"]

final_model_df = final_model_df.withColumn("DMA_Code", map_dma_name_to_code(dma_col))
final_model_df = final_model_df.groupBy(cols).agg(f.sum(f.col('spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'))
# final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

'No Data at National Level'

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
table_name = "temp.ja_blend_mmm2_NonBrandOfflineTV" 
save_df_func(final_model_df, table_name)

# COMMAND ----------



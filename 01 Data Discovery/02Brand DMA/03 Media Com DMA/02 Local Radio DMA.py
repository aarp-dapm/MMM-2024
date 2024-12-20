# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

'''
COMMENTS:
# Update 3/11/2024: Adding Local Radio Data for 2023 (but it has only spend data)
# Update 3/11/2024: Clubbing Data at Week, DMA Level
# Update 3/11/2024: Removing Impression and GRPs as 2023 data has spend numbers only
# Update 5/14/2024: Updating Radio Data with latest data feed from Media Com 
'''

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

## Reading 2022 Data ##
df_22 = read_excel('dbfs:/blend360/sandbox/mmm/brand_offline/radio/AARP 2022 Network Radio Data - Updated 5.5.24 - Dollars only, no talent fees.xlsx')

## Renaming Columns ##
rename_dict = {'WEEK':'Date', 'MEDIA TYPE':'SubChannel', 'Line of Business':'Campaign', 'GROSS COST':'Spend'}
df_22 = rename_cols_func(df_22, rename_dict)

## Creating Monday as first day of week ##
df_22 = df_22.withColumn('Date', f.date_format(f.date_sub(f.to_date(f.col('Date'), 'M/d/yy'), f.dayofweek(f.to_date(f.col('Date'), 'M/d/yy'))-2) ,'yyyy-MM-dd'))

## Imputing Columns ##
df_22 = df_22.withColumn('DmaName', f.lit('National'))

## Re-Arranging Columns ##
cols = ['Date', 'DmaName', 'SubChannel', 'Campaign', 'Spend']
df_22 = df_22.select(cols)

#######################################################


## Reading 2023 data ##
df_23 = read_excel('dbfs:/blend360/sandbox/mmm/brand_offline/radio/AARP 2023 RECONCILIATION RADIO AS OF 4.26.24 - Dollars only rev.xlsx')

## Renaming Columns ##
rename_dict = {'WEEK':'Date', 'Line of Business':'Campaign', 'SPEND (GROSS)':'Spend', 'MARKET':'DmaName'}
df_23 = rename_cols_func(df_23, rename_dict)

## Creating Monday as first day of week ##
df_23 = df_23.withColumn('Date', f.date_format(f.date_sub(f.to_date(f.col('Date'), 'M/d/yy'), f.dayofweek(f.to_date(f.col('Date'), 'M/d/yy'))-2) ,'yyyy-MM-dd'))

## Imputing Columns ##
df_23 = df_23.withColumn('SubChannel', f.lit('Radio'))

## Re-Arranging Columns ##
cols = ['Date', 'DmaName', 'SubChannel', 'Campaign', 'Spend']
df_23 = df_23.select(cols)

###########################################################


## Joining both Dataframe ##
df = df_22.union(df_23)
df.display()


# COMMAND ----------

## Aggregating values ##
final_model_df = df.groupBy('Date', 'DmaName', 'SubChannel', 'Campaign').agg(f.sum(f.col('Spend')).alias("Spend") )
final_model_df = final_model_df.withColumn('Imps', f.lit(0))
final_model_df = final_model_df.withColumn('Clicks', f.lit(0))
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DMA Map

# COMMAND ----------

dma_mapping = {
'DALLAS TX': 623,
 'National': 1000,
 'NEW YORK CITY NY': 501,
 'WASHINGTON DC': 511,
 'HOUSTON TX': 618,
 'NEW ORLEANS LA': 622,
 'AUSTIN TX': 635,
 'FLAGSTAFF AZ': 753,
 'CHICAGO IL': 602,
 'MEMPHIS TN': 640,
 'BOSTON MA': 506,
 'DETROIT MI': 505,
 'SAINT LOUIS MO': 609,
 'MIAMI FL': 528,
 'BIRMINGHAM AL': 630,
 'LOS ANGELES CA': 803,
 'ATLANTA GA': 524,
 'NASSAU/SUFFOLK NY': 501,  # Imputed Manually from Google Search (NYC DMA)
 'PHILADELPHIA PA': 504,
 'SPRINGFIELD MA': 543,
 'OMAHA NE': 652,
 'MILWAUKEE WI': 617
 }

# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  

final_model_df = final_model_df.withColumn("DMA_Code", map_dma_name_to_code("DmaName"))
final_model_df = final_model_df.groupBy('Date', 'DMA_Code', 'SubChannel', 'Campaign').agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks'))
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

## Seperating National and DMA level information ##
final_model_df_national = final_model_df.filter(f.col('DMA_Code')==1000).drop('DMA_Code')
final_model_df_dma = final_model_df.filter(f.col('DMA_Code')!=1000).withColumnRenamed('DMA_Code','DmaCode')





## Creating Key-Value Dataframe ##
data_tuples = [(key,value) for key, value in norm_ch_dma_code_prop.items()]
norm_pop_prop_df = spark.createDataFrame(data_tuples, schema = ['DmaCode', 'Prop'])

## Joining above df with final_model_df ##
final_model_df_national = final_model_df_national.crossJoin(norm_pop_prop_df)

## Dividing Metrics by 210 to distribute data at national level ##
cols = ["Spend", "Imps", "Clicks"]
for col in cols:
  final_model_df_national = final_model_df_national.withColumn(col,f.col(col)*f.col('Prop'))
  



## Re-arranging COlumns ##
cols = ['Date', 'DmaCode', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df_national = final_model_df_national.select(cols)


## Combining back with DMA level Data ##
save_df = final_model_df_national.union(final_model_df_dma)
save_df.display()

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
save_df = save_df.withColumn('Channel', f.lit('Radio')).select(cols) ## Re-arranging Columns 
save_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_MediaCom_Radio" 
save_df_func(save_df, table_name)

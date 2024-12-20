# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

'''
COMMENTS:
# Update 3/11/2024: Adding 2023 data (but it has newspaper only and not TRade and Magazine)
# ToDo 3/14/2024: Provide DMA Population level Information
# Update 5/14/2024: Adding New Print data source provided by Media Com
'''

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

## Reading 2022 Data ##
df_22  = read_excel('dbfs:/blend360/sandbox/mmm/brand_offline/print/AARP 2022 PRINT DATA.xlsx').filter(f.col('MEDIA').isNotNull()).fillna(0,subset=['NET COST'])

## Rename columns ##
rename_map = {'INSERTION DATE':'Date', 'MARKET':'DmaName', 'MEDIA TYPE':'Channel', 'MEDIA':'SubChannel', 'LOB':'Campaign', 'NET COST':'Spend', 'CIRCULATION':'Imps'}
df_22 = rename_cols_func(df_22, rename_map)

## Changing week start to Monday ##
df_22 = df_22.withColumn('Date', f.date_format(f.date_sub(f.to_date(f.col('Date'), 'yyyy-MM-dd'), f.dayofweek(f.to_date(f.col('Date'), 'yyyy-MM-dd'))-2), 'yyyy-MM-dd') )

## Selecting Relevant Columns ##
cols = ['Date', 'DmaName', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps']
df_22 = df_22.select(cols)

## Imputing Channel Name to Print ##
df_22 = df_22.withColumn('Channel', f.lit('Print'))

## Updating SubChannel value ##
df_22 = df_22.replace(['Newspaper', 'N NEWSPAPER', 'T TRADE', 'M MAGAZINE'], ['Newspaper', 'Newspaper', 'Trade', 'Magazine'], 'SubChannel')

#########################################################


## Reading 2023 Data ##
df_23 = read_excel('dbfs:/blend360/sandbox/mmm/brand_offline/print/AARP 2023 PRINT DATA 5.3.24.xlsx').filter(f.col('MEDIA').isNotNull()).fillna(0,subset=['NET COST'])


## Rename columns ##
rename_map = {'INSERTION DATE':'Date', 'MARKET':'DmaName', 'MEDIA TYPE':'Channel', 'MEDIA':'SubChannel', 'Line of Business':'Campaign', 'NET COST':'Spend', 'CIRCULATION':'Imps'}
df_23 = rename_cols_func(df_23, rename_map)

## Update Date Format ##
df_23 = df_23.withColumn('Date', when( f.col('Date')=='11/1/23', '11/1/2023').when( f.col('Date')=='11/17/23', '11/17/2023').when( f.col('Date')=='11/24/23', '11/24/2023').otherwise(f.col('Date')))

## Changing week start to Monday ##
df_23 = df_23.withColumn('Date', f.date_format(f.date_sub(f.to_date(f.col('Date'), 'M/d/y'), f.dayofweek(f.to_date(f.col('Date'), 'M/d/y'))-2), 'yyyy-MM-dd') )

## Imputing Channel Name to Print ##
df_23 = df_23.withColumn('Channel', f.lit('Print'))

## Selecting Relevant Columns ##
cols = ['Date', 'DmaName', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps']
df_23 = df_23.select(cols)


###########################################################


## Joining both Dataframe ##
df = df_22.union(df_23)
df.display()

# COMMAND ----------

## Aggregating values ##
final_model_df = df.groupBy('Date', 'DmaName', 'Channel', 'SubChannel', 'Campaign').agg(f.sum(f.col('Spend')).alias("Spend"), f.sum(f.col('Imps')).alias("Imps") )
final_model_df = final_model_df.withColumn('Clicks', f.lit(0))
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DMA Map

# COMMAND ----------

dma_mapping = {'Phoenix': 753,
 'Omaha': 652,
 'Dallas': 623,
 'Manchester': 506,
 'San Antonio': 641,
 'Philadelphia': 504,
 'Louisville': 529,
 'Los Angeles': 803,
 'Sacramento': 862,
 'San Diego': 825,
 'Tallahassee': 530,
 'St. Louis': 609,
 'Birmingham': 630,
 'Memphis': 640,
 'Austin': 635,
 'Naples': 571,
 'Milwaukee': 617,
 'Chicago': 602,
 'Tampa': 539,
 'El Centro': 771,
 'Fort Lauderdale': 528,
 'Atlanta': 524,
 'Lexington': 541,
 'Seattle': 819,
 'St. Paul': 613,
 'New Jersey': 501,  # from 2020-2022 data file, assigning it to NYC
 'Washington DC': 511,
 'St Louis': 609,
 'Houston': 618,
 'Jacksonville': 561,
 'New Hampshire': 506,  # Assuming it refers to Boston et al, MA-NH
 "Atlaa": 524,  # Atlaa is Atlaa Inquirer, assigning it to Atlanta
 'Cincinnati': 515,
 'San Bernardino': 803,  # Assuming it refers to Los Angeles, CA
 'Miami': 528,
 'Orlando': 534,
 'New York': 501,
 'Sioux Falls': 725}


#  dma_mapping = {
#     "Phoenix": 753,
#     "Omaha": 652,
#     "National": 1000,  # DMA code not provided
#     "Dallas": 623,
#     "Vermont ": 523,
#     "Manchester": 506,
#     "Bakersfield": 800,
#     "San Antonio": 641,
#     "Philadelphia": 504,
#     "Louisville": 529,
#     "Los Angeles": 803,
#     "Sacramento": 862,
#     "San Diego": 825,
#     "San Francisco": 807,
#     "Tallahassee": 530,
#     "St Louis": 609,
#     "St. Louis": 609,
#     "Birmingham": 630,
#     "Memphis": 640,
#     "Austin": 635,
#     "Naples": 571,
#     "Milwaukee": 617,
#     "Chicago": 602,
#     "Tampa": 539,
#     "El Centro": 771,
#     "Fort Lauderdale": 528,
#     "Atlanta": 524,
#     "Long Island": 501,  # Assuming it is part of NYC
#     "Lexington": 541,
#     "Tuscon": 789,
#     "Seattle": 819,
#     "St. Paul": 613,
#     "New Jersey": 501,  # from 2020-2022 data file, assigning it to NYC
#     "Washington DC": 511,
#     "San Jose": 807,
#     "New York City": 501,
#     "Houston": 618,
#     "Jacksonville": 561,
#     "New Hampshire": 506,  # Assuming it refers to Boston et al, MA-NH
#     "Atlaa": 524,  # Atlaa is Atlaa Inquirer, assigning it to Atlanta
#     "Cincinnati": 515,
#     "San Bernardino": 803,  # Assuming it refers to Los Angeles, CA
#     "Miami": 528,
#     "Baltimore": 512,
#     "Orlando": 534,
#     "New York": 501,  # Assuming it refers to NYC
#     "Sioux Falls": 725
# }



# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  

final_model_df = final_model_df.withColumn("DMA_Code", map_dma_name_to_code("DmaName"))
final_model_df = final_model_df.groupBy('Date', 'DMA_Code', 'Channel', 'SubChannel', 'Campaign').agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks') )
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

'''
No Data at National Level
'''

# COMMAND ----------

# MAGIC %md
# MAGIC # Some More Updates

# COMMAND ----------

## Re-arranging Columns ##
cols = ['Date', 'DMA_Code', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.select(cols) 
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_MediaCom_Print" 
save_df_func(final_model_df, table_name)

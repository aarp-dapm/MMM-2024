# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

### Getting 2022 data ###
path_22 = 'dbfs:/blend360/sandbox/mmm/brand_online/Other_Digital_2022_MMM_6.5.xlsx'
df_22 = read_excel(path_22).withColumn("Date", f.to_date(f.col('Week'))).filter(f.year(f.col('Date'))==2022)
col_list = list(df_22.columns)


### Getting 2023 Data ###
path_23 = 'dbfs:/blend360/sandbox/mmm/brand_online/Other_Digital_2023_MMM_6.5.xlsx'
df_23 = read_excel(path_23).withColumn("Date", f.to_date(f.col('Week'))).filter(f.year(f.col('Date'))==2023)
df_23 = df_23.select(col_list)


### Union Data Frame ###
df = df_22.select(col_list).union(df_23.select(col_list))

### Filtering for Cols ###
cols = ['Date', 'Channel', 'Campaign Name', 'Spend', 'Impressions', 'Clicks'] 
df = df.select(cols)

### Filter for Paid Search and Social ###
df = df.filter(~f.col('Channel').isin(['Paid Search', 'Social', 'Facebook']))

### Renaming Columns ###
df = rename_cols_func(df,{'Week':'Date', 'Channel':'SubChannel', 'Impressions':'Imps'})
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Campaign Grouping

# COMMAND ----------

'''
* Campaign Groups: Program, Brand, States etc
* For YouTube: Setting ['YouTube Preferred', 'YouTube Select', 'YouTube TrueView', 'YouTube Vevo'] as campaign and grouping them to 'YouTube' channel
'''

## Campaign Grouping ##

### Code for Previous Grouping ###
# campaign_grp_file = 'dbfs:/blend360/sandbox/mmm/brand_online/campaign_groups/OtherOnline_2022_2023_campaigns.xlsx' 
# campaign_grp = read_excel(campaign_grp_file)
# df = df.join(campaign_grp, on=['Campaign'], how='left').drop('Campaign').withColumnRenamed('Groups', 'Campaign')

### Code for Updated Grouping ###
campaign_grp_file = 'dbfs:/blend360/sandbox/mmm/brand_online/campaign_groups/EM Updated - To Upload - digital_2022_2023_campaigns  Vo2.xlsx'
campaign_grp = read_excel(campaign_grp_file).drop('Campaign')
df = df.join(campaign_grp, on=['Campaign Name'], how='left')


## Impute for NULL campaigns ##
df = df.fillna('Others', subset=['Line of Business Updated'])

## Renaming Columns ##
df = df.withColumnRenamed('Line of Business Updated', 'Campaign')

## ReOrdering Column ##
cols = ['Date', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks'] 
df = df.select(cols)


## Updating SubChannel ##
df = df.withColumn('SubChannel', when(f.lower(f.col('SubChannel')).like('%youtube%'), f.lit('YouTube')).otherwise(f.col('SubChannel')))

# COMMAND ----------

### Aggregating metrics ###
cols = ["Date", "SubChannel", "Campaign"]

spend_col = 'Spend'
imps_col = 'Imps'
click_col = 'Clicks'

final_model_df = df.groupBy(cols).agg(f.sum(f.col(spend_col)).alias('spend'), f.sum(f.col(imps_col)).alias('Imps'), f.sum(f.col(click_col)).alias('clicks')).fillna(0, subset = ['spend', 'Imps', 'clicks'])
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

'No DMA Map Needed as Data is at aggregated level'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

## Dividing Metrics by 210 to distribute data at national level ##
final_model_df = final_model_df.withColumn("Spend",f.col('spend')/210)
final_model_df = final_model_df.withColumn("Imps",f.col('Imps')/210)
final_model_df = final_model_df.withColumn("Clicks",f.col('clicks')/210)

## Creating DMA DF ##
df_dma = spark.createDataFrame(dma_code, StringType()).toDF('DMA_Code')


## Cross Joining Data ##
final_model_df = final_model_df.crossJoin(df_dma)

## Re-arranging COlumns ##
cols = ['Date', 'DMA_Code', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.select(cols)

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
final_model_df = final_model_df.withColumn('Channel', when(f.col('SubChannel').isin(['OLV', 'Video', 'YouTube']),'Video').when(f.col('SubChannel').isin(['Newsletter']),'Email').otherwise(f.col('SubChannel')))

## Re-arranging Columns ##
cols = ['Date', 'DMA_Code', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.select(cols)  
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_MediaCom_OtherOnline" 
save_df_func(final_model_df, table_name)

# COMMAND ----------



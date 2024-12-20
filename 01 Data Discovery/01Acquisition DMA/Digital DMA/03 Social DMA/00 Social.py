# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Final DataFrame

# COMMAND ----------

## Tablenames to be Stitched ## 
table_name = ["temp.ja_blend_mmm2_SocialReddit", "temp.ja_blend_mmm2_SocialTikTok", "temp.ja_blend_mmm2_SocialLinkedin", "temp.ja_blend_mmm2_SocialInstagram", "temp.ja_blend_mmm2_Digital_SocialFB"]

## Saving Dataframes to Stitch ##
df_list = []
for name in table_name:
  df_list.append(load_saved_table(name))


## Stitching Dataframe ##
df = stitch_df(df_list)

# COMMAND ----------

## Adding Channel Column ##
df = df.withColumn("MediaName", f.lit('MembershipDigital'))

## Rearranging Cols ##
cols = ['Date', 'DmaCode', "MediaName", 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
df = df.select(cols)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Campaign Grouping

# COMMAND ----------

## Reading Campaign Files ##
campaign_maps = read_excel('dbfs:/blend360/sandbox/mmm/digital/campaign_groups/Social Campaigns Grouping.xlsx').dropDuplicates()

## Joining Campaign Files ##
df = df.join(campaign_maps, on=['SubChannel','Campaign'], how='left')

## Dropping Campaign Column and Renaming it ##
df = df.drop('Campaign').withColumnRenamed('Groups','Campaign').select(cols)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File 

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_MemDigitalSocial" 
save_df_func(df, table_name)

# COMMAND ----------

table_name = "temp.ja_blend_mmm2_MemDigitalSocial" 
df = read_table(table_name)
df.display()

# COMMAND ----------

df.groupBy( f.year(f.col('Date')).alias('Year'), 'Channel').agg( f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks') ).display()

# COMMAND ----------



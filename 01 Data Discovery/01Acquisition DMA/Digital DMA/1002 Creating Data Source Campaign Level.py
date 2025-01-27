# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

df = spark.read.csv("s3://edp-prod-ue1-720291645793-s3-funnel-io/DIGITALMEMBERSHIP/data/", header='true').dropDuplicates()
df = df.filter(f.year(f.col('Date')).isin([2022,2023]))
df.display()

# COMMAND ----------

## Confirm it with Mrigaya Once
condition = when(f.col('Traffic_Source')=='AdWords', 'Paid Search').\
            when(f.col('Traffic_Source').isin(['Search Non-Brand Bing', 'Search Brand Bing', 'Bing Paid Search']),'Paid Search').\
            when(f.col('Traffic_Source').isin(['Google Paid Search']),'Paid Search').\
            when(f.col('Media_type')=='Unknown',f.col('Traffic_Source')).\
            when(f.col('Traffic_Source') == 'Gmail','Display').\
            when(f.col('Media_type') == 'Content','Native').\
            when(f.col('Traffic_Source') == 'Nextdoor','Social').\
            when(f.col('Media_type') == 'YouTube Prospecting','Social').\
            otherwise(f.col('Media_type'))
df = df.withColumn('comb_media_traffic_source', condition)


### Combing on top of previous combination ###
condition = when(f.col('comb_media_traffic_source').isNull(), 'Paid Search').\
            when(f.col('comb_media_traffic_source')=='Search', 'Paid Search').\
            when(f.col('comb_media_traffic_source')=='Nextdoor', 'Social').\
            when(f.col('comb_media_traffic_source')=='Gmail', 'Display').\
            when(f.col('Traffic_Source')=='YouTube Trueview', 'Youtube Trueview').otherwise(f.col('comb_media_traffic_source'))
      
df = df.withColumn('channel_group', condition)
df = df.withColumn('media_type', when(f.col('Traffic_Source')=='AdWords',f.col('Traffic_Source')).otherwise(f.col('Media_type')))

#######################################  
col = ['Traffic_Source', 'media_type', 'comb_media_traffic_source', 'channel_group']
df.select(col).dropDuplicates().display()


# COMMAND ----------

channel_group = ['Paid Search', 'Social', 'Display', 'Youtube Trueview', 'Native', 'Google Discovery']
df = df.filter(f.col('channel_group').isin(channel_group))

df = df.filter( (~f.col('Traffic_Source_with_DCM_Sites').isNull()) & (~f.col('Media_Tactic').isNull()) | (f.col("media_type")=="Google Discovery"))

temp = df.groupBy(['Date', 'channel_group', 'media_type', 'Traffic_Source', 'campaign']).agg(f.sum('cost').alias('Spend'), f.sum('Clicks').alias('Clicks'), f.sum('Impressions').alias('Impressions'))

temp = temp.withColumn('day_of_week', f.dayofweek(f.col('Date')))
temp = temp.selectExpr('*', 'date_sub(Date, day_of_week-2) as week_start')
temp = temp.select(['week_start', 'channel_group', 'media_type', 'Traffic_Source', 'campaign', 'Spend', 'Clicks', 'Impressions'])
temp = temp.filter(f.col('week_start')<=f.to_date(f.lit('2023-12-31')))
temp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

table_name = "temp.ja_blend_mmm2_DigitalCampaign" 
save_df_func(temp,table_name, display=True)

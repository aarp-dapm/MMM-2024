# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Update 3/7/2024: Script created for YouGov Data

# COMMAND ----------

'''
* Metric for 50+ user only, YouGov not available for <50 audience
* Net Score for metric: Index, Buzz, Impression, Quality, Value, Reputation, Satisfaction, Recommend
* %Yes for metric:  Awareness, Attention, Ad Awareness, WOM Exposure, cosnideration, Purchase Intent, Current Customer, Former Customer
'''

# COMMAND ----------

# df_master = spark.sql("select * from default.yougovdataupload_csv")
df_master = spark.sql("select * from default.you_gov_data_upload")
df_master = df_master.withColumn('Date', f.to_date(f.col('Date'), 'M/d/y'))
df_master.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Save File

# COMMAND ----------



# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_BrandMetrics_YouGovVo2" 
save_df_func(df_master, table_name)

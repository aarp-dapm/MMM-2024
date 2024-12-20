# Databricks notebook source
# MAGIC %md
# MAGIC # Utility File

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Processing Master DMA Files #

# COMMAND ----------

file_path = 'dbfs:/blend360/sandbox/mmm/utils/DMA_Mappings_Master.csv'
df = read_csv(file_path)

## Padding Zero on left ##
df = df.withColumn('Zip',f.lpad(f.col('Zip'),5,'0'))

## State DMA Count ##

df_unique_dma = df.dropDuplicates(['State Code', 'Dma_code'])
df_dma_count = df_unique_dma.groupBy('State Code').count().withColumnRenamed('count', 'StateDmaCount')
df = df.join(df_dma_count, ['State Code'], 'left')
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving DMA Master File 

# COMMAND ----------

table_name = "temp.ja_blend_mmm2_DmaMaster" 
save_df_func(df,table_name, display=True)

# COMMAND ----------



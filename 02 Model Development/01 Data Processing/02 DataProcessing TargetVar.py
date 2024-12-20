# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Target Variable

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Join Data

# COMMAND ----------

table_name = "temp.ja_blend_mmm_dma_JOIN" 
df_join = read_table(table_name).withColumnRenamed('DMA', 'DmaCode').groupBy('Date', 'DmaCode').pivot('Age_Buckets').agg(f.sum(f.col('Joins')))
df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Registration Data

# COMMAND ----------

table_name = "temp.ja_blend_mmm_dma_REG"
df_reg = read_table(table_name).withColumnRenamed('DMA', 'DmaCode').groupBy('Date', 'DmaCode').pivot('Age_Buckets').agg(f.sum(f.col('Reg')))
df_reg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Refferal Click Data

# COMMAND ----------

table_name = "temp.ja_blend_mmm2_ASI_MemRef"
df_ref = read_table(table_name)
df_ref.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Single DataFrame

# COMMAND ----------

df = df_join.join(df_reg, on=['Date', 'DmaCode'], how='left').join(df_ref, on=['Date', 'DmaCode'], how='left').filter(f.year(f.col('Date')).isin([2022, 2023]))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving Dataframe

# COMMAND ----------

table_name = "temp.ja_blend_mmm2_TargetVarDf"
save_df_func(df,table_name)

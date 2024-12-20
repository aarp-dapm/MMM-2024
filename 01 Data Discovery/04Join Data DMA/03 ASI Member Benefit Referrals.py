# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data 

# COMMAND ----------

## Reading Data ##
df = spark.sql("select * from default.asi_member_benfefit_referrals_csv")


## Changing Date datatype Type ##
df = df.withColumn('Date', f.date_format(f.to_date(f.col("Date"),'M/d/y') , 'yyyy-MM-dd') ) ## Data already roled up at weekly level with week starting at Monday 

## Changing COlumn Names ##
rename_dict = {'Benefit_Referrals_-_ALL':'BenRefAll', 'Benefit_Referrals_-_ALL__Providers/Commercial_Only_': 'BenRefComm' }
df = rename_cols_func(df,rename_dict)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Distributing Data at DMA level #

# COMMAND ----------

## Creating Key-Value Dataframe ##
data_tuples = [(key,value) for key, value in norm_ch_dma_code_prop.items()]
norm_pop_prop_df = spark.createDataFrame(data_tuples, schema = ['DmaCode', 'Prop'])

## Joining above df with final_model_df ##
final_model_df = df.crossJoin(norm_pop_prop_df)


## Multiplying Metric by Prop ##
cols = ['BenRefAll', 'BenRefComm']
for col in cols:
  final_model_df = final_model_df.withColumn(col, f.col(col)*f.col('Prop'))

## Dropping 'Prop Column' ##
final_model_df = final_model_df.select('Date', 'DmaCode', 'BenRefAll', 'BenRefComm').dropDuplicates()

final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File 

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_ASI_MemRef" 
save_df_func(final_model_df, table_name)

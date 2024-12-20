# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update 3/4/2024: Created notebook with DMA level Data

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data 

# COMMAND ----------

# df = spark.read.format("parquet").load("dbfs:/blend360/prod/data_move/daily_registrations_by_dma") Archived
df = spark.read.format("parquet").load("dbfs:/blend360/dev/data_move/daily_registrations_by_dma")
df = df.filter(f.col('unique_users').isNotNull())

# COMMAND ----------

## Creating age groups ##

df = df.withColumn('Age_Buckets', when(f.col('age_agg')<50,'REG_less_than_50')\
                                 .when((f.col('age_agg')>=50) & (f.col('age_agg')<60),'REG_50_59')\
                                 .when((f.col('age_agg')>=60) & (f.col('age_agg')<70),'REG_60_69')\
                                 .when((f.col('age_agg')>=70) & (f.col('age_agg')<80),'REG_70_79')\
                                 .when(f.col('age_agg')>80, 'REG_80_plus').otherwise('REG_NULL')) 

## Creating Date Starting from Monday ##
df = df.withColumn('Date_yyyy_mm_dd', f.to_date(f.col('partsn_dt'), 'yyyyMMdd'))
df = df.withColumn('Date', f.date_sub(f.col('Date_yyyy_mm_dd'),f.dayofweek(f.col('Date_yyyy_mm_dd')-2))) ## Monday as starting of week, hence +2

## Aggregating registartions at week, dma and Age Buckets level ##
reg_df = df.groupBy('Date', 'geo_dma', 'Age_Buckets').agg(f.sum(f.col('unique_users')).alias('Reg'))
reg_df.display()  

# COMMAND ----------

## Imputing for NULL AGE Registrations ##

### Pivot Table ###
pivot_df1 = reg_df.groupBy('Date', 'geo_dma').pivot('Age_Buckets').sum('Reg').fillna(0)

cols = ['REG_less_than_50', 'REG_50_59', 'REG_60_69', 'REG_70_79', 'REG_80_plus']
pivot_df1 = pivot_df1.withColumn('Total', f.expr('+'.join(cols)))

for col in cols:
  pivot_df1 = pivot_df1.withColumn(col, when(f.col('Total')!=0,(f.col(col)+(f.col(col)/f.col('Total'))*f.col('REG_NULL'))).otherwise(f.col('REG_NULL')/len(cols)) )

### Unpivot Table ###
unpivot_df1 = pivot_df1.unpivot(['Date', 'geo_dma'], cols, 'Age_Buckets', 'Reg')
unpivot_df1 = unpivot_df1.filter(f.col('geo_dma').between(200,900))
unpivot_df1.display()

# COMMAND ----------

# ## Imputing for NULL DMA Registrations ##

# ### Pivot ###
# pivot_df2 = unpivot_df1.groupBy('Date', 'Age_Buckets').pivot('geo_dma').sum('Reg').fillna(0)


# #### Creating Total Cols for DMA col and Non-DMA col #### 

# cols = []
# for col in pivot_df2.columns:
#   if col not in ['Date','Age_Buckets']:
#     cols.append(int(col))

# cols_dma = []
# for col in cols:
#   if (col<=900) & (col>=200):
#     cols_dma.append(str(col))

# cols_non_dma = [str(col) for col in list(set(cols)-set(cols_dma))]



# # column_expressions = [f.col(column_name) for column_name in cols_dma]
# # dma_sum_expression= reduce(lambda a, b: a + b, column_expressions)

# # column_expressions = [f.col(column_name) for column_name in cols_non_dma]
# # non_dma_sum_expression= reduce(lambda a, b: a + b, column_expressions)


# pivot_df2 = pivot_df2.withColumn('Total_DMA', dma_sum_expression)
# pivot_df2 = pivot_df2.withColumn('Total_non_DMA', non_dma_sum_expression)


##### Re-distributing Non-DMA Values to DMA ####
# for col in cols_dma:
#   pivot_df2 = pivot_df2.withColumn(str(col), (f.col(str(col)))+ (f.col(str(col))/f.col('Total_DMA'))*(f.col('Total_non_DMA')))

# pivot_df2.display()

## Creating Final DataFrame ##
# final_df = pivot_df2.unpivot(['Date', 'Age_Buckets'], [cols_dma], 'DMA', 'Reg')
# final_df.display()

# COMMAND ----------

## Renaming Columns ##
final_df = unpivot_df1.alias('final_df')
final_df = final_df.withColumnRenamed('geo_dma','DMA')

## Re-ordering Columns ##
cols = ['Date', 'Age_Buckets', 'DMA', 'Reg']
final_df = final_df.select(cols)
final_df.display()

# COMMAND ----------

## Saving Dataframe ##

table_name = "temp.ja_blend_mmm_dma_REG" 
spark.sql('DROP TABLE IF EXISTS temp.ja_blend_mmm_dma_REG')
final_df.write.mode("overwrite").saveAsTable(table_name)

df = spark.sql("select * from temp.ja_blend_mmm_dma_REG")
df.display()

# COMMAND ----------

df = spark.sql("select * from temp.ja_blend_mmm_dma_REG")
df.groupby(f.year(f.col('Date')).alias('Year'), f.col('Age_Buckets')).agg(f.sum(f.col('Reg')).alias('Reg')).display()

# COMMAND ----------

# while True:
#   val=1

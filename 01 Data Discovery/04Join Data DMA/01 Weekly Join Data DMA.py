# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update: Notebook Created on 3/1/2024. Joins data have DMA level as well. Plus parquet file created by Engineering team this time.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data 

# COMMAND ----------

df = spark.read.format("parquet").load("dbfs:/blend360/sandbox/data_move/mem_joins")

## Changing Date to Datetime ##
df = df.withColumn('Date', f.date_sub(f.col('order_Date'),f.dayofweek(f.col('order_Date'))-2)) ## converting order date to week date

## filter for year [2020,2021, 2022]
df = df.filter(f.year(f.col('Date')).isin(['2020', '2021', '2022', '2023'])).na.fill(0)

df.display()

# COMMAND ----------

df.filter(f.col('join_renew')=='Join').groupBy( f.year(f.col('order_Date')).alias('Year'),  f.col('strategy_age_at_creation').alias('AgeGroup'),  f.col('forecast_channel').alias('channel')).agg(f.sum(f.col('orders')).alias('Joins')).display()

# COMMAND ----------

## Data avaialable till 12/28/2022, need updated data ##
df.agg({"Date": "max"}).collect()[0], df.agg({"Date": "min"}).collect()[0]

# COMMAND ----------

df = df.withColumn("Total", f.col('orders'))

### Creating new pivot df ###
pivot_df = df.groupBy('Date','dma_cds').pivot('strategy_age_at_creation').sum('Total').na.fill(0)
pivot_df = pivot_df.withColumn("Total", f.col("< 50")+f.col("50-59")+f.col("60-69")+f.col("70-79")+f.col("80+"))


### Distributing Unkowns into Age Buckets and renaming back columns###
pivot_df = pivot_df.withColumn("<50_", round(f.col("< 50")+((f.col("< 50")*f.col("Unk"))/f.col("Total")),2)).drop("< 50").withColumnRenamed("<50_","JOIN_less_than_50")
pivot_df = pivot_df.withColumn("50-59_", round(f.col("50-59")+((f.col("50-59")*(f.col("Unk"))/f.col("Total"))),2)).drop("50-59").withColumnRenamed("50-59_","JOIN_50_59")
pivot_df = pivot_df.withColumn("60-69_", round(f.col("60-69")+((f.col("60-69")*(f.col("Unk"))/f.col("Total"))),2)).drop("60-69").withColumnRenamed("60-69_","JOIN_60_69")
pivot_df = pivot_df.withColumn("70-79_", round(f.col("70-79")+((f.col("70-79")*(f.col("Unk"))/f.col("Total"))),2)).drop("70-79").withColumnRenamed("70-79_","JOIN_70_79")
pivot_df = pivot_df.withColumn("80+_", round(f.col("80+")+((f.col("80+")*(f.col("Unk"))/f.col("Total"))),2)).drop("80+", "Total", "Unk").withColumnRenamed("80+_","JOIN_80_plus").na.fill(0)

pivot_df.display()

# COMMAND ----------

## Unpivot Table ##

unpivot_df = pivot_df.unpivot(["Date", "dma_cds"], ["JOIN_less_than_50", "JOIN_50_59", "JOIN_60_69", "JOIN_70_79", "JOIN_80_plus"], "Age_Buckets", "Joins") 
unpivot_df = unpivot_df.fillna({'dma_cds': 'NULL DMA'})

## Pivoting DMA cols this time ##
pivot_df2 = unpivot_df.groupBy('Date', 'Age_Buckets').pivot("dma_cds").sum('Joins').na.fill(0)


## Selecting DMA cols only ##
cols = list(set(pivot_df2.columns) - set(['Date','Age_Buckets', 'U', 'NULL DMA', 'Total'])) ## removing some columns out
pivot_df2 = pivot_df2.withColumn('Total', f.expr("+".join(cols)))

### Updating Each column ###
for col in cols:
  pivot_df2 = pivot_df2.withColumn(col, f.col(col) + (f.col('U')+f.col('NULL DMA'))*(f.col(col)/f.col('Total')))
pivot_df2 = pivot_df2.select(['Date','Age_Buckets']+cols)
pivot_df2.display()

# COMMAND ----------

## Creating final_df ##

final_df = pivot_df2.unpivot(['Date', 'Age_Buckets'],cols,'DMA', 'Joins')
final_df.display()

# COMMAND ----------

## Saving Dataframe ##

table_name = "temp.ja_blend_mmm_dma_JOIN" 
spark.sql('DROP TABLE IF EXISTS temp.ja_blend_mmm_dma_JOIN')
final_df.write.mode("overwrite").saveAsTable(table_name)

df = spark.sql("select * from temp.ja_blend_mmm_dma_JOIN")
df.display()

# COMMAND ----------

# while True:
#   val = 1

# COMMAND ----------



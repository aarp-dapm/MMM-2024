# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating DataFrame

# COMMAND ----------

## Creating Dates Columns ##

start_date = '2022-01-01'
end_date = '2023-12-31'

promo_df = spark.sparkContext.parallelize([Row(vyge_id=1000, bookingDt=start_date, arrivalDt=end_date)]).toDF()
promo_df = promo_df.withColumn('start_date', f.col('bookingDt').cast('date')).withColumn('end_date', f.col('arrivalDt').cast('date'))
promo_df = promo_df.withColumn('txnDt', f.explode(f.expr('sequence(start_date, end_date, interval 1 day)'))).select('txnDt')

promo_df = promo_df.withColumn('day_of_week', f.dayofweek(f.col('txnDT')))
promo_df = promo_df.selectExpr('*', 'date_sub(txnDT, day_of_week-2) as week_start')
promo_df = promo_df.filter(f.year(f.to_date(f.col('week_start'))).isin(['2022', '2023'])).select('txnDT','week_start').select('week_start').dropDuplicates()

# promo_df.display()

# COMMAND ----------

##########
###2022###
##########

### Q1 Memb. Drive ###
promo_df = promo_df.withColumn("PromoEvnt_22MembDrive", when((f.col('week_start')>=WeekOf(f.lit('2022-02-28')))&(f.col('week_start')<=WeekOf(f.lit('2022-03-14'))),1).otherwise(0)) 

### Q2 Memorial Day Campaign ###
promo_df = promo_df.withColumn("PromoEvnt_22MemorialDay", when((f.col('week_start')>=WeekOf(f.lit('2022-05-16')))&(f.col('week_start')<=WeekOf(f.lit('2022-06-03'))),1).otherwise(0))

### Q3 Labor Day ###
promo_df = promo_df.withColumn("PromoEvnt_22LaborDay", when((f.col('week_start')>=WeekOf(f.lit('2022-08-22')))&(f.col('week_start')<=WeekOf(f.lit('2022-09-11'))),1).otherwise(0)) 

### Q4 Black Friday ###
promo_df = promo_df.withColumn("PromoEvnt_22BlackFriday",  when((f.col('week_start')>=WeekOf(f.lit('2022-11-16')))&(f.col('week_start')<=WeekOf(f.lit('2022-12-07'))),1).otherwise(0) )


### WSJ and Today Show Coverage ###
promo_df = promo_df.withColumn("PromoEvnt_22WsjTodayShow", when(f.col('week_start')==WeekOf(f.lit('2022-10-28')),1).otherwise(0))

### TikTok ###
promo_df = promo_df.withColumn("PromoEvnt_22TikTok", when(f.col('week_start')==WeekOf(f.lit('2022-11-22')),1).otherwise(0))

### Christmas Holiday ### 
'''
Not provided by AARP
'''
promo_df = promo_df.withColumn("Holiday_22Christams",  when((f.col('week_start')>=WeekOf(f.lit('2022-12-22')))&(f.col('week_start')<=WeekOf(f.lit('2022-12-26'))),1).otherwise(0) )

promo_df.orderBy('txnDT').display()

# COMMAND ----------

##########
###2023###
##########

### Kayla Coupon ###
promo_df = promo_df.withColumn("PromoEvnt_23KaylaCoupon", when((f.col('week_start')>= WeekOf(f.lit('2023-01-23')) )&(f.col('week_start')<= WeekOf(f.lit('2023-01-23')) ),1).otherwise(0))

### Everyday Saving Campaign ###
promo_df = promo_df.withColumn("PromoEvnt_23EvdaySavCamp", when((f.col('week_start')>= WeekOf(f.lit('2023-01-27')) )&( f.col('week_start') <= WeekOf(f.lit('2023-01-27'))),1).otherwise(0))

### ROlling Stone Pre Sale ###
promo_df = promo_df.withColumn("PromoEvnt_23RollingStone", when((f.col('week_start') >= WeekOf(f.lit('2023-11-21')))&(f.col('week_start') <= WeekOf(f.lit('2023-11-28'))),1).otherwise(0) )


### Q1 Memb. Drive ###
promo_df = promo_df.withColumn("PromoEvnt_23MembDrive", when((f.col('week_start')>=WeekOf(f.lit('2023-02-23')))&(f.col('week_start')<=WeekOf(f.lit('2023-03-14'))),1).otherwise(0)) 

### Q2 Memorial Day Campaign ###
promo_df = promo_df.withColumn("PromoEvnt_23MemorialDay", when((f.col('week_start')>=WeekOf(f.lit('2023-05-15')))&(f.col('week_start')<=WeekOf(f.lit('2023-06-01'))),1).otherwise(0))

### Q3 Labor Day ###
promo_df = promo_df.withColumn("PromoEvnt_23LaborDay", when((f.col('week_start')>=WeekOf(f.lit('2023-08-21')))&(f.col('week_start')<=WeekOf(f.lit('2023-09-12'))),1).otherwise(0)) 

### Q4 Black Friday ###
promo_df = promo_df.withColumn("PromoEvnt_23BlackFriday",  when((f.col('week_start')>=WeekOf(f.lit('2023-11-20')))&(f.col('week_start')<=WeekOf(f.lit('2023-12-06'))),1).otherwise(0) )


### Christmas Holiday ### 
'''
Not provided by AARP
'''
promo_df = promo_df.withColumn("Holiday_23Christams",  when((f.col('week_start')>=WeekOf(f.lit('2023-12-22')))&(f.col('week_start')<=WeekOf(f.lit('2023-12-26'))),1).otherwise(0) )


promo_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Standardizing Column Names

# COMMAND ----------

## reanmaing 'week_start column ##
promo_df = promo_df.withColumnRenamed("week_start", "Date")

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving Dataframe 

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_PromoEvents" 
save_df_func(promo_df, table_name)

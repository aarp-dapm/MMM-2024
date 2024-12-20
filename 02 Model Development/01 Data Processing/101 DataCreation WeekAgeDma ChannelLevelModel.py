# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Processing Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Dataframes

# COMMAND ----------

## Reading Traget Var Df ##
df_TargetVar = read_table("temp.ja_blend_mmm2_TargetVarDf")

## Reading Media Df at Channel Level ##
df_media = read_table("temp.ja_blend_mmm2_MediaDf_ch")
for col in df_media.columns:
  if col not in ['Date', 'DmaCode']:
    df_media = df_media.withColumn(col,f.col(col)/5)


df_media_social = read_table("temp.ja_blend_mmm2_MediaDf_Social_ch") ## Adding Individual Social Channel as well  on 7/11/2024
for col in df_media_social.columns:
  if col not in ['Date', 'DmaCode']:
    df_media_social = df_media_social.withColumn(col,f.col(col)/5)

## Reading Media Df at Sub Channel Level ##
df_media_subch = read_table("temp.ja_blend_mmm2_MediaDf_Subch")
for col in df_media_subch.columns:
  if col not in ['Date', 'DmaCode']:
    df_media_subch = df_media_subch.withColumn(col,f.col(col)/5)


## Reading Promo Event and Seasonality Df ##
df_PromoEvent = read_table("temp.ja_blend_mmm2_PromoEvents")
df_Seasonality = read_table("temp.ja_blend_mmm2_seasonality"  )


## Reading Brand Metrics Df ##
df_BrandMetrics = read_table("temp.ja_blend_mmm2_BrandMetrics_YouGov" )

# COMMAND ----------

# MAGIC %md
# MAGIC ## UnPivoting Join Data

# COMMAND ----------

## Selecting Relevant Columns ##
join_cols = [ 'JOIN_50_59', 'JOIN_60_69', 'JOIN_70_79', 'JOIN_80_plus', 'JOIN_less_than_50']
rename_join_cols = ['5059', '6069', '7079', '8099', '1849']
primary_cols = ['Date', 'DmaCode']

df_join = df_TargetVar.select(primary_cols+join_cols)


## Renaming Column Names ##
rename_maps = {'JOIN_50_59':'5059', 'JOIN_60_69':'6069', 'JOIN_70_79':'7079', 'JOIN_80_plus':'8099', 'JOIN_less_than_50':'1849'}
df_join = rename_cols_func(df_join, rename_maps)

## Pivoting Columns ##
df_join = df_join.unpivot(primary_cols, rename_join_cols, 'AgeBucket', 'Joins')
df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## UnPivoting Reg Data

# COMMAND ----------

## Selecting Relevant Columns ##
reg_cols = [ 'REG_50_59', 'REG_60_69', 'REG_70_79', 'REG_80_plus', 'REG_less_than_50']
rename_reg_cols = ['5059', '6069', '7079', '8099', '1849']
primary_cols = ['Date', 'DmaCode']

df_reg = df_TargetVar.select(primary_cols+reg_cols)


## Renaming Column Names ##
rename_maps = {'REG_50_59':'5059', 'REG_60_69':'6069', 'REG_70_79':'7079', 'REG_80_plus':'8099', 'REG_less_than_50':'1849'}
df_reg = rename_cols_func(df_reg, rename_maps)

## Pivoting Columns ##
df_reg = df_reg.unpivot(primary_cols, rename_reg_cols, 'AgeBucket', 'Reg')
df_reg.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Member Benefit Referral Click

# COMMAND ----------

df_Ref = df_TargetVar.select('Date', 'DmaCode', 'BenRefAll', 'BenRefComm')

# COMMAND ----------

# MAGIC %md
# MAGIC ## At Channel Level: Creating One Single DataFrame and Saving it

# COMMAND ----------

## Creating at Channel Level ##
df = df_join.join(df_reg, on=['Date', 'DmaCode', 'AgeBucket'], how='left').join(df_Ref, on=['Date', 'DmaCode'], how='left').join(df_media, on=['Date', 'DmaCode'], how='left').join(df_media_social, on=['Date', 'DmaCode'], how='left').join(df_PromoEvent, on=['Date'], how='left').join(df_Seasonality, on=['Date'], how='left').join(df_BrandMetrics, on=['Date'], how='left').fillna(0)

## Saving it ##
table_name = 'temp.ja_blend_mmm2_DateDmaAge_ChModelDf'
save_df_func(df, table_name)


# COMMAND ----------

# MAGIC %md
# MAGIC ## At Sub Channel Level: Creating One Single DataFrame and Saving it

# COMMAND ----------

## Creating at Sub Channel Level ##
df_subch = df_join.join(df_reg, on=['Date', 'DmaCode', 'AgeBucket'], how='left').join(df_Ref, on=['Date', 'DmaCode'], how='left').join(df_media_subch , on=['Date', 'DmaCode'], how='left').join(df_media_social, on=['Date', 'DmaCode'], how='left').join(df_PromoEvent, on=['Date'], how='left').join(df_Seasonality, on=['Date'], how='left').join(df_BrandMetrics, on=['Date'], how='left').fillna(0)

## Saving it ##
table_name = 'temp.ja_blend_mmm2_DateDmaAge_SubChModelDf'
save_df_func(df_subch, table_name)

# COMMAND ----------



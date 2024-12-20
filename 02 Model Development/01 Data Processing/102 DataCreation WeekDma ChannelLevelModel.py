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

## Reading Media Df at Channel level ##
df_media = read_table("temp.ja_blend_mmm2_MediaDf_ch")
df_media_social = read_table("temp.ja_blend_mmm2_MediaDf_Social_ch") ## Adding Individual Social Channel as well  on 7/11/2024

## Reading Media Df at SubChannel level ##
df_media_subch = read_table("temp.ja_blend_mmm2_MediaDf_Subch")


## Reading Media Df at Buying Group level ##
df_media_BuyingGroup = read_table("temp.ja_blend_mmm2_MediaDf_BuyingGroup")

## Reading Media Df at MembershipFlag level ##
df_media_MemFlag = read_table("temp.ja_blend_mmm2_MediaDf_ChannelMemFlagPivot")

## Reading Promo Event and Seasonality Df ##
df_PromoEvent = read_table("temp.ja_blend_mmm2_PromoEvents")
df_Seasonality = read_table("temp.ja_blend_mmm2_seasonality"  )


## Reading Brand Metrics Df ##
df_BrandMetrics = read_table("temp.ja_blend_mmm2_BrandMetrics_YouGov")

# COMMAND ----------

# MAGIC %md
# MAGIC ## At Channel Level: Creating One Single DataFrame and Saving it

# COMMAND ----------

## Creating One single Dataframe ##
df = df_TargetVar.join(
  df_media, 
  on=['Date', 'DmaCode'],
  how='left').join(
    df_media_social, 
    on=['Date', 'DmaCode'], 
    how='left'
  ).join(
    df_PromoEvent, 
    on=['Date'], 
    how='left'
  ).join(
    df_Seasonality, 
    on=['Date'], 
    how='left'
  ).join(
    df_BrandMetrics, 
    on=['Date'],
    how='left'
  ).fillna(0)

## Saving Data Frame ##
table_name = 'temp.ja_blend_mmm2_DateDma_ChModelDf'
save_df_func(df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## At Sub Channel Level: Creating One Single DataFrame and Saving it

# COMMAND ----------

## Creating One single Dataframe ##
df_subch = df_TargetVar.join(df_media_subch, on=['Date', 'DmaCode'], how='left').join(df_media_social, on=['Date', 'DmaCode'], how='left').join(df_PromoEvent, on=['Date'], how='left').join(df_Seasonality, on=['Date'], how='left').join(df_BrandMetrics, on=['Date'], how='left').fillna(0)

## Saving Data Frame ##
table_name = 'temp.ja_blend_mmm2_DateDma_SubChModelDf'
save_df_func(df_subch, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## At Buying Group Level: Creating One Single DataFrame and Saving it

# COMMAND ----------

## Creating One single Dataframe ##
df_BuyingGroup = df_TargetVar.join(df_media_BuyingGroup, on=['Date', 'DmaCode'], how='left').join(df_PromoEvent, on=['Date'], how='left').join(df_Seasonality, on=['Date'], how='left').join(df_BrandMetrics, on=['Date'], how='left').fillna(0)

## Saving Data Frame ##
table_name = 'temp.ja_blend_mmm2_DateDma_BuyingGroupDf'
save_df_func(df_BuyingGroup, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## At MembershipFlag Level: Creating One Single DataFrame and Saving it

# COMMAND ----------

## Creating One single Dataframe ##
df_BuyingGroup = df_TargetVar.join(df_media_MemFlag, on=['Date', 'DmaCode'], how='left').join(df_PromoEvent, on=['Date'], how='left').join(df_Seasonality, on=['Date'], how='left').join(df_BrandMetrics, on=['Date'], how='left').fillna(0)

## Saving Data Frame ##
table_name = 'temp.ja_blend_mmm2_DateDma_MemFlagDf'
save_df_func(df_BuyingGroup, table_name)

# COMMAND ----------

v=1

# COMMAND ----------



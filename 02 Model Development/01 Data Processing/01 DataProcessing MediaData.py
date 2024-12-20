# Databricks notebook source
# MAGIC %md
# MAGIC # 1.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Reading Membership Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Acq. Mail Data [link](https://edp-prod-adl.cloud.databricks.com/browse/folders/1742481253828662?o=7340359528453699)

# COMMAND ----------

## Acq. Mail ##
AcqMailList = ["temp.ja_blend_mmm2_AcqMail"]
df_AcqMail = df_from_name_list(AcqMailList)
# df_AcqMail.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Alt. Media Data [link](https://edp-prod-adl.cloud.databricks.com/?o=7340359528453699#notebook/1015183552956439/command/1015183552956440)

# COMMAND ----------

## Alt. Mail ##
AltMailList = ["temp.ja_blend_mmm2_IcmDigital_AltMedia"]
df_AltMail = df_from_name_list(AltMailList)
# df_AltMail.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading DRTV Data [link](https://edp-prod-adl.cloud.databricks.com/browse/folders/1742481253828536?o=7340359528453699)

# COMMAND ----------

## DRTV ##
DrtvList = ["temp.ja_blend_mmm2_DRTV"]
df_Drtv = df_from_name_list(DrtvList).withColumnRenamed('DMA_Code','DmaCode')
# df_Drtv.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Digital Memebrship Data [link](https://edp-prod-adl.cloud.databricks.com/browse/folders/4233474517231033?o=7340359528453699)

# COMMAND ----------

## Membership Digital ##
MemDigiList = ["temp.ja_blend_mmm2_MemDigitalSearch", "temp.ja_blend_mmm2_MemDigitalDisplay", "temp.ja_blend_mmm2_MemDigitalSocial", "temp.ja_blend_mmm2_MemDigitalOther"]
df_MemDigi = df_from_name_list(MemDigiList).withColumnRenamed('DMA_Code','DmaCode')
# df_MemDigi.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Leadgen Data [link](https://edp-prod-adl.cloud.databricks.com/browse/folders/1742481253828555?o=7340359528453699)

# COMMAND ----------

## Leadgen Data ##
LeadgenList = ["temp.ja_blend_mmm2_LeadGen_LeadGen"]
df_leadgen = df_from_name_list(LeadgenList).withColumnRenamed('DMA_Code','DmaCode')
# df_leadgen.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Reading Brand Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading ICM Social [link](https://edp-prod-adl.cloud.databricks.com/browse/folders/1742481253828391?o=7340359528453699)

# COMMAND ----------

## ICM Social Data ##
IcmSocialList = ["temp.ja_blend_mmm2_IcmSocial" ]
df_IcmSocial = df_from_name_list(IcmSocialList)
# df_IcmSocial.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading ICM Digital [link](https://edp-prod-adl.cloud.databricks.com/browse/folders/1742481253828427?o=7340359528453699)

# COMMAND ----------

## ICM Digital Data ##
IcmDigitalList = ["temp.ja_blend_mmm2_IcmDigital"]
df_IcmDigital = df_from_name_list(IcmDigitalList).withColumnRenamed('DMA_Code','DmaCode')
# df_IcmDigital.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading MediaCom [link](https://edp-prod-adl.cloud.databricks.com/browse/folders/1742481253828315?o=7340359528453699)

# COMMAND ----------

## Media Com Data ##
# MediaComList = ["temp.ja_blend_mmm2_MediaCom"]
MediaComList = ["temp.ja_blend_mmm2_MediaCom2"]
df_MediaCom = df_from_name_list(MediaComList).withColumnRenamed('DMA_Code','DmaCode')
df_MediaCom.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.0 Reading ASI [link](https://edp-prod-adl.cloud.databricks.com/browse/folders/1742481253828218?o=7340359528453699)

# COMMAND ----------

## ASI Data ##
AsiList = ["temp.ja_blend_mmm2_ASI"]
df_Asi = df_from_name_list(AsiList).withColumnRenamed('DMA_Code','DmaCode')
# df_Asi.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 5.0 Joining All Media Spend

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Marketing

# COMMAND ----------

df_perf_list = [df_MemDigi, df_leadgen, df_AcqMail, df_AltMail, df_Drtv]
df_perf = reduce(DataFrame.unionAll, df_perf_list)
# df_perf.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Brand Marketing
# MAGIC

# COMMAND ----------

df_brand_list =[df_IcmSocial, df_IcmDigital, df_MediaCom]
df_brand = reduce(DataFrame.unionAll, df_brand_list)
# df_brand.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Main Dataframe 

# COMMAND ----------

df = reduce(DataFrame.unionAll, [df_perf, df_brand, df_Asi])
df.display()

# COMMAND ----------

### Creating Additonal Flag for Membership and Non Memebrship Media ###
df = df.withColumn( "MembershipFlag", when(f.col('MediaName').isin(['AcqMail', 'AltMedia', 'DRTV', 'LeadGen', 'MembershipDigital']), 'Membership').otherwise('NonMembership'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving Dataframe 

# COMMAND ----------

# table_name = "temp.ja_blend_mmm2_MediaDf"
# table_name = "temp.ja_blend_mmm2_MediaDf2" 
table_name = "temp.ja_blend_mmm2_MediaDf3"
save_df_func(df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Data at Channel Level 

# COMMAND ----------

## Reading Table ##
# df = read_table("temp.ja_blend_mmm2_MediaDf")
df = read_table("temp.ja_blend_mmm2_MediaDf2")

## Creating Individual Dataframe ##
spend_df = df.withColumn('Channel',f.concat_ws('_',f.col('Channel'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df.withColumn('Channel',f.concat_ws('_',f.col('Channel'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df.withColumn('Channel',f.concat_ws('_',f.col('Channel'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Saving DataFrame ##
# table_name = "temp.ja_blend_mmm2_MediaDf_ch"
table_name = "temp.ja_blend_mmm2_MediaDf_ch2"
save_df_func(final_model_df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Data at SubChannel Level 

# COMMAND ----------

## Reading Table ##
df = read_table("temp.ja_blend_mmm2_MediaDf")

## Creating another subchannel column ##
df = df.withColumn('SubChannelPivot', f.concat_ws('_',f.col('Channel'),f.col('MediaName'),f.col('SubChannel') ))

## Renaming and Replacing few column values ##
df = df.withColumn( 'SubChannelPivot', when(f.col('SubChannelPivot') == 'Social_IcmSocial_facebook', 'Social_IcmSocial_Facebook')\
                                      .when(f.col('SubChannelPivot') == 'Social_IcmSocial_audience_network', 'Social_IcmSocial_Facebook Audience Network')\
                                      .when(f.col('SubChannelPivot') == 'Social_IcmSocial_instagram', 'Social_IcmSocial_Instagram')\
                                      .when(f.col('SubChannelPivot') == 'AltMedia_AltMedia_BLOW-IN', 'AltMedia_AltMedia_BlowIn')\
                                      .when(f.col('SubChannelPivot').like('%AltMedia_AltMedia_CO-OP%'), 'AltMedia_AltMedia_CoOp')\
                                      .otherwise(f.col('SubChannelPivot')))

## Creating Individual Dataframe ##
spend_df = df.withColumn('Channel',f.concat_ws('_',f.col('SubChannelPivot'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df.withColumn('Channel',f.concat_ws('_',f.col('SubChannelPivot'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df.withColumn('Channel',f.concat_ws('_',f.col('SubChannelPivot'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Repalcing special character with '_' ##
final_model_df = final_model_df.select([f.col(col).alias(col.replace(' ', '')) for col in final_model_df.columns])

## Saving DataFrame ##
table_name = "temp.ja_blend_mmm2_MediaDf_Subch"
save_df_func(final_model_df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Data at Buying Group and Channel Level 

# COMMAND ----------

## Reading Table ##
df = read_table("temp.ja_blend_mmm2_MediaDf2")

## Creating another subchannel column ##
df = df.withColumn('BuyingGroupPivot', f.concat_ws('_',f.col('Channel'),f.col('MediaName')))


## Creating Individual Dataframe ##
spend_df = df.withColumn('Channel',f.concat_ws('_',f.col('BuyingGroupPivot'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df.withColumn('Channel',f.concat_ws('_',f.col('BuyingGroupPivot'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df.withColumn('Channel',f.concat_ws('_',f.col('BuyingGroupPivot'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Repalcing special character with '_' ##
final_model_df = final_model_df.select([f.col(col).alias(col.replace(' ', '')) for col in final_model_df.columns])

## Saving DataFrame ##
table_name = "temp.ja_blend_mmm2_MediaDf_BuyingGroup"
save_df_func(final_model_df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Data at Membership and Non Membership Channel Level 

# COMMAND ----------

## Reading Table ##
df = read_table("temp.ja_blend_mmm2_MediaDf3")

## Creating another subchannel column ##
df = df.withColumn('MemFlagPivot', f.concat_ws('_',f.col('Channel'),f.col('MembershipFlag')))


## Creating Individual Dataframe ##
spend_df = df.withColumn('Channel',f.concat_ws('_',f.col('MemFlagPivot'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df.withColumn('Channel',f.concat_ws('_',f.col('MemFlagPivot'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df.withColumn('Channel',f.concat_ws('_',f.col('MemFlagPivot'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Repalcing special character with '_' ##
final_model_df = final_model_df.select([f.col(col).alias(col.replace(' ', '')) for col in final_model_df.columns])

## Saving DataFrame ##
table_name = "temp.ja_blend_mmm2_MediaDf_ChannelMemFlagPivot"
save_df_func(final_model_df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Data at Campaign Level

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving Data for TV (Connected TV)

# COMMAND ----------

##################################
### Reading and Filtering Data ###
##################################

table_name = "temp.ja_blend_mmm2_MediaDf"
df_tv = read_table(table_name)
df_tv_ctv = df_tv.filter(f.col('SubChannel')=='ConnectedTV')


##########################
### Creating Dataframe ###
##########################

## Creating another subchannel column ##
df_tv_ctv  = df_tv_ctv .withColumn('CampaignPivot', f.concat_ws('_',f.col('Channel'),f.col('MediaName'),f.col('SubChannel'),f.col('Campaign') ))

## Creating Individual Dataframe ##
spend_df = df_tv_ctv.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df_tv_ctv.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df_tv_ctv.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Filtering for 2022 and 2023 ##
final_model_df = final_model_df.filter(f.year(f.col('Date')).isin([2022, 2023]))


## Saving DataFrame ##
table_name = "temp.ja_blend_mmm2_MediaDf_Campaign_TV_CTV"
save_df_func(final_model_df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving Data for TV (Linear TV)

# COMMAND ----------

##################################
### Reading and Filtering Data ###
##################################

table_name = "temp.ja_blend_mmm2_MediaDf"
df_tv = read_table(table_name)
df_tv_ltv = df_tv.filter(f.col('SubChannel')=='LinearTV')

# Adding one more row ##
df_tv_ltv = df_tv_ltv.join(read_table(table_name).select('Date', 'DmaCode').filter(f.year(f.col('Date')).isin([2022, 2023])).dropDuplicates() , on= ['Date', 'DmaCode'], how='right').fillna(0)


##########################
### Creating Dataframe ###
##########################

## Creating another subchannel column ##
df_tv_ltv  = df_tv_ltv .withColumn('CampaignPivot', f.concat_ws('_',f.col('Channel'),f.col('MediaName'),f.col('SubChannel'),f.col('Campaign') ))

## Creating Individual Dataframe ##
spend_df = df_tv_ltv.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df_tv_ltv.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df_tv_ltv.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Filtering for 2022 and 2023 ##
final_model_df = final_model_df.filter(f.year(f.col('Date')).isin([2022, 2023]))


## Repalcing special character with '_' ##
final_model_df = final_model_df.select([f.col(col).alias(col.replace(' ', '')) for col in final_model_df.columns])

## Saving DataFrame ##
table_name = "temp.ja_blend_mmm2_MediaDf_Campaign_TV_LTV"
save_df_func(final_model_df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving Data for Media Com Search (Google)

# COMMAND ----------

##################################
### Reading and Filtering Data ###
##################################

table_name = "temp.ja_blend_mmm2_MediaDf"
df = read_table(table_name)
df_MediaCom_SearchGoogle = df.filter( (f.col('MediaName')=='MediaCom') & (f.col('Channel')=='Search') & (f.col('SubChannel')=='Google')  )

# Adding one more row ##
df_MediaCom_SearchGoogle = df_MediaCom_SearchGoogle.join(read_table(table_name).select('Date', 'DmaCode').filter(f.year(f.col('Date')).isin([2022, 2023])).dropDuplicates() , on= ['Date', 'DmaCode'], how='right').fillna(0)


##########################
### Creating Dataframe ###
##########################

## Creating another subchannel column ##
df_MediaCom_SearchGoogle = df_MediaCom_SearchGoogle.withColumn('CampaignPivot', f.concat_ws('_',f.col('Channel'),f.col('MediaName'),f.col('SubChannel'),f.col('Campaign') ))

## Creating Individual Dataframe ##
spend_df = df_MediaCom_SearchGoogle.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df_MediaCom_SearchGoogle.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df_MediaCom_SearchGoogle.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Filtering for 2022 and 2023 ##
final_model_df = final_model_df.filter(f.year(f.col('Date')).isin([2022, 2023]))


## Saving DataFrame ##
table_name = "temp.ja_blend_mmm2_MediaDf_Campaign_MediaCom_SearchGoogle"
save_df_func(final_model_df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving Data for Media Com Search (Bing)

# COMMAND ----------

##################################
### Reading and Filtering Data ###
##################################

table_name = "temp.ja_blend_mmm2_MediaDf"
df = read_table(table_name)
df_MediaCom_SearchBing = df.filter( (f.col('MediaName')=='MediaCom') & (f.col('Channel')=='Search') & (f.col('SubChannel')=='Bing')  )

# Adding one more row ##
df_MediaCom_SearchBing = df_MediaCom_SearchBing.join(read_table(table_name).select('Date', 'DmaCode').filter(f.year(f.col('Date')).isin([2022, 2023])).dropDuplicates() , on= ['Date', 'DmaCode'], how='right').fillna(0)


##########################
### Creating Dataframe ###
##########################

## Creating another subchannel column ##
df_MediaCom_SearchBing = df_MediaCom_SearchBing.withColumn('CampaignPivot', f.concat_ws('_',f.col('Channel'),f.col('MediaName'),f.col('SubChannel'),f.col('Campaign') ))

## Creating Individual Dataframe ##
spend_df = df_MediaCom_SearchBing.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df_MediaCom_SearchBing.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df_MediaCom_SearchBing.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Filtering for 2022 and 2023 ##
final_model_df = final_model_df.filter(f.year(f.col('Date')).isin([2022, 2023]))


## Saving DataFrame ##
table_name = "temp.ja_blend_mmm2_MediaDf_Campaign_MediaCom_SearchBing"
save_df_func(final_model_df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving Data for Media Com Social (Meta)

# COMMAND ----------

##################################
### Reading and Filtering Data ###
##################################

table_name = "temp.ja_blend_mmm2_MediaDf"
df = read_table(table_name)
df_MediaCom_SocialMeta = df.filter( (f.col('MediaName')=='MediaCom') & (f.col('Channel')=='Social') & (f.col('SubChannel')=='Meta')  )

# Adding one more row ##
df_MediaCom_SocialMeta = df_MediaCom_SocialMeta.join(read_table(table_name).select('Date', 'DmaCode').filter(f.year(f.col('Date')).isin([2022, 2023])).dropDuplicates() , on= ['Date', 'DmaCode'], how='right').fillna(0)


##########################
### Creating Dataframe ###
##########################

## Creating another subchannel column ##
df_MediaCom_SocialMeta = df_MediaCom_SocialMeta.withColumn('CampaignPivot', f.concat_ws('_',f.col('Channel'),f.col('MediaName'),f.col('SubChannel'),f.col('Campaign') ))

## Creating Individual Dataframe ##
spend_df = df_MediaCom_SocialMeta.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df_MediaCom_SocialMeta.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df_MediaCom_SocialMeta.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Filtering for 2022 and 2023 ##
final_model_df = final_model_df.filter(f.year(f.col('Date')).isin([2022, 2023]))


## Saving DataFrame ##
table_name = "temp.ja_blend_mmm2_MediaDf_Campaign_MediaCom_SocialMeta"
save_df_func(final_model_df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving Data for Media Com Video (YouTube)

# COMMAND ----------

##################################
### Reading and Filtering Data ###
##################################

table_name = "temp.ja_blend_mmm2_MediaDf"
df = read_table(table_name)
df_MediaCom_VideoYT = df.filter( (f.col('MediaName')=='MediaCom') & (f.col('Channel')=='Video') & (f.col('SubChannel')=='YouTube')  )

# Adding one more row ##
df_MediaCom_VideoYT = df_MediaCom_VideoYT.join(read_table(table_name).select('Date', 'DmaCode').filter(f.year(f.col('Date')).isin([2022, 2023])).dropDuplicates() , on= ['Date', 'DmaCode'], how='right').fillna(0)


##########################
### Creating Dataframe ###
##########################

## Creating another subchannel column ##
df_MediaCom_VideoYT = df_MediaCom_VideoYT.withColumn('CampaignPivot', f.concat_ws('_',f.col('Channel'),f.col('MediaName'),f.col('SubChannel'),f.col('Campaign') ))

## Creating Individual Dataframe ##
spend_df = df_MediaCom_VideoYT.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df_MediaCom_VideoYT.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df_MediaCom_VideoYT.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Filtering for 2022 and 2023 ##
final_model_df = final_model_df.filter(f.year(f.col('Date')).isin([2022, 2023]))


## Saving DataFrame ##
table_name = "temp.ja_blend_mmm2_MediaDf_Campaign_MediaCom_VideoYT"
save_df_func(final_model_df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving Data for Media Com Video (OLV)

# COMMAND ----------

##################################
### Reading and Filtering Data ###
##################################

table_name = "temp.ja_blend_mmm2_MediaDf"
df = read_table(table_name)
df_MediaCom_VideoOLV = df.filter( (f.col('MediaName')=='MediaCom') & (f.col('Channel')=='Video') & (f.col('SubChannel')=='OLV')  )

# Adding one more row ##
df_MediaCom_VideoOLV = df_MediaCom_VideoOLV.join(read_table(table_name).select('Date', 'DmaCode').filter(f.year(f.col('Date')).isin([2022, 2023])).dropDuplicates() , on= ['Date', 'DmaCode'], how='right').fillna(0)


##########################
### Creating Dataframe ###
##########################

## Creating another subchannel column ##
df_MediaCom_VideoOLV = df_MediaCom_VideoOLV.withColumn('CampaignPivot', f.concat_ws('_',f.col('Channel'),f.col('MediaName'),f.col('SubChannel'),f.col('Campaign') ))

## Creating Individual Dataframe ##
spend_df = df_MediaCom_VideoOLV.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df_MediaCom_VideoOLV.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df_MediaCom_VideoOLV.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Filtering for 2022 and 2023 ##
final_model_df = final_model_df.filter(f.year(f.col('Date')).isin([2022, 2023]))


## Saving DataFrame ##
table_name = "temp.ja_blend_mmm2_MediaDf_Campaign_MediaCom_VideoOLV"
save_df_func(final_model_df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving Data for Media Com Display

# COMMAND ----------

##################################
### Reading and Filtering Data ###
##################################

table_name = "temp.ja_blend_mmm2_MediaDf"
df = read_table(table_name)
df_MediaCom_Disp = df.filter( (f.col('MediaName')=='MediaCom') & (f.col('Channel')=='Display'))

# Adding one more row ##
df_MediaCom_Disp = df_MediaCom_Disp.join(read_table(table_name).select('Date', 'DmaCode').filter(f.year(f.col('Date')).isin([2022, 2023])).dropDuplicates() , on= ['Date', 'DmaCode'], how='right').fillna(0)


##########################
### Creating Dataframe ###
##########################

## Creating another subchannel column ##
df_MediaCom_Disp = df_MediaCom_Disp.withColumn('CampaignPivot', f.concat_ws('_',f.col('Channel'),f.col('MediaName'),f.col('SubChannel'),f.col('Campaign') ))

## Creating Individual Dataframe ##
spend_df = df_MediaCom_Disp.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df_MediaCom_Disp.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df_MediaCom_Disp.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Filtering for 2022 and 2023 ##
final_model_df = final_model_df.filter(f.year(f.col('Date')).isin([2022, 2023]))


## Saving DataFrame ##
table_name = "temp.ja_blend_mmm2_MediaDf_Campaign_MediaCom_Display"
save_df_func(final_model_df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving Data for Media Com Brand TV

# COMMAND ----------

##################################
### Reading and Filtering Data ###
##################################

table_name = "temp.ja_blend_mmm2_MediaDf"
df_tv = read_table(table_name)
df_tv_brand = df_tv.filter(f.col('SubChannel')=='BrandTV')


# Adding Missing Rows ##
df_tv_brand = df_tv_brand.join(read_table(table_name).select('Date', 'DmaCode').filter(f.year(f.col('Date')).isin([2022, 2023])).dropDuplicates() , on= ['Date', 'DmaCode'], how='right').fillna(0)



##########################
### Creating Dataframe ###
##########################

## Creating another subchannel column ##
df_tv_brand  = df_tv_brand.withColumn('CampaignPivot', f.concat_ws('_',f.col('Channel'),f.col('MediaName'),f.col('SubChannel'),f.col('Campaign') ))

## Creating Individual Dataframe ##
spend_df = df_tv_brand.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Spend'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Spend')))
imp_df = df_tv_brand.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Imps'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Imps')))
clicks_df = df_tv_brand.withColumn('Channel',f.concat_ws('_',f.col('CampaignPivot'), f.lit('Clicks'))).groupBy('Date', 'DmaCode').pivot('Channel').agg(f.sum(f.col('Clicks')))

## Creating Single Dataframe ##
final_model_df = spend_df.join(imp_df, on=['Date', 'DmaCode'], how='left').join(clicks_df, on=['Date', 'DmaCode'], how='left').fillna(0)

## Filtering for 2022 and 2023 ##
final_model_df = final_model_df.filter(f.year(f.col('Date')).isin([2022, 2023]))


## Saving DataFrame ##
table_name = "temp.ja_blend_mmm2_MediaDf_Campaign_TV_BrandTV"
save_df_func(final_model_df, table_name)

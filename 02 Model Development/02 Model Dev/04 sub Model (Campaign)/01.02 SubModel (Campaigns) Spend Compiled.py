# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading SubModel Model Spend

# COMMAND ----------

## Read Dataframe ##
submodel_spend_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_submodel_spend_compiled.csv').drop('Unnamed: 0',axis=1)
submodel_spend_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Adding TV CTV SubModel

# COMMAND ----------

## Reading Dataframe ##
spend_cols = ['TV_DRTV_ConnectedTV_PublisherDirect_Spend', 'TV_DRTV_ConnectedTV_Roku_Spend', 'TV_DRTV_ConnectedTV_Samsung_Spend', 'TV_DRTV_ConnectedTV_Yahoo_Spend']
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_Campaign_TV_CTV.csv').drop('Unnamed: 0',axis=1).groupby('Date')[spend_cols].sum().reset_index()

## Creating new df ##
submodel_campaign_model_spend_df = submodel_spend_df.copy()


## Dropping Channel Column ##
ch_name = 'TV_DRTV_ConnectedTV_Spend'

## Adding SubChannel level Spend ##
submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Adding TV LTV SubModel

# COMMAND ----------

## Reading Dataframe ##
spend_cols = ['TV_DRTV_LinearTV_10_Spend', 'TV_DRTV_LinearTV_120_Spend', 'TV_DRTV_LinearTV_15_Spend', 'TV_DRTV_LinearTV_30_Spend', 'TV_DRTV_LinearTV_60_Spend']
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_Campaign_TV_LTV.csv').drop('Unnamed: 0',axis=1).groupby('Date')[spend_cols].sum().reset_index()



## Dropping Channel Column ##
ch_name = 'TV_DRTV_LinearTV_Spend'

## Adding SubChannel level Spend ##
submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Adding TV Media Com SubModel

# COMMAND ----------

## Reading Dataframe ##
spend_cols = ['TV_MediaCom_BrandTV_Spend', 'TV_MediaCom_NonBrandTV_Spend']
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_SubChannel_TV.csv').drop('Unnamed: 0',axis=1).groupby('Date')[spend_cols].sum().reset_index()



## Dropping Channel Column ##
ch_name = 'TV_MediaCom_BrandnNonBrandTV_Spend'

## Adding SubChannel level Spend ##
submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Adding Display Media Com SubModel

# COMMAND ----------

## Reading Dataframe ##
spend_cols = ['Display_MediaCom_Display_Brand_Spend','Display_MediaCom_Display_NonBrand_Spend']
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_Campaign_Display_MediaCom.csv').drop('Unnamed: 0',axis=1).groupby('Date')[spend_cols].sum().reset_index()



## Dropping Channel Column ##
ch_name = 'Display_MediaCom_Display_Spend'

## Adding SubChannel level Spend ##
submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Adding Search Google Media Com SubModel

# COMMAND ----------

## Reading Dataframe ##
spend_cols = ['Search_MediaCom_Google_Brand_Spend', 'Search_MediaCom_Google_NonBrand_Spend']
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_Campaign_MediaCom_SearchGoogle.csv').drop('Unnamed: 0',axis=1).groupby('Date')[spend_cols].sum().reset_index()



## Dropping Channel Column ##
ch_name = 'Search_MediaCom_Google_Spend'

## Adding SubChannel level Spend ##
submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Adding Search Bing Media Com SubModel

# COMMAND ----------

## Reading Dataframe ##
spend_cols = ['Search_MediaCom_Bing_Brand_Spend', 'Search_MediaCom_Bing_TargetedCommunities_Spend']
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_Campaign_MediaCom_SearchBing.csv').drop('Unnamed: 0',axis=1).groupby('Date')[spend_cols].sum().reset_index()




## Dropping Channel Column ##
ch_name = 'Search_MediaCom_Bing_Spend'

## Adding SubChannel level Spend ##
submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 Adding Social Meta Media Com SubModel

# COMMAND ----------

## Reading Dataframe ##
spend_cols = ['Social_MediaCom_Meta_Brand_Spend', 'Social_MediaCom_Meta_NonBrand_Spend' ]
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_Campaign_MediaCom_SocialMeta.csv').drop('Unnamed: 0',axis=1).groupby('Date')[spend_cols].sum().reset_index()



## Dropping Channel Column ##
ch_name = 'Social_MediaCom_Meta_Spend'

## Adding SubChannel level Spend ##
submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8 Adding Video Youtube Media Com SubModel

# COMMAND ----------

## Reading Dataframe ##
spend_cols = ['Video_MediaCom_YouTube_Brand_Spend','Video_MediaCom_YouTube_NonBrand_Spend']
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_Campaign_MediaCom_VideoYT.csv').drop('Unnamed: 0',axis=1).groupby('Date')[spend_cols].sum().reset_index()




## Dropping Channel Column ##
ch_name = 'Video_MediaCom_YouTube_Spend'

## Adding SubChannel level Spend ##
submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.9 Adding Video OLV Media Com SubModel

# COMMAND ----------

## Reading Dataframe ##
spend_cols = ['Video_MediaCom_OLV_Brand_Spend','Video_MediaCom_OLV_NonBrand_Spend']
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_Campaign_MediaCom_VideoOLV.csv').drop('Unnamed: 0',axis=1).groupby('Date')[spend_cols].sum().reset_index()




## Dropping Channel Column ##
ch_name = 'Video_MediaCom_OLV_Spend'

## Adding SubChannel level Spend ##
submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_campaign_model_spend_df.groupby(pd.to_datetime(submodel_campaign_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_campaign_model_spend_df = submodel_campaign_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Saving Data

# COMMAND ----------

## Save Dataframe ##
submodel_campaign_model_spend_df.to_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_SubModel_Campaign_spend_compiled.csv')

## Read Dataframe ##
submodel_campaign_model_spend_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_SubModel_Campaign_spend_compiled.csv').drop('Unnamed: 0',axis=1)
submodel_campaign_model_spend_df.display()

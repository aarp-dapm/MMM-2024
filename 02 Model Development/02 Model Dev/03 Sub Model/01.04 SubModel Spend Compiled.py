# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading Buying Group Model Spend

# COMMAND ----------

## Read Agg. Analysis Dataframe ##
buying_group_model_spend = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_BuyingGroup_spend_compiled.csv').drop('Unnamed: 0',axis=1)
buying_group_model_spend.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Adding Search SubModel Spend

# COMMAND ----------

## Reading Dataframe ##

spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_BuyingGroup_Search.csv').drop('Unnamed: 0',axis=1)


spend_cols = ['Search_MembershipDigital_Bing_Spend', 'Search_MembershipDigital_GoogleDiscovery_Spend', 'Search_MembershipDigital_GooglePerfMax_Spend', 'Search_MembershipDigital_GoogleSearch_Spend', 'Search_MembershipDigital_YouTubeTrueView_Spend', 'Search_MediaCom_Bing_Spend', 'Search_MediaCom_Google_Spend', 'Search_IcmDigital_BingSearch_Spend', 'Search_IcmDigital_GoogleSearch_Spend', 'Search_ASI_Bing_Spend', 'Search_ASI_Google_Spend', 'Search_ASI_Unknown_Spend']
spend_df = spend_df.groupby('Date')[spend_cols].sum().reset_index()

## Creating new df ##
submodel_spend_df = buying_group_model_spend.copy()


## Dropping Channel Column ##
ch_name = ['Search_MembershipDigital_Spend', 'Search_MediaCom_Spend', 'Search_IcmDigital_Spend', 'Search_ASI_Spend']

## Adding SubChannel level Spend ##
submodel_spend_df = submodel_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(submodel_spend_df.groupby(pd.to_datetime(submodel_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).reset_index()
ch_df = pd.DataFrame(submodel_spend_df.groupby(pd.to_datetime(submodel_spend_df['Date']).dt.year)[ch_name].sum()).sum(axis=1).reset_index()

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_spend_df = submodel_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Adding Social SubModel Spend

# COMMAND ----------

## Reading Dataframe ##

spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_BuyingGroup_Social.csv').drop('Unnamed: 0',axis=1)


spend_cols = ['Social_MembershipDigital_Facebook_Spend', 'Social_MembershipDigital_Instagram_Spend', 'Social_MembershipDigital_Linkedin_Spend', 'Social_MembershipDigital_Nextdoor_Spend', 'Social_MembershipDigital_Reddit_Spend', 'Social_MembershipDigital_TikTok_Spend', 'Social_MediaCom_Meta_Spend', 'Social_MediaCom_TikTok_Spend', 'Social_MediaCom_X_Spend', 'Social_IcmDigital_Facebook_Spend', 'Social_IcmSocial_FacebookAudienceNetwork_Spend', 'Social_IcmSocial_FacebookMessenger_Spend', 'Social_IcmSocial_Facebook_Spend', 'Social_IcmSocial_Instagram_Spend', 'Social_IcmSocial_LinkedIn_Spend', 'Social_IcmSocial_Pinterest_Spend', 'Social_IcmSocial_TikTok_Spend', 'Social_IcmSocial_Twitter_Spend', 'Social_IcmSocial_messenger_Spend', 'Social_IcmSocial_unknown_Spend', 'Social_ASI_Meta_Spend']
spend_df = spend_df.groupby('Date')[spend_cols].sum().reset_index()


## Dropping Channel Column ##
ch_name = ['Social_MembershipDigital_Spend', 'Social_MediaCom_Spend', 'Social_IcmDigital_Spend', 'Social_IcmSocial_Spend','Social_ASI_Spend']

## Adding SubChannel level Spend ##
submodel_spend_df = submodel_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(submodel_spend_df.groupby(pd.to_datetime(submodel_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).reset_index()
ch_df = pd.DataFrame(submodel_spend_df.groupby(pd.to_datetime(submodel_spend_df['Date']).dt.year)[ch_name].sum()).sum(axis=1).reset_index()

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_spend_df = submodel_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Saving Data

# COMMAND ----------

## Save Dataframe ##
submodel_spend_df.to_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_submodel_spend_compiled.csv')

## Read Dataframe ##
submodel_spend_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_submodel_spend_compiled.csv').drop('Unnamed: 0',axis=1)
submodel_spend_df.display()

# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading Initial Model Spend

# COMMAND ----------

## Read Dataframe ##
initial_model_spend_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_initialmodel_spend.csv').drop('Unnamed: 0',axis=1)
initial_model_spend_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Adding Leadgen SubModel

# COMMAND ----------

## Reading Dataframe ##
spend_cols = [ 'LeadGen_LeadGen_CARDLINKING_Spend','LeadGen_LeadGen_CoRegLeads_Spend', 'LeadGen_LeadGen_Impact_Spend', 'LeadGen_LeadGen_Linkouts_Spend','LeadGen_LeadGen_MajorRocket_Spend', 'LeadGen_LeadGen_OfferWalls_Spend']
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_SubChannel_Leadgen.csv').drop('Unnamed: 0',axis=1).groupby('Date')[spend_cols].sum().reset_index()

## Creating new df ##
buying_group_model_spend_df = initial_model_spend_df.copy()


## Dropping Channel Column ##
ch_name = 'LeadGen_Spend'

## Adding SubChannel level Spend ##
buying_group_model_spend_df = buying_group_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  buying_group_model_spend_df = buying_group_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Adding TV SubModel

# COMMAND ----------

## Reading Dataframe ##

spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_SubChannel_TV.csv').drop('Unnamed: 0',axis=1)
spend_df['TV_MediaCom_BrandnNonBrandTV_Spend'] = spend_df['TV_MediaCom_BrandTV_Spend'] + spend_df['TV_MediaCom_NonBrandTV_Spend']

spend_cols = ['TV_DRTV_ConnectedTV_Spend', 'TV_DRTV_LinearTV_Spend', 'TV_MediaCom_BrandnNonBrandTV_Spend']
spend_df = spend_df.groupby('Date')[spend_cols].sum().reset_index()


## Dropping Channel Column ##
ch_name = 'TV_Spend'

## Adding SubChannel level Spend ##
buying_group_model_spend_df = buying_group_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  buying_group_model_spend_df = buying_group_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Adding Alt-Media SubModel

# COMMAND ----------

## Reading Dataframe ##

spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_SubChannel_AltMedia.csv').drop('Unnamed: 0',axis=1)

spend_df['AltMedia_AltMedia_News&Mag_Spend'] = spend_df['AltMedia_AltMedia_MAGAZINE_Spend'] + spend_df['AltMedia_AltMedia_NEWSPAPER_Spend']
spend_df['AltMedia_AltMedia_SharedMailnWrap_Spend'] = spend_df['AltMedia_AltMedia_SHAREDMAIL_Spend'] + spend_df['AltMedia_AltMedia_SHAREDMAILWRAP_Spend']
spend_df['AltMedia_AltMedia_Other_Spend'] = spend_df['AltMedia_AltMedia_BILLINGSTATEMENT_Spend'] + spend_df['AltMedia_AltMedia_BlowIn_Spend'] + spend_df['AltMedia_AltMedia_CoOp_Spend'] + spend_df['AltMedia_AltMedia_PIP_Spend'] + spend_df['AltMedia_AltMedia_PROSPECTRAL_Spend']

spend_cols = ['AltMedia_AltMedia_Other_Spend', 'AltMedia_AltMedia_News&Mag_Spend', 'AltMedia_AltMedia_SharedMailnWrap_Spend']
spend_df = spend_df.groupby('Date')[spend_cols].sum().reset_index()


## Dropping Channel Column ##
ch_name = 'AltMedia_Spend'

## Adding SubChannel level Spend ##
buying_group_model_spend_df = buying_group_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  buying_group_model_spend_df = buying_group_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Adding Direct Mail SubModel

# COMMAND ----------

## Reading Dataframe ##

spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_SubChannel_DirectMail.csv').drop('Unnamed: 0',axis=1)
spend_df['DirectMail_AcqMail_DirectnConnectedTV_Spend'] = spend_df['DirectMail_AcqMail_ConnectedTV_Spend'] + spend_df['DirectMail_AcqMail_DirectMail_Spend']


spend_cols = ['DirectMail_AcqMail_ConnectedTV_Spend', 'DirectMail_AcqMail_DirectMail_Spend', 'DirectMail_AcqMail_WinbackMail_Spend']
spend_df = spend_df.groupby('Date')[spend_cols].sum().reset_index()


## Dropping Channel Column ##
ch_name = 'DirectMail_Spend'

## Adding SubChannel level Spend ##
buying_group_model_spend_df = buying_group_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  buying_group_model_spend_df = buying_group_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Adding Email SubModel

# COMMAND ----------

## Reading Dataframe ##

spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_SubChannel_Email.csv').drop('Unnamed: 0',axis=1)


spend_cols = ['Email_AcqMail_WinbackEmail_Spend', 'Email_LeadGen_Email_Spend', 'Email_MediaCom_Newsletter_Spend']
spend_df = spend_df.groupby('Date')[spend_cols].sum().reset_index()


## Dropping Channel Column ##
ch_name = 'Email_Spend'

## Adding SubChannel level Spend ##
buying_group_model_spend_df = buying_group_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  buying_group_model_spend_df = buying_group_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Adding Video SubModel

# COMMAND ----------

## Reading Dataframe ##

spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_SubChannel_Video.csv').drop('Unnamed: 0',axis=1)


spend_cols = ['Video_IcmDigital_Native_Spend',  'Video_IcmDigital_YouTube_Spend',  'Video_MediaCom_OLV_Spend', 'Video_MediaCom_YouTube_Spend']
spend_df = spend_df.groupby('Date')[spend_cols].sum().reset_index()


## Dropping Channel Column ##
ch_name = 'Video_Spend'

## Adding SubChannel level Spend ##
buying_group_model_spend_df = buying_group_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  buying_group_model_spend_df = buying_group_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 Adding Display SubModel

# COMMAND ----------

## Reading Dataframe ##

spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_SubChannel_Display.csv').drop('Unnamed: 0',axis=1)


spend_cols = ['Display_ASI_Display_Spend', 'Display_IcmDigital_Display_Spend', 'Display_IcmDigital_Other_Spend', 'Display_MediaCom_Display_Spend', 'Display_MembershipDigital_AmazonDSP_Spend','Display_MembershipDigital_DV360_Spend', 'Display_MembershipDigital_Outbrain_Spend', 'Display_MembershipDigital_Yahoo_Spend']
spend_df = spend_df.groupby('Date')[spend_cols].sum().reset_index()


## Dropping Channel Column ##
ch_name = 'Display_Spend'

## Adding SubChannel level Spend ##
buying_group_model_spend_df = buying_group_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  buying_group_model_spend_df = buying_group_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8 Adding Search SubModel

# COMMAND ----------

## Reading Dataframe ##

spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_BuyingGroup_Search.csv').drop('Unnamed: 0',axis=1)

spend_df['Search_MembershipDigital_Spend'] = spend_df[['Search_MembershipDigital_Bing_Spend', 'Search_MembershipDigital_GoogleDiscovery_Spend', 'Search_MembershipDigital_GooglePerfMax_Spend', 'Search_MembershipDigital_GoogleSearch_Spend', 'Search_MembershipDigital_YouTubeTrueView_Spend']].sum(axis=1)
spend_df['Search_MediaCom_Spend'] = spend_df[['Search_MediaCom_Bing_Spend', 'Search_MediaCom_Google_Spend']].sum(axis=1)
spend_df['Search_IcmDigital_Spend'] = spend_df[['Search_IcmDigital_BingSearch_Spend', 'Search_IcmDigital_GoogleSearch_Spend']].sum(axis=1)
spend_df['Search_ASI_Spend'] = spend_df[['Search_ASI_Bing_Spend', 'Search_ASI_Google_Spend', 'Search_ASI_Unknown_Spend']].sum(axis=1)


spend_cols = ['Search_MembershipDigital_Spend', 'Search_MediaCom_Spend', 'Search_IcmDigital_Spend', 'Search_ASI_Spend']
spend_df = spend_df.groupby('Date')[spend_cols].sum().reset_index()


## Dropping Channel Column ##
ch_name = 'Search_Spend'

## Adding SubChannel level Spend ##
buying_group_model_spend_df = buying_group_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  buying_group_model_spend_df = buying_group_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.9 Adding Social SubModel

# COMMAND ----------

## Reading Dataframe ##

spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_BuyingGroup_Social.csv').drop('Unnamed: 0',axis=1)

spend_df['Social_MembershipDigital_Spend'] = spend_df[['Social_MembershipDigital_Facebook_Spend', 'Social_MembershipDigital_Instagram_Spend', 'Social_MembershipDigital_Linkedin_Spend', 'Social_MembershipDigital_Nextdoor_Spend', 'Social_MembershipDigital_Reddit_Spend', 'Social_MembershipDigital_TikTok_Spend']].sum(axis=1)
spend_df['Social_MediaCom_Spend'] = spend_df[['Social_MediaCom_Meta_Spend', 'Social_MediaCom_TikTok_Spend', 'Social_MediaCom_X_Spend']].sum(axis=1)
spend_df['Social_IcmDigital_Spend'] = spend_df[['Social_IcmDigital_Facebook_Spend']].sum(axis=1)
spend_df['Social_IcmSocial_Spend'] = spend_df[['Social_IcmSocial_FacebookAudienceNetwork_Spend', 'Social_IcmSocial_FacebookMessenger_Spend', 'Social_IcmSocial_Facebook_Spend', 'Social_IcmSocial_Instagram_Spend', 'Social_IcmSocial_LinkedIn_Spend', 'Social_IcmSocial_Pinterest_Spend', 'Social_IcmSocial_TikTok_Spend', 'Social_IcmSocial_Twitter_Spend', 'Social_IcmSocial_messenger_Spend', 'Social_IcmSocial_unknown_Spend']].sum(axis=1)
spend_df['Social_ASI_Spend'] = spend_df[['Social_ASI_Meta_Spend']].sum(axis=1)


spend_cols = ['Social_MembershipDigital_Spend',  'Social_MediaCom_Spend', 'Social_IcmDigital_Spend', 'Social_IcmSocial_Spend', 'Social_ASI_Spend']
spend_df = spend_df.groupby('Date')[spend_cols].sum().reset_index()


## Dropping Channel Column ##
ch_name = 'Social_Spend'

## Adding SubChannel level Spend ##
buying_group_model_spend_df = buying_group_model_spend_df.merge(spend_df, on='Date')


## Assetions ##
sub_ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[spend_cols].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  buying_group_model_spend_df = buying_group_model_spend_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

 cols = ['Affiliate_Spend', 'Audio_Spend', 'DMP_Spend', 'Radio_Spend',
       'OOH_Spend', 'Print_Spend', 'LeadGen_LeadGen_CARDLINKING_Spend',
       'LeadGen_LeadGen_CoRegLeads_Spend', 'LeadGen_LeadGen_Impact_Spend',
       'LeadGen_LeadGen_Linkouts_Spend', 'LeadGen_LeadGen_MajorRocket_Spend',
       'LeadGen_LeadGen_OfferWalls_Spend', 'TV_DRTV_ConnectedTV_Spend',
       'TV_DRTV_LinearTV_Spend', 'TV_MediaCom_BrandnNonBrandTV_Spend',
       'AltMedia_AltMedia_Other_Spend', 'AltMedia_AltMedia_News&Mag_Spend',
       'AltMedia_AltMedia_SharedMailnWrap_Spend',
       'DirectMail_AcqMail_ConnectedTV_Spend',
       'DirectMail_AcqMail_DirectMail_Spend',
       'DirectMail_AcqMail_WinbackMail_Spend',
       'Email_AcqMail_WinbackEmail_Spend', 'Email_LeadGen_Email_Spend',
       'Email_MediaCom_Newsletter_Spend', 'Video_IcmDigital_Native_Spend',
       'Video_IcmDigital_YouTube_Spend', 'Video_MediaCom_OLV_Spend',
       'Video_MediaCom_YouTube_Spend', 'Display_ASI_Display_Spend',
       'Display_IcmDigital_Display_Spend', 'Display_IcmDigital_Other_Spend',
       'Display_MediaCom_Display_Spend',
       'Display_MembershipDigital_AmazonDSP_Spend',
       'Display_MembershipDigital_DV360_Spend',
       'Display_MembershipDigital_Outbrain_Spend',
       'Display_MembershipDigital_Yahoo_Spend',
       'Search_MembershipDigital_Spend', 'Search_MediaCom_Spend',
       'Search_IcmDigital_Spend', 'Search_ASI_Spend',
       'Social_MembershipDigital_Spend', 'Social_MediaCom_Spend',
       'Social_IcmDigital_Spend', 'Social_IcmSocial_Spend',
       'Social_ASI_Spend']
 buying_group_model_spend_df.groupby(pd.to_datetime(buying_group_model_spend_df['Date']).dt.year)[cols].sum().sum(axis=1)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Saving Data

# COMMAND ----------

## Save Dataframe ##
buying_group_model_spend_df.to_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_BuyingGroup_spend_compiled.csv')

## Read Dataframe ##
buying_group_model_spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_BuyingGroup_spend_compiled.csv').drop('Unnamed: 0',axis=1)
buying_group_model_spend_df.display()

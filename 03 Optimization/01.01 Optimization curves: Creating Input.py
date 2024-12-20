# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Reading Contibution and Spend

# COMMAND ----------

## Read contribution Dataframe ##
submodel_camp_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_SubModel_Campaign_results_compiled.csv').drop('Unnamed: 0',axis=1)
submodel_camp_df.display()

# COMMAND ----------

## Read Spend Dataframe ##
submodel_campaign_model_spend_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_SubModel_Campaign_spend_compiled.csv').drop('Unnamed: 0',axis=1)
submodel_campaign_model_spend_df.rename(columns={'Social_IcmDigital_Facebook_Spend':'Social_IcmDigital_Spend', 'Social_ASI_Meta_Spend':'Social_ASI_Spend'}, inplace=True)
submodel_campaign_model_spend_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Creating Bucketed DF

# COMMAND ----------

## Channels to initialize DF ## 
other_contri_cols=['Date', 'Joins', 'Joins_unnorm', 'Pred', 'PromoEvnt_22MembDrive_Eng','PromoEvnt_22BlackFriday_Eng', 'July0422_lag', 'Seasonality_Feb', 'Seasonality_Mar',         
            'Seasonality_Apr', 'Seasonality_Dec', 'PromoEvnt_23MembDrive_Eng', 'PromoEvnt_23BlackFriday_Eng','Fixed Dma Effect']

other_spend_cols = ['Date', 'Affiliate_Spend', 'Audio_Spend', 'DMP_Spend', 'Radio_Spend','OOH_Spend', 'Print_Spend']


## Channels Buckets ##

contri_buckets = { 
                  'LeadGen_Membership':['LeadGen_LeadGen_CARDLINKING', 'LeadGen_LeadGen_CoRegLeads', 'LeadGen_LeadGen_Impact', 'LeadGen_LeadGen_Linkouts', 'LeadGen_LeadGen_MajorRocket', 'LeadGen_LeadGen_OfferWalls'], 
                  'AltMedia_Membership':['AltMedia_AltMedia_Other', 'AltMedia_AltMedia_News&Mag', 'AltMedia_AltMedia_SharedMailnWrap'],
                  'DirectMail_Membership':['DirectMail_AcqMail_DirectMail', 'DirectMail_AcqMail_WinbackMail'],
                  'Email_Membership':['Email_AcqMail_WinbackEmail', 'Email_LeadGen_Email'],
                  'Display_Membership':['Display_MembershipDigital_AmazonDSP', 'Display_MembershipDigital_DV360', 'Display_MembershipDigital_Yahoo'],
                  'Search_Membership':['Search_MembershipDigital_Bing', 'Search_MembershipDigital_GoogleDiscovery', 'Search_MembershipDigital_GooglePerfMax', 'Search_MembershipDigital_GoogleSearch','Search_MembershipDigital_YouTubeTrueView'],
                  'Social_Membership':['Social_MembershipDigital_Facebook', 'Social_MembershipDigital_Instagram', 'Social_MembershipDigital_Linkedin', 'Social_MembershipDigital_Nextdoor', 'Social_MembershipDigital_Reddit', 'Social_MembershipDigital_TikTok'], 
                  'TV_Membership':['TV_DRTV_ConnectedTV_PublisherDirect', 'TV_DRTV_ConnectedTV_Roku', 'TV_DRTV_ConnectedTV_Samsung', 'TV_DRTV_ConnectedTV_Yahoo', 'TV_DRTV_LinearTV_10', 'TV_DRTV_LinearTV_120', 'TV_DRTV_LinearTV_15', 'TV_DRTV_LinearTV_30', 'TV_DRTV_LinearTV_60'], 

                  'Video_ICM':['Video_IcmDigital_Native', 'Video_IcmDigital_YouTube'], 
                  'Display_ICM':['Display_IcmDigital_Display'],
                  'Search_ICM':['Search_IcmDigital_BingSearch', 'Search_IcmDigital_GoogleSearch'], 
                  'Social_ICM':['Social_IcmDigital', 'Social_IcmSocial_FacebookAudienceNetwork', 'Social_IcmSocial_Facebook', 'Social_IcmSocial_Instagram', 'Social_IcmSocial_LinkedIn', 'Social_IcmSocial_TikTok'],


                  'TV_Brand':['TV_MediaCom_BrandTV'], 
                  'Display_Brand':['Display_MediaCom_Display_Brand'],
                  'Search_Brand':['Search_MediaCom_Google_Brand', 'Search_MediaCom_Bing_Brand'], 
                  'Social_Brand':['Social_MediaCom_Meta_Brand', 'Social_MediaCom_TikTok', 'Social_MediaCom_X'],
                  'Video_Brand':['Video_MediaCom_YouTube_Brand', 'Video_MediaCom_OLV_Brand'],

                  'Print_RemainingEM':['Print'],   
                  'TV_RemainingEM':['TV_MediaCom_NonBrandTV'], 
                  'Display_RemainingEM':['Display_MediaCom_Display_NonBrand'], 
                  'Search_RemainingEM':['Search_MediaCom_Google_NonBrand', 'Search_MediaCom_Bing_TargetedCommunities'], 
                  'Social_RemainingEM':['Social_MediaCom_Meta_NonBrand'], 
                  'Video_RemainingEM':['Video_MediaCom_YouTube_NonBrand', 'Video_MediaCom_OLV_NonBrand'],

                  'Display_ASI':['Display_ASI_Display'], 
                  'Social_ASI':['Social_ASI'], 
                  'Search_ASI':['Search_ASI_Bing', 'Search_ASI_Google']

                  }

## Creating Spend Bucket using Contri Bucket ## 
spend_buckets = {k+'_Spend':[ch+'_Spend' for ch in v] for k,v in contri_buckets.items()}
# spend_buckets

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1.1 Creating Contribution DF

# COMMAND ----------

###################################
## Creating Contribution Channel ##
###################################

## Initializing DF ##
contri_df = submodel_camp_df[other_contri_cols]



## Adding Buckets ##
for ch_bucket, ch in contri_buckets.items():
  contri_df[ch_bucket] = submodel_camp_df[ch].sum(axis=1)

contri_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1.2 Saving Contribution DF

# COMMAND ----------

## Save Dataframe ##
contri_df.to_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_contri_df.csv')

## Read Dataframe ##
contri_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_contri_df.csv').drop('Unnamed: 0',axis=1)
contri_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2.1 Creating Spend DF

# COMMAND ----------

###################################
## Creating Spend Channel ##
###################################

## Initializing DF ##
spend_df = submodel_campaign_model_spend_df[other_spend_cols]

## Adding Buckets ##
for ch_bucket, ch in spend_buckets.items():
  spend_df[ch_bucket] = submodel_campaign_model_spend_df[ch].sum(axis=1)

spend_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2.2 Saving Spend DF

# COMMAND ----------

## Save Dataframe ##
spend_df.to_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_spend_df.csv')

## Read Dataframe ##
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_spend_df.csv').drop('Unnamed: 0',axis=1)
spend_df.display()

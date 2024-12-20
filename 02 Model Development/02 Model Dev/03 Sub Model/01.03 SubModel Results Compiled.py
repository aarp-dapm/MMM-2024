# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading Buying Group Model Report

# COMMAND ----------

## Read Agg. Analysis Dataframe ##
buying_group_model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_BuyingGroup_results_compiled.csv').drop('Unnamed: 0',axis=1)
buying_group_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Adding Search MembershipDigital SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Search_MembershipDigital_Bing':	0.102391269,
                  'Search_MembershipDigital_GoogleDiscovery':	0.020060985,
                  'Search_MembershipDigital_GooglePerfMax':	0.055047344,
                  'Search_MembershipDigital_GoogleSearch':	0.783341358,
                  'Search_MembershipDigital_YouTubeTrueView':	0.039159043
                   }

contri_prop_22 = {
                  'Search_MembershipDigital_Bing':	0.097049689,
                  'Search_MembershipDigital_GoogleDiscovery':	0.01552795,
                  'Search_MembershipDigital_GooglePerfMax':	0.012732919,
                  'Search_MembershipDigital_GoogleSearch':	0.849223602,
                  'Search_MembershipDigital_YouTubeTrueView':	0.025465839
                  }



## Dropping Column ##
ch_name = 'Search_MembershipDigital'
submodel_df = buying_group_model_df.copy()

## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##


sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Adding Search MediaCom SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Search_MediaCom_Bing':	0.360613811,
                  'Search_MediaCom_Google':	0.639386189
                   }

contri_prop_22 = {
                  'Search_MediaCom_Bing':	0.337662338,
                  'Search_MediaCom_Google':	0.662337662
                  }



## Dropping Column ##
ch_name = 'Search_MediaCom'


## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##


sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Adding Search IcmDigital SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Search_IcmDigital_BingSearch':	0.430319779,
                  'Search_IcmDigital_GoogleSearch':	0.569680221
                   }

contri_prop_22 = {
                  'Search_IcmDigital_BingSearch':	0.334055459,
                  'Search_IcmDigital_GoogleSearch':	0.665944541
                  }



## Dropping Column ##
ch_name = 'Search_IcmDigital'


## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##


sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Adding Search ASI SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Search_ASI_Bing':	0.033966034,
                  'Search_ASI_Google':	0.966033966                  
                   }

contri_prop_22 = {
                  'Search_ASI_Bing':	0,
                  'Search_ASI_Google':	1
                  }



## Dropping Column ##
ch_name = 'Search_ASI'


## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##


sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Adding Social MembershipDigital SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Social_MembershipDigital_Facebook':	0.868065268,
                  'Social_MembershipDigital_Instagram':	0.094638695,
                  'Social_MembershipDigital_Linkedin':	0,
                  'Social_MembershipDigital_Nextdoor':	0.027039627,
                  'Social_MembershipDigital_Reddit':	0.005128205,
                  'Social_MembershipDigital_TikTok':	0.005128205,               
                   }

contri_prop_22 = {
                  'Social_MembershipDigital_Facebook':	0.883153449,
                  'Social_MembershipDigital_Instagram':	0.048334115,
                  'Social_MembershipDigital_Linkedin':	0.006569686,
                  'Social_MembershipDigital_Nextdoor':	0.056311591,
                  'Social_MembershipDigital_Reddit':	0.003284843,
                  'Social_MembershipDigital_TikTok':	0.002346316,
                 
                  }



## Dropping Column ##
ch_name = 'Social_MembershipDigital'


## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##


sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Adding Social MediaCom SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Social_MediaCom_Meta':	0.714285714,
                  'Social_MediaCom_TikTok':	0.285714286,
                  'Social_MediaCom_X':	0                               
                   }

contri_prop_22 = {
                  'Social_MediaCom_Meta':	0.25,
                  'Social_MediaCom_TikTok':	0,
                  'Social_MediaCom_X':	0.75
                  }



## Dropping Column ##
ch_name = 'Social_MediaCom'


## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##


sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 Adding Social_IcmSocial SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Social_IcmSocial_FacebookAudienceNetwork':	0.00729927,
                  'Social_IcmSocial_Facebook':	0.896593674,
                  'Social_IcmSocial_Instagram':	0.054744526,
                  'Social_IcmSocial_LinkedIn':	0.010948905,
                  'Social_IcmSocial_TikTok':	0.030413625
                   }

contri_prop_22 = {
                  'Social_IcmSocial_FacebookAudienceNetwork':	0.004201681,
                  'Social_IcmSocial_Facebook':	0.966386555,
                  'Social_IcmSocial_Instagram':	0.008403361,
                  'Social_IcmSocial_LinkedIn':	0.008403361,
                  'Social_IcmSocial_TikTok':	0.012605042                  
                  }



## Dropping Column ##
ch_name = 'Social_IcmSocial'


## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##


sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.0 Saving Data

# COMMAND ----------



# COMMAND ----------

## Save Dataframe ##
submodel_df.to_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_submodel_results_compiled.csv')

## Read Dataframe ##
submodel_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_submodel_results_compiled.csv').drop('Unnamed: 0',axis=1)
submodel_df.display()

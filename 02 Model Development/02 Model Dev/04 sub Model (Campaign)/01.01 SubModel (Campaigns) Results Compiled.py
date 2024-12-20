# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading Sub-Channel Report

# COMMAND ----------

submodel_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_submodel_results_compiled.csv').drop('Unnamed: 0',axis=1)
submodel_df .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Adding TV CTV SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'TV_DRTV_ConnectedTV_PublisherDirect':	0.012,
                  'TV_DRTV_ConnectedTV_Roku':	0.644,
                  'TV_DRTV_ConnectedTV_Samsung':	0.338666667,
                  'TV_DRTV_ConnectedTV_Yahoo':	0.005333333
                   }

contri_prop_22 = {
                  'TV_DRTV_ConnectedTV_PublisherDirect':	0.01171459,
                  'TV_DRTV_ConnectedTV_Roku':	0.642172524,
                  'TV_DRTV_ConnectedTV_Samsung':	0.341853035,
                  'TV_DRTV_ConnectedTV_Yahoo':	0.004259851                 
                  }



## Creating Copy of Dataframe ##
ch_name = 'TV_DRTV_ConnectedTV'
submodel_camp_df = submodel_df.copy()

## Adding SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_camp_df[sub_ch] = submodel_camp_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_camp_df = submodel_camp_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Adding TV LTV SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'TV_DRTV_LinearTV_10':	0.002373042,
                  'TV_DRTV_LinearTV_120':	0.010915994,
                  'TV_DRTV_LinearTV_15':	0.010441386,
                  'TV_DRTV_LinearTV_30':	0.435215947,
                  'TV_DRTV_LinearTV_60':	0.541053631
                   }

contri_prop_22 = {
                  'TV_DRTV_LinearTV_10':	0.001967729,
                  'TV_DRTV_LinearTV_120':	0.011019284,
                  'TV_DRTV_LinearTV_15':	0.0094451,
                  'TV_DRTV_LinearTV_30':	0.409681228,
                  'TV_DRTV_LinearTV_60':	0.567886659                 
                  }



## Creating Copy of Dataframe ##
ch_name = 'TV_DRTV_LinearTV'

## Adding SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_camp_df[sub_ch] = submodel_camp_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_camp_df = submodel_camp_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Adding TV Media Com SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'TV_MediaCom_BrandTV':	0.993037975,
                  'TV_MediaCom_NonBrandTV':	0.006962025
                   }

contri_prop_22 = {
                  'TV_MediaCom_BrandTV':	0.999773756,
                  'TV_MediaCom_NonBrandTV':	0.000226244
                  }



## Creating Copy of Dataframe ##
ch_name = 'TV_MediaCom_BrandnNonBrandTV'

## Adding SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_camp_df[sub_ch] = submodel_camp_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_camp_df = submodel_camp_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Adding Display Media Com SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Display_MediaCom_Display_Brand':	0.668751862,
                  'Display_MediaCom_Display_NonBrand':	0.331248138
                   }

contri_prop_22 = {
                  'Display_MediaCom_Display_Brand':	0.621281101,
                  'Display_MediaCom_Display_NonBrand':	0.378718899
                  }



## Creating Copy of Dataframe ##
ch_name = 'Display_MediaCom_Display'

## Adding SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_camp_df[sub_ch] = submodel_camp_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_camp_df = submodel_camp_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Adding Search Google Media Com SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Search_MediaCom_Google_Brand':	0.386243386,
                  'Search_MediaCom_Google_NonBrand':	0.613756614
                   }

contri_prop_22 = {
                'Search_MediaCom_Google_Brand':	0.186147186,
                'Search_MediaCom_Google_NonBrand':	0.813852814
                  }



## Creating Copy of Dataframe ##
ch_name = 'Search_MediaCom_Google'

## Adding SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_camp_df[sub_ch] = submodel_camp_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_camp_df = submodel_camp_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Adding Search Bing Media Com SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Search_MediaCom_Bing_Brand':	0.608870968,
                  'Search_MediaCom_Bing_TargetedCommunities':	0.391129032
                   }

contri_prop_22 = {
                  'Search_MediaCom_Bing_Brand':	0.929133858,
                  'Search_MediaCom_Bing_TargetedCommunities':	0.070866142
                  }



## Creating Copy of Dataframe ##
ch_name = 'Search_MediaCom_Bing'

## Adding SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_camp_df[sub_ch] = submodel_camp_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_camp_df = submodel_camp_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 Adding Social Meta Media Com SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Social_MediaCom_Meta_Brand':	0.862068966,
                  'Social_MediaCom_Meta_NonBrand':	0.137931034
                   }

contri_prop_22 = {
                  'Social_MediaCom_Meta_Brand':	0.837837838,
                  'Social_MediaCom_Meta_NonBrand':	0.162162162
                  }



## Creating Copy of Dataframe ##
ch_name = 'Social_MediaCom_Meta'

## Adding SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_camp_df[sub_ch] = submodel_camp_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_camp_df = submodel_camp_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8 Adding Video Youtube Media Com SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Video_MediaCom_YouTube_Brand':	0.96108291,
                  'Video_MediaCom_YouTube_NonBrand':	0.03891709
                   }

contri_prop_22 = {
                  'Video_MediaCom_YouTube_Brand':	0.959770115,
                  'Video_MediaCom_YouTube_NonBrand':	0.040229885
                  }



## Creating Copy of Dataframe ##
ch_name = 'Video_MediaCom_YouTube'

## Adding SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_camp_df[sub_ch] = submodel_camp_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_camp_df = submodel_camp_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.9 Adding Video OLV Media Com SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Video_MediaCom_OLV_Brand':	0.748479428,
                  'Video_MediaCom_OLV_NonBrand':	0.251520572
                   }

contri_prop_22 = {
                  'Video_MediaCom_OLV_Brand':	0.748908297,
                  'Video_MediaCom_OLV_NonBrand':	0.251091703
                  }



## Creating Copy of Dataframe ##
ch_name = 'Video_MediaCom_OLV'

## Adding SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_camp_df[sub_ch] = submodel_camp_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_camp_df.groupby(pd.to_datetime(submodel_camp_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_camp_df = submodel_camp_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Saving Data

# COMMAND ----------

## Save Dataframe ##
submodel_camp_df .to_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_SubModel_Campaign_results_compiled.csv')

## Read Dataframe ##
submodel_camp_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_SubModel_Campaign_results_compiled.csv').drop('Unnamed: 0',axis=1)
submodel_camp_df .display()

# COMMAND ----------



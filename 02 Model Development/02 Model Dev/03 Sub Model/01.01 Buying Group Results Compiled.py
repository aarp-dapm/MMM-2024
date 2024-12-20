# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading Initial Model Report

# COMMAND ----------

## Read Agg. Analysis Dataframe ##
initial_model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_initialmodel_results.csv').drop('Unnamed: 0',axis=1)
initial_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Adding Leadgen SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'LeadGen_LeadGen_CARDLINKING':	0.339261594, 
                   'LeadGen_LeadGen_CoRegLeads':	0.007975815, 
                   'LeadGen_LeadGen_Impact':	0.32134817, 
                   'LeadGen_LeadGen_Linkouts':	0.015790828,
                   'LeadGen_LeadGen_MajorRocket':	0.238309642, 
                   'LeadGen_LeadGen_OfferWalls':	0.077313951
                   }

contri_prop_22 = {
                  'LeadGen_LeadGen_CARDLINKING':	0.335115865,
                  'LeadGen_LeadGen_CoRegLeads':	0.046725073,
                  'LeadGen_LeadGen_Impact':	0.105245193,
                  'LeadGen_LeadGen_Linkouts':	0.078165889,
                  'LeadGen_LeadGen_MajorRocket':	0.090947017,
                  'LeadGen_LeadGen_OfferWalls':	0.343800963
                  }



## Dropping Leadgen Column ##
ch_name = 'LeadGen'
submodel_df = initial_model_df.copy()

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
# MAGIC ## 1.2 Adding TV SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'TV_DRTV_ConnectedTV':	0.086250125,
                  'TV_DRTV_LinearTV':	0.420820379,
                  'TV_MediaCom_BrandnNonBrandTV':	0.492929496
                   }

contri_prop_22 = {
                'TV_DRTV_ConnectedTV':	0.194006849,
                'TV_DRTV_LinearTV':	0.562671233,
                'TV_MediaCom_BrandnNonBrandTV':	0.243321918
                  }



## Dropping Leadgen Column ##
ch_name = 'TV'

## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Adding Alt-Media SubModel

# COMMAND ----------

contri_prop_23 = { 
                  
                  'AltMedia_AltMedia_Other': 0.024959742,
                  'AltMedia_AltMedia_News&Mag':	0.526301664,
                  'AltMedia_AltMedia_SharedMailnWrap':	0.448738594

               
                   }

contri_prop_22 = {

                  'AltMedia_AltMedia_Other':	0.031890661,
                  'AltMedia_AltMedia_News&Mag':	0.426980511,
                  'AltMedia_AltMedia_SharedMailnWrap':	0.541128828

    
                  }



## Dropping Leadgen Column ##
ch_name = 'AltMedia'

## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Adding Direct Mail SubModel

# COMMAND ----------

contri_prop_23 = { 
                 'DirectMail_AcqMail_DirectMail':	0.515113636,
                 'DirectMail_AcqMail_WinbackMail':	0.484886364
                   }

contri_prop_22 = {
                  'DirectMail_AcqMail_DirectMail':	0.565650051,
                  'DirectMail_AcqMail_WinbackMail':	0.434349949
                  }



## Dropping Leadgen Column ##
ch_name = 'DirectMail'

## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.5 Adding Email SubModel

# COMMAND ----------

contri_prop_23 = { 
                'Email_AcqMail_WinbackEmail':	0.048243758,
                'Email_LeadGen_Email':	0.951756242

                   }

contri_prop_22 = {
                  'Email_AcqMail_WinbackEmail':	0.097146739,
                  'Email_LeadGen_Email':	0.902853261
                  }



## Dropping Leadgen Column ##
ch_name = 'Email'

## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.6 Adding Video SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Video_IcmDigital_Native':	0.089316988,
                  'Video_IcmDigital_YouTube':	0.022985989,
                  'Video_MediaCom_OLV':	0.728765324,
                  'Video_MediaCom_YouTube':	0.158931699
                   }

contri_prop_22 = {
                  'Video_IcmDigital_Native':	0.078500481,
                  'Video_IcmDigital_YouTube':	0.020666453,
                  'Video_MediaCom_OLV':	0.746555591,
                  'Video_MediaCom_YouTube':	0.154277475
                  }



## Dropping Leadgen Column ##
ch_name = 'Video'

## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.7 Adding Display SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Display_ASI_Display':	0.058202022,
                  'Display_IcmDigital_Display':	0.075507788,
                  'Display_MediaCom_Display':	0.501001913,
                  'Display_MembershipDigital_AmazonDSP':	0.006239184,
                  'Display_MembershipDigital_DV360':	0.321705073,
                  'Display_MembershipDigital_Yahoo':	0.03734402
                   }

contri_prop_22 = {
                  'Display_ASI_Display':	0.083613061,
                  'Display_IcmDigital_Display':	0.097395992,
                  'Display_MediaCom_Display':	0.483114637,
                  'Display_MembershipDigital_AmazonDSP':	0.032651816,
                  'Display_MembershipDigital_DV360':	0.213813447,
                  'Display_MembershipDigital_Yahoo':	0.089411047
                  }



## Dropping Leadgen Column ##
ch_name = 'Display'

## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.8 Adding Search SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Search_MembershipDigital':	0.63073138,
                  'Search_MediaCom':	0.033102214,
                  'Search_IcmDigital':	0.253634534,
                  'Search_ASI':	0.082531872
                  
                   }

contri_prop_22 = {
                'Search_MembershipDigital':	0.639060025,
                'Search_MediaCom':	0.027459954,
                'Search_IcmDigital':	0.246259461,
                'Search_ASI':	0.08722056                  
                  }



## Dropping Leadgen Column ##
ch_name = 'Search'

## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.9 Adding Social SubModel

# COMMAND ----------

contri_prop_23 = { 
                  'Social_MembershipDigital':	0.63757764,
                  'Social_MediaCom':	0.020807453,
                  'Social_IcmDigital':	0.006832298,
                  'Social_IcmSocial':	0.245962733,
                  'Social_ASI':	0.088819876
                   }

contri_prop_22 = {
                'Social_MembershipDigital':	0.596165368,
                'Social_MediaCom':	0.030257639,
                'Social_IcmDigital':	0.006590773,
                'Social_IcmSocial':	0.261533853,
                'Social_ASI':	0.105452367    
                  }



## Dropping Leadgen Column ##
ch_name = 'Social'

## Adding Leadgen SubChannels ##
for sub_ch, prop in contri_prop_23.items():
  submodel_df[sub_ch] = submodel_df[['Date', ch_name]].apply(lambda x: x[ch_name]*prop if pd.to_datetime(x['Date']).year==2023 else ( x[ch_name]*contri_prop_22[sub_ch] if pd.to_datetime(x['Date']).year==2022 else 0), axis=1)

## Assetions ##

## Check 1 : If SubChannels Decomposition is equal to Channel level Value ##
sub_ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[list(contri_prop_23.keys())].sum().sum(axis=1)).rename(columns={0:ch_name})
ch_df = pd.DataFrame(submodel_df.groupby(pd.to_datetime(submodel_df['Date']).dt.year)[ch_name].sum())

try:
  assert_frame_equal(sub_ch_df, ch_df)
  submodel_df = submodel_df.drop(columns=ch_name)
except AssertionError:
  print('Assertion Failed')

# COMMAND ----------

## Save Dataframe ##
submodel_df .to_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_BuyingGroup_results_compiled.csv')

## Read Dataframe ##
submodel_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_BuyingGroup_results_compiled.csv').drop('Unnamed: 0',axis=1)
submodel_df .display()

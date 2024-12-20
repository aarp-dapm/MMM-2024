# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading Model Reports

# COMMAND ----------

## Loading Initial Model ##

cols = ['Joins', 'Joins_unnorm', 'Pred', 'PromoEvnt_22MembDrive_Eng', 'PromoEvnt_22BlackFriday_Eng', 'July0422_lag', 'Seasonality_Feb', 'Seasonality_Mar', 'Seasonality_Apr', 'Seasonality_Dec', 'PromoEvnt_23MembDrive_Eng', 'PromoEvnt_23BlackFriday_Eng', 'TV_Imps_AdStock6L2Wk70Ad_Power70', 'Search_Clicks_AdStock6L1Wk90Ad_Power90', 'Social_Imps_AdStock6L1Wk90Ad_Power90', 'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'Email_Spend_AdStock6L1Wk20Ad_Power90', 'DirectMail_Imps_AdStock6L3Wk70Ad_Power90', 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90', 'Fixed Dma Effect']

main_model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_main_model_results.csv')
main_model_df = main_model_df.groupby('Date')[cols].sum().reset_index()

## Creating a new df with calibrated Joins ##
report_df = main_model_df[['Date','Joins', 'Joins_unnorm', 'Pred', 'PromoEvnt_22MembDrive_Eng', 'PromoEvnt_22BlackFriday_Eng', 'July0422_lag', 'Seasonality_Feb', 'Seasonality_Mar', 'Seasonality_Apr', 'Seasonality_Dec', 'PromoEvnt_23MembDrive_Eng', 'PromoEvnt_23BlackFriday_Eng', 'Fixed Dma Effect']]
report_df['TV'] = main_model_df['TV_Imps_AdStock6L2Wk70Ad_Power70']
report_df['Search'] = main_model_df['Search_Clicks_AdStock6L1Wk90Ad_Power90']
report_df['Social'] = main_model_df['Social_Imps_AdStock6L1Wk90Ad_Power90']
report_df['LeadGen'] = main_model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80']
report_df['Email'] = main_model_df['Email_Spend_AdStock6L1Wk20Ad_Power90']
report_df['DirectMail'] = main_model_df['DirectMail_Imps_AdStock6L3Wk70Ad_Power90']
report_df['AltMedia'] = main_model_df['AltMedia_Imps_AdStock6L3Wk90Ad_Power90']


report_df.display()

# COMMAND ----------

## Loading Leadgen Submodel ##

cols = [ 'Joins', 'Pred', 'TV_Imps_AdStock6L2Wk70Ad_Power70', 'DirectMail_Imps_AdStock6L3Wk70Ad_Power90', 'Display_Imps_AdStock6L1Wk80Ad_Power40', 'Print_Imps_AdStock13L1Wk30Ad_Power30', 'AltMedia_Imps_AdStock6L5Wk30Ad_Power70', 'Seasonality_Mar23', 'Seasonality_Apr23', 'Seasonality_Sep23', 'Seasonality_Oct23', 'Fixed Dma Effect']


leadgen_submodel_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_Leadgen_submodel_results.csv')
leadgen_submodel_df = leadgen_submodel_df.groupby('Date')[cols].sum().reset_index()

## Adding Column to Calibrate ##
leadgen_submodel_df = leadgen_submodel_df.merge(main_model_df[['Date', 'LeadGen_Imps_AdStock6L1Wk60Ad_Power80']].rename(columns={'LeadGen_Imps_AdStock6L1Wk60Ad_Power80': 'LeadGen_Calibration'}), on='Date', how='left')

## Creating Df with calibrated Joins ##
calibrated_leadgen_submodel_df = leadgen_submodel_df[['Date','LeadGen_Calibration']]
calibrated_leadgen_submodel_df['Year'] = pd.to_datetime(calibrated_leadgen_submodel_df['Date']).dt.year ## Adding Year Column ##


##########################
## Distributing Numbers ##
##########################

calibrated_leadgen_submodel_df['LeadGen'] = leadgen_submodel_df[['Seasonality_Mar23', 'Seasonality_Apr23', 'Seasonality_Sep23', 'Seasonality_Oct23', 'Fixed Dma Effect']].sum(axis=1)
calibrated_leadgen_submodel_df['TV'] = leadgen_submodel_df['TV_Imps_AdStock6L2Wk70Ad_Power70']
calibrated_leadgen_submodel_df['DirectMail'] = leadgen_submodel_df['DirectMail_Imps_AdStock6L3Wk70Ad_Power90']
calibrated_leadgen_submodel_df['Display'] = leadgen_submodel_df['Display_Imps_AdStock6L1Wk80Ad_Power40']
calibrated_leadgen_submodel_df['Print'] = leadgen_submodel_df['Print_Imps_AdStock13L1Wk30Ad_Power30']
calibrated_leadgen_submodel_df['AltMedia'] = leadgen_submodel_df['AltMedia_Imps_AdStock6L5Wk30Ad_Power70']

### Normalization Dataframe ###
grouped_df = calibrated_leadgen_submodel_df.groupby('Year')[['LeadGen', 'TV', 'DirectMail', 'Display', 'Print', 'AltMedia']].sum()
normalized_df = grouped_df.div(grouped_df.sum(axis=1), axis=0)


for col in ['LeadGen', 'TV', 'DirectMail', 'Display', 'Print','AltMedia']:
  calibrated_leadgen_submodel_df[col] = calibrated_leadgen_submodel_df[['Year','LeadGen_Calibration']].apply(lambda x: x.LeadGen_Calibration*normalized_df.loc[2022][col] if x.Year==2022 else x.LeadGen_Calibration*normalized_df.loc[2023][col], axis=1)

calibrated_leadgen_submodel_df.display()

# COMMAND ----------

## Loading Search Submodel ##
cols = ['Joins', 'Pred', 'Trend', 'Social_Imps_norm', 'TV_Imps_AdStock6L3Wk90Ad_Power30', 'Video_Imps_AdStock6L4Wk30Ad_Power30', 'Fixed Dma Effect']

search_submodel_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_Search_submodel_results.csv')
search_submodel_df = search_submodel_df.groupby('Date')[cols].sum().reset_index()

## Adding Column to Calibrate ##
search_submodel_df = search_submodel_df.merge(main_model_df[['Date', 'Search_Clicks_AdStock6L1Wk90Ad_Power90']].rename(columns={'Search_Clicks_AdStock6L1Wk90Ad_Power90':'Search_Calibration'}), on='Date', how='left')

## Creating Df with calibrated Joins ##
calibrated_search_submodel_df = search_submodel_df[['Date','Search_Calibration']]
calibrated_search_submodel_df['Year'] = pd.to_datetime(calibrated_search_submodel_df['Date']).dt.year ## Adding Year Column ##

##########################
## Distributing Numbers ##
##########################

calibrated_search_submodel_df['Search'] = search_submodel_df[['Trend','Fixed Dma Effect']].sum(axis=1)
calibrated_search_submodel_df['Social'] = search_submodel_df['Social_Imps_norm']
calibrated_search_submodel_df['TV'] = search_submodel_df['TV_Imps_AdStock6L3Wk90Ad_Power30']
calibrated_search_submodel_df['Video'] = search_submodel_df['Video_Imps_AdStock6L4Wk30Ad_Power30']

### Normalization Dataframe ###
grouped_df = calibrated_search_submodel_df.groupby('Year')[['Search', 'Social', 'TV', 'Video']].sum()
normalized_df = grouped_df.div(grouped_df.sum(axis=1), axis=0)

for col in ['Search', 'Social', 'TV', 'Video']:
  calibrated_search_submodel_df[col] = calibrated_search_submodel_df[['Year','Search_Calibration']].apply(lambda x: x.Search_Calibration*normalized_df.loc[2022][col] if x.Year==2022 else x.Search_Calibration*normalized_df.loc[2023][col], axis=1)

calibrated_search_submodel_df.head(100).display()


# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Calibration

# COMMAND ----------

## Merging Leadgen with Main Model ##
report_df['LeadGen'] = calibrated_leadgen_submodel_df['LeadGen']
report_df['Display'] = calibrated_leadgen_submodel_df['Display']
report_df['Print'] = calibrated_leadgen_submodel_df['Print']

report_df['TV'] = report_df['TV'] + calibrated_leadgen_submodel_df['TV']
report_df['DirectMail'] = report_df['DirectMail'] + calibrated_leadgen_submodel_df['DirectMail']
report_df['AltMedia'] = report_df['AltMedia'] + calibrated_leadgen_submodel_df['AltMedia']


## Merging Search with Main Model ##
report_df['Search'] = calibrated_search_submodel_df['Search']
report_df['Video'] = calibrated_search_submodel_df['Video']

report_df['Social'] = report_df['Social'] + calibrated_search_submodel_df['Social']
report_df['TV'] = report_df['TV'] + calibrated_search_submodel_df['TV']


#######################
## Upscaling Numbers ##
#######################

## Calculating Scalar to upscale values ##
scalar = report_df.groupby(pd.to_datetime(report_df['Date']).dt.year)[['Joins', 'Joins_unnorm']].sum().reset_index()
scalar_22 = scalar.iloc[0]['Joins_unnorm']/scalar.iloc[0]['Joins']
scalar_23 = scalar.iloc[1]['Joins_unnorm']/scalar.iloc[1]['Joins']

## Adding Year Column ##
report_df['Year'] = pd.to_datetime(report_df['Date']).dt.year


cols = ['Pred', 'PromoEvnt_22MembDrive_Eng', 'PromoEvnt_22BlackFriday_Eng', 'July0422_lag', 'Seasonality_Feb', 'Seasonality_Mar', 'Seasonality_Apr', 'Seasonality_Dec', 'PromoEvnt_23MembDrive_Eng', 'PromoEvnt_23BlackFriday_Eng', 'Fixed Dma Effect', 'TV', 'Search', 'Social', 'LeadGen', 'Email', 'DirectMail', 'AltMedia', 'Display', 'Print', 'Video']

final_report_df = report_df[['Date', 'Joins', 'Joins_unnorm']] 
for col in cols:
  final_report_df[col] = report_df[['Year',col]].apply(lambda x: x*scalar_22 if x.Year==2022 else x*scalar_23, axis=1)[col]

final_report_df.display()

# COMMAND ----------

## Save Dataframe ##
final_report_df.to_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_initialmodel_results.csv')

## Read Dataframe ##
final_report_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_initialmodel_results.csv').drop('Unnamed: 0',axis=1)
final_report_df.display()

# COMMAND ----------

## Read Dataframe ##
final_report_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_initialmodel_results.csv').drop('Unnamed: 0',axis=1)
final_report_df.columns

# COMMAND ----------

 'Seasonality_Feb','Seasonality_Mar', 'Seasonality_Apr', 'Seasonality_Dec','Fixed Dma Effect'

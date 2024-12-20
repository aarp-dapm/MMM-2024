# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0 Reading Data and Model

# COMMAND ----------

#################
##### Data #####
#################


## Reading Data ##
# model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_statsmodel.csv').drop('Unnamed: 0',axis=1)
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_TV_SubModal_df.csv').drop('Unnamed: 0',axis=1)
model_df.head()


# COMMAND ----------

#################
##### Model #####
#################


## Reading Model ##
model_path =  model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo16_updatedData.pkl'

with open(model_path, "rb") as f:
  model = pickle.load(f)

## Model Vars ##
input_vars = list(model.params.index)
input_vars.remove('Intercept')
input_vars.remove('DmaCode Var')


#Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.0 Model Dev

# COMMAND ----------

# ############################
# ### Creating Variables #####
# #############################


# ## Creating Search Target Variable ##
# coeff = 0.037
# model_df['TV_contri'] = model_df['TV_Imps_AdStock6L2Wk70Ad_Power70']*coeff

# ## Re-Normalize variable ##
# model_df['TV_contri_norm'] = model_df['TV_contri'].transform(min_max_normalize)



# ###################################################################################################
# ################################## Special Variable Transformation ################################
# ###################################################################################################

# ### Splitting Leadgen ###
# model_df['Date'] = pd.to_datetime(model_df['Date']) 
# mask_2022 = model_df['Date'].dt.year==2022

# ### Splitting Leadgen ###
# var = 'LeadGen_Imps_AdStock6L1Wk60Ad_Power80'
# model_df[f'{var}_2022'] = model_df[var]*mask_2022
# model_df[f'{var}_2023'] = model_df[var]*(1-mask_2022)

# ## Min max Scaling Target Variable ##
# # model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2022'] = model_df.groupby('DmaCode')['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2022'].transform(min_max_normalize)
# # model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2023'] = model_df.groupby('DmaCode')['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2023'].transform(min_max_normalize)


# ### Splitting Email ###
# var = 'Email_Imps_AdStock6L1Wk60Ad_Power30'
# model_df[f'{var}_2022'] = model_df[var]*mask_2022
# model_df[f'{var}_2023'] = model_df[var]*(1-mask_2022)

# ### SPlitting Display ###
# var = 'Display_Imps_AdStock6L4Wk30Ad_Power30'
# model_df[f'{var}_2022'] = model_df[var]*mask_2022
# model_df[f'{var}_2023'] = model_df[var]*(1-mask_2022)

# ### Creating New Variables ###
# model_df['Social_Imps_norm'] = model_df.groupby('DmaCode')['Social_Imps'].transform(min_max_normalize)
# model_df['Search_Clicks_norm'] = model_df.groupby('DmaCode')['Search_Clicks'].transform(min_max_normalize)

# COMMAND ----------

## Adding Trend to it ##
model_df['Trend'] = model_df.sort_values(by='Date').groupby('DmaCode').cumcount() + 1

## Adding Seasonality variables ##


# COMMAND ----------

## Saving File ##
# model_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_TV_SubModal_df.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Model Creation

# COMMAND ----------

model_df[['Date','TV_contri_norm']].groupby("Date")['TV_contri_norm'].sum().reset_index().sort_values(by='Date').display()

# COMMAND ----------

plt.figure(figsize=(15,6))
plt.plot(model_df[['Date','TV_contri_norm']].groupby("Date")['TV_contri_norm'].sum().reset_index().sort_values(by='Date')['TV_contri_norm'])
plt.show()

# COMMAND ----------

input_vars = [ 
              
       'Trend',
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
       # 'Display_Imps_AdStock6L1Wk80Ad_Power40',

       # 'PromoEvnt_22MembDrive_Eng',
       # 'PromoEvnt_22BlackFriday_Eng',
       # 'July0422_lag',
       'Seasonality_Jan',
       'Seasonality_Feb',
       'Seasonality_Mar',
       'Seasonality_Apr',
       'Seasonality_May',
       'Seasonality_Jun',
       'Seasonality_Jul',
       # 'Seasonality_Aug',
       'Seasonality_Sep',
       'Seasonality_Oct',
       # 'Seasonality_Nov',
       'Seasonality_Dec',
       # 'PromoEvnt_23MembDrive_Eng',
       # 'PromoEvnt_23BlackFriday_Eng',

       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("TV_contri_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

#Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

## Saving Model ##
model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_submodel_TV.pkl'
with open(model_path,"wb") as f:
  pickle.dump(model, f)


# ## Reading Model ##
# model_path = 
# with open(model_path, "rb") as f:
#   model = pickle.load(f)

# COMMAND ----------

## Creating Model Predict ##
joins = model_df[['DmaCode', 'Date','TV_contri_norm']]
pred = model.fittedvalues.rename('Pred')

if 'Joins' not in joins.columns:
  joins.rename(columns = {joins.columns[-1]:'Joins'},inplace=True)

pred_df = pd.concat([joins, pred],axis=1)

pred_df = pd.concat([joins, pred],axis=1)

# Aggregate the data by 'Date'
pred_df_date = pred_df.groupby('Date').agg({'Joins':'sum', 'Pred':'sum'}).reset_index()

## Plots ##
plt.figure(figsize=(25,6))
plt.plot( pred_df_date.Joins, label='Actual')
plt.plot( pred_df_date.Pred, label='Predicted')

plt.axvline(x=0, color='r')
plt.axvline(x=51, color='r')
plt.axvline(x=103, color='r')

# Calculate and display residuals
residuals = pred_df_date.Joins - pred_df_date.Pred
# Create a figure with two subplots (1 row, 2 columns)
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(25, 8))

# Plot residuals over time
ax1.scatter(pred_df_date.Date, residuals, label='Residuals')
ax1.axhline(y=0, color='r', linestyle='--')
ax1.set_xlabel('Date')
ax1.set_ylabel('Residuals')
ax1.set_title('Residuals Over Time')

ax1.axvline(x=0, color='r')
ax1.axvline(x=51, color='r')
ax1.axvline(x=103, color='r')



ax1.legend()

# Plot histogram of residuals
ax2.hist(residuals, bins=30, edgecolor='black')
ax2.set_xlabel('Residuals')
ax2.set_ylabel('Frequency')
ax2.set_title('Histogram of Residuals')


# Calculate and display mean squared error (MSE) and R-squared
mape = mean_absolute_percentage_error(pred_df_date.Joins, pred_df_date.Pred)
r_squared = r2_score(pred_df_date.Joins, pred_df_date.Pred)
skewness = skew(residuals)
kurt = kurtosis(residuals)
dw_stat = sm.stats.stattools.durbin_watson(residuals)


print(f"Mean Absolute Percentage Error (MAPE): {mape}")
print(f"R-squared: {r_squared}")
print(f"Skewness: {skewness}")
print(f"Kurtosis: {kurt}")
print(f"DW Stat: {dw_stat}")
print(f"Condition Number: {np.linalg.cond(model.model.exog)}")


plt.tight_layout()
plt.show()

# COMMAND ----------

#Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

## VIF ##
X = model_df[input_vars]
# X = sm.add_constant(X) ## Check if there is no intercept

## Creating 
vif_data = pd.DataFrame()
vif_data["feature"] = X.columns
vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]

vif_data.round(2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.0 Analyzing Residuals 

# COMMAND ----------

## Getting Residuals ##
pred_df_date['Residuals'] = pred_df_date['Joins'] - pred_df_date['Pred']
pred_df_date.display()

# COMMAND ----------

plt.figure(figsize=(20,8))
plt.plot(pred_df_date['Residuals'])

plt.axhline(y=0, color='r', linestyle='--')
plt.axvline(x=51, color='r')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 ACF and PACF plots for Residuals

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

# Assuming your DataFrame is named model_df and the time series column is named 'time_series_column'
time_series = pred_df_date['Residuals']

fig, ax = plt.subplots(2, 1, figsize=(20, 8))

# Plot ACF
plot_acf(time_series, ax=ax[0], lags=60, alpha=0.05)
ax[0].set_title('Autocorrelation Function (ACF)')

# Plot PACF
plot_pacf(time_series, ax=ax[1], lags=40, method='ywm')
ax[1].set_title('Partial Autocorrelation Function (PACF)')

plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Test for Stationarity of Residuals

# COMMAND ----------

import pandas as pd
from statsmodels.tsa.stattools import adfuller, kpss

# Assuming you have a DataFrame 'df' with a column 'residuals'
residuals = pred_df_date['Residuals']

# Augmented Dickey-Fuller test     
'''
The null hypothesis of the ADF test is that the series has a unit root (i.e., it is non-stationary).

'''
adf_result = adfuller(residuals)
print('ADF Statistic:', adf_result[0])
print('p-value:', adf_result[1])
for key, value in adf_result[4].items():
    print('Critical Value (%s): %.3f' % (key, value))

# Kwiatkowski-Phillips-Schmidt-Shin test

'''

The null hypothesis of the KPSS test is that the series is stationary.

'''



kpss_result = kpss(residuals, regression='c')
print('\nKPSS Statistic:', kpss_result[0])
print('p-value:', kpss_result[1])
for key, value in kpss_result[3].items():
    print('Critical Value (%s): %.3f' % (key, value))


# COMMAND ----------

## Data ##
# analysis_df = model_df.drop(columns = ['DmaCode']).groupby('Date').sum().reset_index()

# COMMAND ----------

# ## Plots ##
# fig, ax1 = plt.subplots(1,1, figsize = (25,6))

# ax1.plot( pred_df_date.Joins.values, label='Actual')
# ax1.plot( pred_df_date.Pred.values, label='Predicted')


# ax2 = ax1.twinx()
# # ax2.plot(analysis_df['Seasonality_Sin5'][:].values, linestyle= '--', color ='g', label = 'Sin Wave') ## Phased Out totally
# # ax2.plot(analysis_df['Seasonality_Cos5'][:].values, linestyle= '--', color ='r', label = 'Cos Wave')

# # ax2.plot(analysis_df['Seasonality_Sin6'][:].values, linestyle= '--', color =(0.8 , 0.2,0.5), label = 'Sin Wave') ## Not capturing early year seasonlaity
# ax2.plot(analysis_df['Seasonality_Cos6'][:].values, linestyle= '--', color =(1 , 0.2,1), label = 'Cos Wave')

# ax2.set_ylim(3500,-3500)
# ax2.legend()


# ax1.axvline(x=0, color='r')
# ax1.axvline(x=51, color='r')
# ax1.axvline(x=103, color='r')

# COMMAND ----------

# vars = ['TV_Imps_AdStock6L2Wk60Ad_Power60', 'Search_Clicks_AdStock6L1Wk90Ad_Power30', 'Social_Imps_AdStock6L2Wk90Ad_Power90']
# analysis_df = model_df.groupby(['Date', 'DmaCode']+vars).sum().reset_index()

# # Increase overall figure size
# p
# sns.pairplot(analysis_df[vars])

# COMMAND ----------

# model_df[vars].corr()

# COMMAND ----------

# MAGIC %md
# MAGIC # 5.0 Saving Contribution File 

# COMMAND ----------

# while True:
#   var=1
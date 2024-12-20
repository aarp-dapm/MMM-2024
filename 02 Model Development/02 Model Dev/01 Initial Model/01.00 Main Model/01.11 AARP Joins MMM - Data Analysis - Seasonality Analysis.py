# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Reading Data

# COMMAND ----------

####################
### Reading Data ###
####################

model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_statsmodel.csv').drop('Unnamed: 0',axis=1)


#######################
### Model Variables ###
#######################
input_vars = [ 
       'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
       'PromoEvnt_23RollingStone',

       'PromoEvnt_23MembDrive',



       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
       'Search_Clicks_AdStock6L1Wk80Ad_Power70',
       'Social_Imps_AdStock6L1Wk60Ad_Power90',
       'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'
       ]


seasonality_terms = [ 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin5', 'Seasonality_Cos5', 'Seasonality_Sin6', 'Seasonality_Cos6', 'Seasonality_Sin7', 'Seasonality_Cos7', 'Seasonality_Sin8', 'Seasonality_Cos8', 'Seasonality_Sin9', 'Seasonality_Cos9', 'Seasonality_Sin10', 'Seasonality_Cos10']




#################
### Agg. Data ###
#################
analysis_df = model_df[['Date', 'Joins_norm', 'Joins']+input_vars+seasonality_terms].groupby('Date').sum()
analysis_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.1 YoY Plot

# COMMAND ----------


# Generate week numbers
weeks = pd.date_range(start='2022-01-01', periods=52, freq='W-MON')

# month_start_weeks = {month: week for month, week in zip(weeks.month_name().unique(), range(0, 52, 4))}
month_start_weeks = {'January': 0,
 'February': 5,
 'March': 9,
 'April': 13,
 'May': 17,
 'June': 22,
 'July': 26,
 'August': 30,
 'September': 35,
 'October': 39,
 'November': 44,
 'December': 48}




fig, ax = plt.subplots(1,1,figsize = (20,8))
ax.plot(analysis_df['Joins'][:52].values, label = '2022')
ax.plot(analysis_df['Joins'][52:].values, label = '2023')



ax.set_xticks(range(52))
ax.set_xticklabels([date.strftime('%b') for date in weeks], rotation=45)
# Add vertical lines for each month
for month, week in month_start_weeks.items():
    ax.axvline(x=week, color='green', linestyle='--', alpha=0.5)



ax.grid('True')
ax.legend()
plt.show()

# COMMAND ----------


fig, ax = plt.subplots(1,1,figsize = (20,8))
ax.plot(analysis_df['Joins'][:52].values, label = '2022')
ax.plot(analysis_df['Joins'][52:].values, label = '2023')
ax.set_xticks(range(52))
ax.grid('True')

ax2 = ax.twinx()

ax2.plot(analysis_df['Seasonality_Sin8'][52:].values, linestyle= '--', color ='g', label = 'Sin Wave')
ax2.plot(analysis_df['Seasonality_Cos8'][52:].values, linestyle= '--', color ='r', label = 'Cos Wave')
# ax2.plot(analysis_df['Seasonality_Sin6'][52:].values, linestyle= '--', color ='g', label = 'Sin Wave')
# ax2.plot(analysis_df['Seasonality_Cos6'][52:].values, linestyle= '--', color ='r', label = 'Cos Wave')
ax2.set_ylim(3500,-3500)

ax.legend()
ax2.legend(loc='upper left')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.2 Seasonal Decompose by Statsmodel

# COMMAND ----------

import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose
import matplotlib.pyplot as plt

# Assuming 'df' is your DataFrame with a datetime index
result = seasonal_decompose(analysis_df['Joins'], model='additive', period=13)

trend = result.trend
seasonal = result.seasonal
residual = result.resid

# Plotting the components
plt.figure(figsize=(12, 8))
plt.subplot(411)
plt.plot(analysis_df['Joins'], label='Original')
plt.legend(loc='best')
plt.subplot(412)
plt.plot(trend, label='Trend')
plt.legend(loc='best')
plt.subplot(413)
plt.plot(seasonal, label='Seasonality')
plt.legend(loc='best')
plt.subplot(414)
plt.plot(residual, label='Residuals')
plt.legend(loc='best')
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # Plot ACF PCF plots for given Joins

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

# Assuming your DataFrame is named model_df and the time series column is named 'time_series_column'
time_series = analysis_df['Joins']

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



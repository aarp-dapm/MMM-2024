# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Reading Data

# COMMAND ----------

## Reaqding Data ##
rawdata_df=spark.sql('select * from temp.ja_blend_mmm2_DateDmaAge_ChModelDf').toPandas()

## Archive: Creating CrossSection at DMA and Age Bucket level ##
# rawdata_df.insert(1, 'DmaAge',rawdata_df['DmaCode']+ "-" + rawdata_df['AgeBucket'])
# rawdata_df.drop(['DmaCode', 'AgeBucket'], axis=1, inplace=True)

## Creating CrossSection at DMA level ##
rawdata_df.drop(['AgeBucket'], axis=1, inplace=True)
rawdata_df = rawdata_df.groupby(['Date', 'DmaCode']).sum().reset_index()
rawdata_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Column Classification

# COMMAND ----------

## General Classification ##
primary_cols = ['DmaCode', 'Date']
target_col = ['Joins']
other_col = ['Reg', 'BenRefAll']

spend_col = ['Affiliate_Spend', 'AltMedia_Spend', 'Audio_Spend', 'DMP_Spend', 'DirectMail_Spend', 'Display_Spend', 'Email_Spend', 'LeadGen_Spend', 'Radio_Spend', 'OOH_Spend', 'Print_Spend', 'Search_Spend','Social_Spend', 'TV_Spend', 'Video_Spend']

imps_col = ['Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps', 'LeadGen_Imps',
       'Radio_Imps', 'OOH_Imps', 'Print_Imps', 'Search_Imps', 'Social_Imps', 'TV_Imps', 'Video_Imps']

clicks_col = ['Affiliate_Clicks', 'AltMedia_Clicks', 'Audio_Clicks', 'DMP_Clicks', 'DirectMail_Clicks', 'Display_Clicks', 'Email_Clicks', 'LeadGen_Clicks', 'Radio_Clicks', 'OOH_Clicks', 'Print_Clicks', 'Search_Clicks', 'Social_Clicks', 'TV_Clicks', 'Video_Clicks']

HnP_col = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow',
       'PromoEvnt_22TikTok', 'Holiday_22Christams', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23EvdaySavCamp', 'PromoEvnt_23RollingStone',
       'PromoEvnt_23MemorialDay', 'PromoEvnt_23LaborDay', 'PromoEvnt_23BlackFriday', 'Holiday_23Christams']

seasonality_col = ['Seasonality_Sin1', 'Seasonality_Cos1','Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin3', 'Seasonality_Cos3', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6']

Brand_col = ['Index', 'Buzz', 'Impression', 'Quality', 'Value', 'Reputation', 'Satisfaction', 'Recommend',
       'Awareness', 'Attention', 'Ad_Awareness', 'WOM_Exposure', 'Consideration', 'Purchase_Intent', 'Current_Customer', 'Former_Customer']



## Model Columns ##
model_col = ['Date', 'Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps', 'LeadGen_Imps', 'Radio_Spend', 'OOH_Imps', 'Print_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps']

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Model Data

# COMMAND ----------

analysis_df = rawdata_df.drop(columns = ['DmaCode']).groupby('Date').sum().reset_index()
# analysis_df = rawdata_df.drop(columns = ['DmaCode'])
analysis_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.0 Tranformation Section

# COMMAND ----------

plt.figure(figsize=(20,6))
plt.plot(analysis_df['Joins'])


######################################
############# 2022 ###################
######################################

## Q1 Membership Drive ##
plt.axvline(x=8, color=(0.5, 0.2, 0.8))
plt.axvline(x=9, color=(0.5, 0.2, 0.8))
plt.axvline(x=10, color=(0.5, 0.2, 0.8), label= 'Membership Drive')

## Q2 Memorial Day ##
plt.axvline(x=19, color=(0.0, 0.2, 0.8))
plt.axvline(x=20, color=(0.0, 0.2, 0.8))
plt.axvline(x=21, color=(0.0, 0.2, 0.8), label = 'Memorial Day')

## Q3 Labor Day ##
plt.axvline(x=33, color=(1.0, 0.2, 0.8))
plt.axvline(x=34, color=(1.0, 0.2, 0.8))
plt.axvline(x=35, color=(1.0, 0.2, 0.8))
plt.axvline(x=36, color=(1.0, 0.2, 0.8), label = 'Labor Day')

## Q3 Black Friday ##
plt.axvline(x=45, color=(0.5, 0.2, 1.0))
# plt.axvline(x=46, color=(0.5, 0.2, 1.0))
plt.axvline(x=47, color=(0.5, 0.2, 1.0))
plt.axvline(x=48, color=(0.5, 0.2, 1.0), label = 'Black Friday')

## WnJ Today Evnt ##
plt.axvline(x=42, linestyle='--', color='g', label = 'WnJ Today')

## TikTok Video ##
plt.axvline(x=46, linestyle='--', color=(0.5, 0.2, 1.0), label = 'TikTok Video')


######################################
############# 2023 ###################
######################################

## Everyday Saving Campaigns ##
plt.axvline(x=55, color=(0.5, 0.2, 0.0), label = 'Everyday Saving and Kayla Coupons')

## Kayla Coupons Campaigns ##
# plt.axvline(x=55, color='g')

## R0lling Stones Campaigns ##
plt.axvline(x=98, color='g')
plt.axvline(x=99, color='g', label = 'ROlling Stones')



## Yearly Flag ##
plt.axvline(x=0, color='r')
plt.axvline(x=51, color='r')
plt.axvline(x=103, color='r')

plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1), shadow=True, ncol=8)
plt.show()

# COMMAND ----------

## Creating Dfs ##
spend_df = analysis_df[['Date','Joins']+spend_col]
imps_df = analysis_df[['Date','Joins']+imps_col]
click_df = analysis_df[['Date','Joins']+clicks_col]
model_support_df = analysis_df[['Joins']+model_col]

joins_df = analysis_df[['Date', 'Joins']]
Reg_df = analysis_df[['Date', 'Joins', 'Reg']]
brand_df = analysis_df[['Date','Joins']+Brand_col]


# COMMAND ----------

## Utility Function ##
def get_corr(df):

  if 'Date' in df.columns:
    df = df.drop(columns=['Date'])

  correlation_matrix = df.corr().sort_values(by='Joins', ascending=False)

  # Create a heatmap
  plt.figure(figsize=(15, 6))
  sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap='coolwarm')
  plt.title('Correlation Matrix')
  plt.show()

# COMMAND ----------

## check spend correlation ##
get_corr(spend_df)

'''
Comments: 

High Correlation Channels:
*Leadgen has high correlation: As it is pay per conversion channel it is expected to have it high correlation
*Email has also very high correlation, Acq Mail and Leadgen Email are clubbed, Leadgen email might be driving high correlation
*Search is expected to have correlation as it is down the funnel channel

Mid Corrleation Channels:
* DMP Spend: SURPRISING, although it has very low spend
* Social has okkayish correlation as expected.

Low correlation Channels:
* TV, Radio have ~9% of correlation
* Affiliate, Display, OOH has very low correlation
* AltMedia, Direct Mail have very low corrleation: They should be transformed in such a way so that they have some positive correlation
* Audio, Print, Video are from Media Com expected to have lower correlation. Find their transformation which has posiitve correlation with Brand Metric and use that in model
'''

# COMMAND ----------

## get imps correlation ##
get_corr(imps_df)

'''
Comments: 

High Correlation Channels:
* Email, Leadgen has high corrleation, pay per conversion is the suspected reason for Leadegn (Since email has leadgen email clubbed in hence the reason)
* Surprisingly DISPLAY imps have high correlation
* TV has both membership and brand awareness imps, have decent correlation.
* DMP needs INVESTIGATION.
* Social Imps have okkayish correlation
* Video, Audio, Print, OOH are part of Brand awareness and have low correlation which is expected.
* Affiliate is from ICM Digital
* Alt Media and Direct Mail will need some special attention. Coz their correlations are very low.
'''

# COMMAND ----------

## get imps correlation ##
get_corr(model_support_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plots

# COMMAND ----------

while True:
  val=1

# COMMAND ----------

import matplotlib.pyplot as plt


model_col = ['Date',  'AltMedia_Imps', 'DirectMail_Imps', 'TV_Imps', 'Social_Imps', 'Search_Clicks', 'LeadGen_Imps', 'Email_Spend']

# Create a figure
fig, ax = plt.subplots(nrows=8, ncols=1, figsize=(20, 30))

# Plot 'Joins' on the primary y-axis of the first subplot
ax[0].plot(analysis_df['Joins'], label='Joins', color='blue')
ax[0].set_title('Joins')
ax[0].set_ylabel('Joins')
ax[0].legend(loc='upper left')

## Q1 Membership Drive ##
ax[0].axvline(x=8, color=(0.5, 0.2, 0.8))
ax[0].axvline(x=9, color=(0.5, 0.2, 0.8))
ax[0].axvline(x=10, color=(0.5, 0.2, 0.8), label= 'Membership Drive')


# Create a secondary y-axis for the first subplot
ax1_2 = ax[0].twinx()

# Plot the remaining columns on the secondary y-axis of the first subplot
# for idx, col in enumerate(model_col[1:]):
#     ax1_2.plot(analysis_df[col], label=col)
# ax1_2.set_ylabel('Values')
# ax1_2.legend(loc='upper right')

# Plot the remaining columns in the other subplots
for idx, col in enumerate(model_col[1:]):

    ax[idx + 1].plot(analysis_df['Joins'], label='Joins', color='blue')



###############################################################################################################################
###############################################################################################################################
###############################################################################################################################

    ######################################
    ############# 2022 ###################
    ######################################

    ## Q1 Membership Drive ##
    ax[idx + 1].axvline(x=8, color=(0.5, 0.2, 0.8))
    ax[idx + 1].axvline(x=9, color=(0.5, 0.2, 0.8))
    ax[idx + 1].axvline(x=10, color=(0.5, 0.2, 0.8), label= 'Membership Drive')

    ## Q2 Memorial Day ##
    ax[idx + 1].axvline(x=19, color=(0.0, 0.2, 0.8))
    ax[idx + 1].axvline(x=20, color=(0.0, 0.2, 0.8))
    ax[idx + 1].axvline(x=21, color=(0.0, 0.2, 0.8), label = 'Memorial Day')

    ## Q3 Labor Day ##
    ax[idx + 1].axvline(x=33, color=(1.0, 0.2, 0.8))
    ax[idx + 1].axvline(x=34, color=(1.0, 0.2, 0.8))
    ax[idx + 1].axvline(x=35, color=(1.0, 0.2, 0.8))
    ax[idx + 1].axvline(x=36, color=(1.0, 0.2, 0.8), label = 'Labor Day')

    ## Q3 Black Friday ##
    ax[idx + 1].axvline(x=45, color=(0.5, 0.2, 1.0))
    # plt.axvline(x=46, color=(0.5, 0.2, 1.0))
    ax[idx + 1].axvline(x=47, color=(0.5, 0.2, 1.0))
    ax[idx + 1].axvline(x=48, color=(0.5, 0.2, 1.0), label = 'Black Friday')

    ## WnJ Today Evnt ##
    ax[idx + 1].axvline(x=42, linestyle='--', color='g', label = 'WnJ Today')

    ## TikTok Video ##
    ax[idx + 1].axvline(x=46, linestyle='--', color=(0.5, 0.2, 1.0), label = 'TikTok Video')


    ######################################
    ############# 2023 ###################
    ######################################

    ## Everyday Saving Campaigns ##
    ax[idx + 1].axvline(x=55, color=(0.5, 0.2, 0.0), label = 'Everyday Saving and Kayla Coupons')

    ## Kayla Coupons Campaigns ##
    # ax[idx + 1].axvline(x=55, color='g')

    ## R0lling Stones Campaigns ##
    ax[idx + 1].axvline(x=98, color='g')
    ax[idx + 1].axvline(x=99, color='g', label = 'ROlling Stones')



    ## Yearly Flag ##
    ax[idx + 1].axvline(x=0, color='r')
    ax[idx + 1].axvline(x=51, color='r')
    ax[idx + 1].axvline(x=103, color='r')

    



###############################################################################################################################
###############################################################################################################################
###############################################################################################################################


    ax2 = ax[idx + 1].twinx()
    ax2.plot(analysis_df[col], label=col, color='orange')
    ax[idx + 1].set_title(f'Joins and {col}')
    ax[idx + 1].set_ylabel('Joins')
    ax2.set_ylabel(col)
    ax[idx + 1].legend(loc='upper left')
    ax2.legend(loc='upper right')
    ax[idx + 1].legend(loc='upper center', bbox_to_anchor=(0.5, -0.1), shadow=True, ncol=8)

# Adjust the layout and spacing
plt.tight_layout()
plt.subplots_adjust(hspace=0.5)  # Increase space between plots

plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC # YoY Plot

# COMMAND ----------


fig, ax = plt.subplots(1,1,figsize = (15,6))
ax.plot(analysis_df['Joins'][:52].values, label = '2022')
ax.plot(analysis_df['Joins'][52:].values, label = '2023')


ax2 = ax.twinx()
ax2.plot(analysis_df['Seasonality_Sin6'][52:].values, linestyle= '--', color ='g', label = 'Sin Wave')
ax2.plot(analysis_df['Seasonality_Cos6'][52:].values, linestyle= '--', color ='r', label = 'Cos Wave')
ax2.set_ylim(3500,-3500)

ax.legend()
ax2.legend()
plt.show()

# COMMAND ----------

'''
Selecting Sin5, Cos5 and Sin6, Cos6 for seasonality
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pair Plot

# COMMAND ----------

model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_statsmodel.csv').drop('Unnamed: 0',axis=1)

# COMMAND ----------

input_vars = [

       'PromoEvnt_22MembDrive_Eng',
'PromoEvnt_22BlackFriday_Eng',

'July0422_lag',
'Seasonality_Feb', 
'Seasonality_Mar', 
'Seasonality_Apr',
'Seasonality_Dec',

'PromoEvnt_23MembDrive_Eng', 
'PromoEvnt_23BlackFriday_Eng',

'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk90Ad_Power90',
'Social_Imps_AdStock6L1Wk90Ad_Power90',


'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'Email_Spend_AdStock6L1Wk20Ad_Power90',

'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'

       ]


# COMMAND ----------

vars = ['TV_Imps_AdStock6L2Wk70Ad_Power70', 'Search_Clicks_AdStock6L1Wk90Ad_Power90', 'Social_Imps_AdStock6L1Wk90Ad_Power90', 'LeadGen_Imps_AdStock6L1Wk60Ad_Power80', 'Email_Spend_AdStock6L1Wk20Ad_Power90',
'DirectMail_Imps_AdStock6L3Wk70Ad_Power90', 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90', 'Joins']

analysis_df = model_df[['Date', 'DmaCode']+vars].groupby(['Date'])[vars].sum().reset_index()
analysis_df = analysis_df.rename(columns= {'AltMedia_Imps_AdStock6L3Wk90Ad_Power90':'AltMedia','DirectMail_Imps_AdStock6L3Wk70Ad_Power90':'DirectMail','TV_Imps_AdStock6L2Wk70Ad_Power70':'TV', 'Search_Clicks_AdStock6L1Wk90Ad_Power90':'Search', 'Social_Imps_AdStock6L1Wk90Ad_Power90':'Social', 'LeadGen_Imps_AdStock6L1Wk60Ad_Power80':'Leadgen', 'Email_Spend_AdStock6L1Wk20Ad_Power90': 'Email'})

rename_vars = ['AltMedia', 'DirectMail', 'TV', 'Search', 'Social', 'Leadgen', 'Email', 'Joins']

sns.pairplot(analysis_df[rename_vars])

# COMMAND ----------

corr = analysis_df[rename_vars].corr()

plt.figure(figsize=(10,8))
sns.heatmap(corr, annot=True, cmap='coolwarm')


'''

* Leadgen has visibily very high correlation with target variable
* Leadgen has very poor corrleation with Search and Social
* Intrestingly Leadgen has high correlarion with Direct Mail (0.15) and TV (0.27). Leadgen is dominantly Digital sort of Display channel. Since Leadgen is Pay per conversion channel, and given high correlation with TV does it mean Leadgen capturing TV conversions?
* SOcial has high correlation with Search , moderate with Target Variable. I dont think so Social and TV has same flighting pattern

'''

# COMMAND ----------

# MAGIC %md
# MAGIC # 5.0 Investigating Leadgen

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Pair Plots

# COMMAND ----------

sns.pairplot(analysis_df[['Joins', 'Leadgen', 'TV', 'DirectMail', 'AltMedia']])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.2 Line Chart

# COMMAND ----------

fig, ax = plt.subplots(1,1, figsize = (15,6))

ax.plot(analysis_df['Leadgen'], label= 'Leadgen', color = 'g')
ax.legend(loc='upper left')

ax.axvline(x=0, color = 'r', linestyle= '--')
ax.axvline(x=51, color = 'r', linestyle= '--')
ax.axvline(x=103, color = 'r', linestyle= '--')

ax1 = ax.twinx()
ax1.plot(analysis_df['Joins'], label = 'Joins')
ax1.legend()

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.3 Investigating Seasonality

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

import pandas as pd
import prophet

model = prophet.Prophet()

# COMMAND ----------

import pandas as pd
import prophet

# Assuming 'df' is your DataFrame with a datetime index and numeric column 'your_column'
df = analysis_df[['Date', 'Joins']].rename(columns={'Date':'ds', 'Joins':'y'})

# Initialize Prophet model
model = prophet.Prophet()

# Fit the model
model.fit(df)

# Make future predictions for decomposition
future = model.make_future_dataframe(periods=0)
forecast = model.predict(future)

# The forecast DataFrame contains the trend, seasonality, and residuals components
trend = forecast['trend']
seasonal = forecast['seasonal']
residuals = forecast['yhat'] - forecast['trend'] - forecast['seasonal']

# Alternatively, use 'additive_terms' which includes all seasonalities (weekly, yearly, etc.)
additive_terms = forecast['additive_terms']
residuals = forecast['yhat'] - forecast['trend'] - additive_terms


import matplotlib.pyplot as plt

# Plot trend
plt.figure(figsize=(10, 6))
plt.plot(df['ds'], df['y'], label='Original')
plt.plot(forecast['ds'], trend, label='Trend')
plt.legend()
plt.show()

# Plot seasonal
plt.figure(figsize=(10, 6))
plt.plot(forecast['ds'], seasonal, label='Seasonal')
plt.legend()
plt.show()

# Plot residuals
plt.figure(figsize=(10, 6))
plt.plot(forecast['ds'], residuals, label='Residuals')
plt.legend()
plt.show()



# COMMAND ----------

forecast

# COMMAND ----------



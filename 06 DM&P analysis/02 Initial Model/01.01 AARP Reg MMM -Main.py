# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading Data

# COMMAND ----------

## Reading Model ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_reg_df_BuyingGroup.csv').drop('Unnamed: 0',axis=1)

## Re-Ordering DataFrame ##
reorder_df = model_df[['DmaCode', 'Date',  'Joins', 'Reg', 'BenRefAll']]
model_df = model_df.drop(columns=['DmaCode', 'Date',  'Joins', 'Reg', 'BenRefAll'])


## Merge Re-Ordered Df ##
model_df = reorder_df.join(model_df)
model_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 EDA and Variable transformation

# COMMAND ----------

## Utility Function ##
def get_corr(df):

  if 'Date' in df.columns:
    df = df.drop(columns=['Date'])

  correlation_matrix = df.corr().sort_values(by='Reg', ascending=False)

  # Create a heatmap
  plt.figure(figsize=(25, 12))
  sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap='coolwarm')
  plt.title('Correlation Matrix')
  plt.show()
  return correlation_matrix

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 EDA

# COMMAND ----------

####################################
##### Creating Agg. Dataframe ######
####################################



## Selecting columns ##
primary_cols = ['DmaCode', 'Date']
target_col = ['Joins', 'Reg', 'BenRefAll']
model_cols = ['TV_Imps', 'Search_Clicks', 'Social_Imps', 'LeadGen_Imps', 'Email_Spend', 'DirectMail_Imps', 'AltMedia_Imps', 'Display_Imps', 'Video_Imps' , 'Print_Imps']


Mem_cols = [
'Email_AcqMail_Spend',
'Email_LeadGen_Spend',
'AltMedia_AltMedia_Imps',
'DirectMail_AcqMail_Imps',
'Display_MembershipDigital_Imps',
'Social_MembershipDigital_Imps',
'Search_MembershipDigital_Clicks',
'LeadGen_LeadGen_Imps',
'TV_DRTV_Imps'
]

OtherEM_cols = [
'Email_MediaComOtherEM_Spend',
'Radio_MediaComOtherEM_Spend',
'Audio_MediaComOtherEM_Imps',
'Display_MediaComOtherEM_Imps',
'OOH_MediaComOtherEM_Imps',
'Print_MediaComOtherEM_Imps',
'Social_MediaComOtherEM_Imps',
'TV_MediaComOtherEM_Imps',
'Video_MediaComOtherEM_Imps',
'Search_MediaComOtherEM_Clicks'
]


IcmDigital_cols = [
'Affiliate_IcmDigital_Imps',
'DMP_IcmDigital_Imps',
'Display_IcmDigital_Imps',
'Social_IcmDigital_Imps',
'Video_IcmDigital_Imps',
'Search_IcmDigital_Clicks'
]


AarpBrandSocial_cols = [
'Social_IcmSocial_Imps'
]

MediaComBrand_cols = [
'Audio_MediaComBrand_Imps',
'Display_MediaComBrand_Imps',
'Social_MediaComBrand_Imps',
'TV_MediaComBrand_Imps',
'Video_MediaComBrand_Imps',
'Search_MediaComBrand_Clicks'
]


asi_cols = [
'Display_ASI_Imps',
'Social_ASI_Imps',
'Search_ASI_Clicks'
]


analysis_df = model_df.groupby('Date')[target_col+model_cols+Mem_cols+OtherEM_cols+IcmDigital_cols+AarpBrandSocial_cols+MediaComBrand_cols+asi_cols].sum().reset_index()
analysis_df.display()

# COMMAND ----------

## Plotting Registrations ##
plt.figure(figsize=(20,6))
plt.plot(analysis_df['Reg'], label = 'Reg', color='red')

ax2 = plt.twinx()
ax2.plot(analysis_df['Joins'], label = 'Joins')

## Adding markers for Yeara and Months ##

## Yearly ##
plt.axvline(x=51, color = 'g', linestyle='--')



## Adding Legen ##
plt.legend(['Reg'], loc='upper right')
ax2.legend(['Joins'], loc='upper right')
plt.show()


# COMMAND ----------

## Quarterly Correlations ##

## Creating Year and Quarter columns ##
analysis_df['Year'] = pd.to_datetime(analysis_df['Date']).dt.year
analysis_df['Quarter'] = pd.to_datetime(analysis_df['Date']).dt.quarter

## calcualting correlation for each quarter ##
corr_df = analysis_df.groupby(['Year' ,'Quarter']).apply(lambda x: x['Reg'].corr(x['Joins']) ).reset_index()
corr_df['Yr-Qt'] = corr_df['Year'].astype('str') +" Q" + corr_df['Quarter'].astype('str')


## Plot ##
plt.figure(figsize=(20,6))
plt.bar(corr_df['Yr-Qt'], corr_df[0])
plt.show()

# COMMAND ----------

## Plotting Benefit Referral Clicks ##
plt.figure(figsize=(20,6))
plt.plot(analysis_df['Reg'], label = 'Reg', color='red')

ax2 = plt.twinx()
ax2.plot(analysis_df['BenRefAll'], label = 'BenRefAll')

## Adding markers for Years and Months ##

## Yearly ##
plt.axvline(x=51, color = 'g', linestyle='--')



## Adding Legen ##
plt.legend(['Reg'], loc='upper right')
ax2.legend(['BenRefAll'], loc='upper right')
plt.show()


# COMMAND ----------

## Quarterly Correlations ##

## Creating Year and Quarter columns ##
analysis_df['Year'] = pd.to_datetime(analysis_df['Date']).dt.year
analysis_df['Quarter'] = pd.to_datetime(analysis_df['Date']).dt.quarter

## calcualting correlation for each quarter ##
corr_df = analysis_df.groupby(['Year' ,'Quarter']).apply(lambda x: x['BenRefAll'].corr(x['Joins']) ).reset_index()
corr_df['Yr-Qt'] = corr_df['Year'].astype('str') +" Q" + corr_df['Quarter'].astype('str')


## Plot ##
plt.figure(figsize=(20,6))
plt.bar(corr_df['Yr-Qt'], corr_df[0])
plt.show()

# COMMAND ----------

## Plotting Registrations ##
plt.figure(figsize=(20,6))
plt.plot(analysis_df['Reg'], label = 'Reg')



## Adding markers for Yeara and Months ##

## Yearly ##
plt.axvline(x=51, color = 'g', linestyle='--')

## Monthly ##

plt.axvline(x=0, color='y', linestyle='--')
plt.axvline(x=4, color='y', linestyle='--')
plt.axvline(x=56, color='y', linestyle='--')  ## Jan ##

plt.axvline(x=8, color='y', linestyle='--')
plt.axvline(x=60, color='y', linestyle='--')  ## Feb ##

plt.axvline(x=12, color='y', linestyle='--')
plt.axvline(x=64, color='y', linestyle='--')  ## Mar ##

plt.axvline(x=16, linestyle='--', color=(0.5, 0.2, 0.8))
plt.axvline(x=68,linestyle='--', color=(0.5, 0.2, 0.8))  ## Apr ##

plt.axvline(x=21, linestyle='--', color=(0.5, 0.2, 0.8))
plt.axvline(x=73, linestyle='--', color=(0.5, 0.2, 0.8))  ## may ##

plt.axvline(x=25, linestyle='--', color=(0.5, 0.2, 0.8))
plt.axvline(x=77, linestyle='--', color=(0.5, 0.2, 0.8))  ## Jun ##

plt.axvline(x=29, linestyle='--', color='g')
plt.axvline(x=82, linestyle='--', color='g')  ## July ##

plt.axvline(x=34, linestyle='--', color='g')
plt.axvline(x=86, linestyle='--', color='g')  ## Aug ##

plt.axvline(x=38, linestyle='--', color='g')
plt.axvline(x=90, linestyle='--', color='g')  ## Sept ##

plt.axvline(x=43, linestyle='--', color=(1.0, 0.2, 0.8))
plt.axvline(x=95, linestyle='--', color=(1.0, 0.2, 0.8))  ## Oct ##

plt.axvline(x=47,  linestyle='--', color=(1.0, 0.2, 0.8))
plt.axvline(x=99,  linestyle='--', color=(1.0, 0.2, 0.8))  ## Nov ##

plt.axvline(x=51, linestyle='--', color=(1.0, 0.2, 0.8))
plt.axvline(x=103,linestyle='--', color=(1.0, 0.2, 0.8))  ## Dec ##

plt.show()

# COMMAND ----------

### YoY Analysis ###
plt.figure(figsize=(20,6))

plt.plot(analysis_df['Reg'][:52].values, label = 'Reg 2022')
plt.plot(analysis_df['Reg'][52:].values, label = 'Reg 2023')


## Adding Monthly Markers ##
plt.axvline(x=0, color='y', linestyle='--')
plt.axvline(x=4, color='y', linestyle='--')  ## Jan ##

plt.axvline(x=8, color='y', linestyle='--')  ## Feb ##

plt.axvline(x=12, color='y', linestyle='--') ## Mar ##

plt.axvline(x=16, linestyle='--', color=(0.5, 0.2, 0.8))  ## Apr ##

plt.axvline(x=21, linestyle='--', color=(0.5, 0.2, 0.8))  ## may ##

plt.axvline(x=25, linestyle='--', color=(0.5, 0.2, 0.8))  ## Jun ##

plt.axvline(x=29, linestyle='--', color='g')  ## July ##

plt.axvline(x=34, linestyle='--', color='g')  ## Aug ##

plt.axvline(x=38, linestyle='--', color='g')  ## Sept ##

plt.axvline(x=43, linestyle='--', color=(1.0, 0.2, 0.8))  ## Oct ##

plt.axvline(x=47,  linestyle='--', color=(1.0, 0.2, 0.8))  ## Nov ##

plt.axvline(x=51, linestyle='--', color=(1.0, 0.2, 0.8)) ## Dec ##

plt.legend(['2022', '2023'], loc='upper right')

plt.show()

# COMMAND ----------

np.corrcoef(analysis_df['Reg'][:52].values, analysis_df['Reg'][52:].values)

# COMMAND ----------

## Correlation Matrix ##
get_corr(analysis_df[['Joins', 'Reg']])

# COMMAND ----------

### Media and Registrations ##
get_corr(analysis_df[['Reg', 'Joins']+model_cols])


'''
Majorly driven by Lower funnel channels

'''

# COMMAND ----------

### Media and Registrations for filtered date ramge##
get_corr(analysis_df[analysis_df['Date']>'2022-08-01'][['Reg', 'Joins']+model_cols])

# COMMAND ----------

### Media and Registrations for filtered date ramge##
t = get_corr(analysis_df[analysis_df['Date']>'2022-08-01'][['Reg']+Mem_cols+OtherEM_cols+IcmDigital_cols+AarpBrandSocial_cols+MediaComBrand_cols+asi_cols])


# COMMAND ----------

t.reset_index().display()

# COMMAND ----------

## Correlation with Lags of Reg. and Joins ##

### Lag for one Month ### 
analysis_df['Reg_lag1'] = analysis_df['Reg'].shift(1)
analysis_df['Reg_lag2'] = analysis_df['Reg'].shift(2)
analysis_df['Reg_lag3'] = analysis_df['Reg'].shift(3)
analysis_df['Reg_lag4'] = analysis_df['Reg'].shift(4)


### Lag for two Month ### 
analysis_df['Reg_lag5'] = analysis_df['Reg'].shift(5)
analysis_df['Reg_lag6'] = analysis_df['Reg'].shift(6)
analysis_df['Reg_lag7'] = analysis_df['Reg'].shift(7)
analysis_df['Reg_lag8'] = analysis_df['Reg'].shift(8)



## Getting all cols ##
corr_cols = ['Reg', 'Reg_lag1', 'Reg_lag2', 'Reg_lag3', 'Reg_lag4', 'Reg_lag5', 'Reg_lag6', 'Reg_lag7', 'Reg_lag8', 'Joins']
get_corr(analysis_df[corr_cols])

# COMMAND ----------

## Correlation with Lags of Joins and Reg ##

### Lag for one Month ### 
analysis_df['Joins_lag1'] = analysis_df['Joins'].shift(1)
analysis_df['Joins_lag2'] = analysis_df['Joins'].shift(2)
analysis_df['Joins_lag3'] = analysis_df['Joins'].shift(3)
analysis_df['Joins_lag4'] = analysis_df['Joins'].shift(4)


### Lag for two Month ### 
analysis_df['Joins_lag5'] = analysis_df['Joins'].shift(5)
analysis_df['Joins_lag6'] = analysis_df['Joins'].shift(6)
analysis_df['Joins_lag7'] = analysis_df['Joins'].shift(7)
analysis_df['Joins_lag8'] = analysis_df['Joins'].shift(8)



## Getting all cols ##
corr_cols = ['Reg', 'Joins', 'Joins_lag1', 'Joins_lag2', 'Joins_lag3', 'Joins_lag4', 'Joins_lag5', 'Joins_lag6', 'Joins_lag7', 'Joins_lag8']
get_corr(analysis_df[corr_cols])

# COMMAND ----------

############################################
### Effect of Promos and Holidays on Reg ###
############################################


plt.figure(figsize=(20, 6))
plt.plot(analysis_df['Reg'], label='Registrations', color='blue')


############
### 2022 ###
############

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


############
### 2023 ###
############

## Everyday Saving Campaigns ##
plt.axvline(x=55, color=(0.5, 0.2, 0.0), label = 'Everyday Saving and Kayla Coupons')


## Q2 Memorial Day ##
plt.axvline(x=72, color=(0.0, 0.2, 0.8))
plt.axvline(x=73, color=(0.0, 0.2, 0.8), label = 'Memorial Day')

## Q3 Labor Day ##
plt.axvline(x=86, color=(1.0, 0.2, 0.8))
plt.axvline(x=87, color=(1.0, 0.2, 0.8), label = 'Labor Day')

## Q3 Black Friday ##
plt.axvline(x=98, color=(0.5, 0.2, 1.0))
plt.axvline(x=99, color=(0.5, 0.2, 1.0), label = 'Black Friday')

## Kayla Coupons Campaigns ##
# ax[idx + 1].axvline(x=55, color='g')

## R0lling Stones Campaigns ##
plt.axvline(x=98, color='g')
plt.axvline(x=99, color='g', label = 'ROlling Stones')




###################
### Other Falgs ###
###################

## Yearly Flag ##
plt.axvline(x=0, color='r')
plt.axvline(x=51, color='r')
plt.axvline(x=103, color='r')

## Legend ##
plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.1), shadow=True, ncol=8)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.1.01 Seasonality Analaysis 

# COMMAND ----------

## Reg Correlations from 2022 and 2023 ##
reg_corr = np.corrcoef(analysis_df['Reg'][:52].values, analysis_df['Reg'][52:].values)[0,1]
join_corr = np .corrcoef(analysis_df['Joins'][:52].values, analysis_df['Joins'][52:].values)[0,1]
benrefall_corr = np.corrcoef(analysis_df['BenRefAll'][:52].values, analysis_df['BenRefAll'][52:].values)[0,1]



print("Registration correlation is ", reg_corr)
print("Join correlation is ", join_corr)
print("BenRef correlation is ", benrefall_corr)

# COMMAND ----------

## Time Series Decomposition ##
from statsmodels.tsa.seasonal import seasonal_decompose

result = seasonal_decompose(analysis_df['Reg'], model='additive', period=52)
result.plot()


'''

Doesn't looks like there is a seaosonality in data

'''

# COMMAND ----------

#############################
## Trying Fourier Features ##
#############################

## FFT Analysis ##
y = analysis_df["Reg"]

## Deocmposing the signal ##
coeff = np.fft.fft(y)
N = len(coeff)

## Storing each frequency of Signal ##
freq_sig = {}
n = np.arange(N)

### Getting Top K Indices ###
k = 10
top_k_coeff_ind = np.argsort(np.abs(coeff))[-k:][::-1]
top_k_coeff_ind

# COMMAND ----------

## Creating Fourier Features ##

def ff(index, n , order):
  
  time = np.arange(len(index), dtype=np.float32)
  k = 2*np.pi*(1/n)*time
  features = {}
  
  for i in order:
    
    features.update({f"Seasonality_Sin{i}": np.sin(i*k), f"Seasonality_Cos{i}": np.cos(i*k),})
    
  return pd.DataFrame(features, index=index)


import statsmodels.api as sm

order = [7,2,9,3,1]
index = analysis_df['Date'].values
n = len(y[:104])

ff_df = ff(index, n, order)
ff_df = ff_df.join(analysis_df[[ 'Date','Reg']].set_index('Date'))

ff_df.display()

## Plot ###

# COMMAND ----------

## Plot ##
plt.figure(figsize=(20,10))

plt.plot(ff_df['Reg'])

ax = plt.twinx()
ax.plot(ff_df['Seasonality_Cos3'], color = 'g', linestyle='--', label = 'Cos3')
ax.plot(ff_df['Seasonality_Cos7'], color = 'r', linestyle='--', label = 'Cos7')
ax.set_ylim(-3,3)

ax.legend(loc='upper right')
plt.axvline(x=51, linestyle='--', color = 'r')
plt.show()

# COMMAND ----------

## Correlation between Seasonal Features and Target Variable ##
get_corr(ff_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Adding New Data 

# COMMAND ----------

### Reading Data ###
pe_df = read_table("default.priortyengagement20222023_csv")

## Filtering PE ##
filter_cols = [
'Games', 'AARP Job Board', 'AARP Rewards', 'AARP Skills Builder', 'AARP Money Map', 'Online Community', 'State Events', 'Movies for Grownups: Event Registration',
'State Volunteer, Digital Fraud Fighter, Virtual Veterans Brigade, Wish of a Lifetime Volunteer, Disrupt Aging Classroom', 'Volunteer: Submit an Interest Form', 'On membership FAQ Page'
]

pe_df = pe_df.filter(f.col('PE_Name').isin(filter_cols))

## Reducing Data to DMA level ##
pe_df = pe_df.withColumn('UniqueVisitors', f.col('UniqueVisitors')/210)

### Pivoting Dataset ###
pe_df = pe_df.groupBy('Date').pivot('PE_Name').sum('UniqueVisitors')
pe_df = pe_df.fillna(0)
pe_df  = pe_df.select([f.col(col).alias(col.replace(' ', '_')) for col in pe_df.columns])

## Row Sum ##
pe_df = pe_df.withColumn('PE_Total', reduce(lambda a, b: a + b, [f.col(c) for c in pe_df.columns if c != 'Date']))

## Merging ##
pe_df = pe_df.toPandas()
model_df = model_df.merge(pe_df, on='Date', how='left')
# model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Variable Transformation

# COMMAND ----------

## Min max Scaling Target Variable ##
model_df['Reg_norm'] = model_df.groupby('DmaCode')['Reg'].transform(min_max_normalize)
model_df['PE_Total_norm'] = model_df.groupby('DmaCode')['PE_Total'].transform(min_max_normalize)

## Adding Trend ##
model_df['Trend'] = model_df.groupby('DmaCode').cumcount() + 1

## Adding Indicator for Registration Wall ##
model_df['Reg_Wall'] = model_df['Date'].apply(lambda x: 1 if x >= '2022-07-25' else 0)



## Adding Halloween ##
model_df['Holiday_22Halloween'] = model_df.apply(lambda x: 1 if x.Date == '2022-10-31' else 0, axis=1)



## Date Indicator ##
# model_df['Daily_20220919'] = model_df.apply(lambda x: x.Date == '2022-09-19', axis=1)
# model_df['Daily_20220926'] = model_df.apply(lambda x: x.Date == '2022-09-26', axis=1)



## Adding Joins for Qtr 4 only ##
model_df['Qtr'] = pd.to_datetime(model_df['Date']).dt.quarter
model_df['year'] = pd.to_datetime(model_df['Date']).dt.year

# model_df['Joins_Q4'] = model_df.apply(lambda x: x.Joins if x.Qtr == 4 else 0,axis=1)

## Saturating Joins ##
# model_df['Joins_Q4_Sat90'] = model_df['Joins_Q4']**(0.90)
# model_df['Joins_Q4_Sat70'] = model_df['Joins_Q4']**(0.70)
# model_df['Joins_Q4_Sat50'] = model_df['Joins_Q4']**(0.50)
# model_df['Joins_Q4_Sat30'] = model_df['Joins_Q4']**(0.30)


# model_df['Joins_Sat90'] = model_df['Joins']**(0.90)
# model_df['Joins_Sat70'] = model_df['Joins']**(0.70)
# model_df['Joins_Sat50'] = model_df['Joins']**(0.50)
# model_df['Joins_Sat30'] = model_df['Joins']**(0.30)



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Model Cols

# COMMAND ----------

## Model Building Columns Inventory ##
primary_cols = ['DmaCode', 'Date']
target_col = ['Reg_norm']

MediaModel_col = ['Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps', 'LeadGen_Imps',
       'Radio_Spend', 'OOH_Imps', 'Print_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps'] 

HnP_col = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow',
       'PromoEvnt_22TikTok', 'Holiday_22Christams', 'PromoEvnt_23KaylaCoupon', 
       'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'PromoEvnt_23RollingStone', 'Holiday_23Christams'] ## Removed  'PromoEvnt_23EvdaySavCamp' (con)

seasonality_col = ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6']

Brand_col = ['Index', 'Buzz', 'Impression', 'Quality', 'Value', 'Reputation', 'Satisfaction', 'Recommend',
       'Awareness', 'Attention', 'Ad_Awareness', 'WOM_Exposure', 'Consideration', 'Purchase_Intent', 'Current_Customer', 'Former_Customer']

dummy_col = ['dummy_20220711']

other_col = ['Joins', 'BenRefAll']

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Model Development

# COMMAND ----------

## Filtering Data ##

'''

Taking Data only where reg wall is present 

'''

model_df = model_df[model_df['Date']>='2022-08-01']

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.01 Loading Joins Model

# COMMAND ----------

input_vars = [ 

'PromoEvnt_22WsjTodayShow',
'PromoEvnt_23RollingStone',
'Holiday_22Halloween',

## DM&P ##
# 'Display_IcmDigital_Imps_AdStock6L1Wk80Ad_Power40',
'Social_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Affiliate_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Video_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
'Search_IcmDigital_Clicks_AdStock6L1Wk90Ad_Power90',

'Social_IcmSocial_Imps_AdStock6L1Wk90Ad_Power90',

# 'Display_ASI_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_ASI_Imps_AdStock6L1Wk90Ad_Power90',
'Search_ASI_Clicks_AdStock6L1Wk90Ad_Power90',



# 'Display_MediaComBrand_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'TV_MediaComBrand_Imps_AdStock6L2Wk70Ad_Power70',
# 'Video_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_MediaComBrand_Clicks_AdStock6L1Wk90Ad_Power90'


         
       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Reg_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.02 Adding Priorty Engagement Data

# COMMAND ----------

input_vars = [ 

'PromoEvnt_22WsjTodayShow',
'PromoEvnt_23RollingStone',
'Holiday_22Halloween',

## DM&P ##
# 'Display_IcmDigital_Imps_AdStock6L1Wk80Ad_Power40',
'Social_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Affiliate_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Video_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
'Search_IcmDigital_Clicks_AdStock6L1Wk90Ad_Power90',

'Social_IcmSocial_Imps_AdStock6L1Wk90Ad_Power90',

# 'Display_ASI_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_ASI_Imps_AdStock6L1Wk90Ad_Power90',
'Search_ASI_Clicks_AdStock6L1Wk90Ad_Power90',



# 'Display_MediaComBrand_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'TV_MediaComBrand_Imps_AdStock6L2Wk70Ad_Power70',
# 'Video_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_MediaComBrand_Clicks_AdStock6L1Wk90Ad_Power90'




'AARP_Rewards', 
'AARP_Skills_Builder', 
'Online_Community', 

# 'State Events', 
# 'Movies for Grownups: Event Registration',
# 'State Volunteer, Digital Fraud Fighter, Virtual Veterans Brigade, Wish of a Lifetime Volunteer, Disrupt Aging Classroom', 
# 'Volunteer: Submit an Interest Form', 
# 'On membership FAQ Page',
# 'AARP Money Map', 
# 'Games', 
# 'AARP Job Board', 

# 'PE_Total'
         
       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Reg_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.03 Adding Priorty Engagement Data and Total sum of priroty engagement

# COMMAND ----------

input_vars = [ 

'PromoEvnt_22WsjTodayShow',
'PromoEvnt_23RollingStone',
'Holiday_22Halloween',

## DM&P ##
# 'Display_IcmDigital_Imps_AdStock6L1Wk80Ad_Power40',
'Social_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Affiliate_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Video_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
'Search_IcmDigital_Clicks_AdStock6L1Wk90Ad_Power90',

'Social_IcmSocial_Imps_AdStock6L1Wk90Ad_Power90',

# 'Display_ASI_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_ASI_Imps_AdStock6L1Wk90Ad_Power90',
'Search_ASI_Clicks_AdStock6L1Wk90Ad_Power90',



# 'Display_MediaComBrand_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'TV_MediaComBrand_Imps_AdStock6L2Wk70Ad_Power70',
# 'Video_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_MediaComBrand_Clicks_AdStock6L1Wk90Ad_Power90'




# 'AARP_Rewards', 
# 'AARP_Skills_Builder', 
# 'Online_Community', 

# 'State Events', 
# 'Movies for Grownups: Event Registration',
# 'State Volunteer, Digital Fraud Fighter, Virtual Veterans Brigade, Wish of a Lifetime Volunteer, Disrupt Aging Classroom', 
# 'Volunteer: Submit an Interest Form', 
# 'On membership FAQ Page',
# 'AARP Money Map', 
# 'Games', 
# 'AARP Job Board', 

'PE_Total'
         
       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Reg_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.04 Bringing back 3.02

# COMMAND ----------

input_vars = [ 

# 'PromoEvnt_22WsjTodayShow',
# 'PromoEvnt_23RollingStone',
# 'Holiday_22Halloween',

## DM&P ##
# 'Display_IcmDigital_Imps_AdStock6L1Wk80Ad_Power40',
'Social_IcmDigital_Imps_AdStock6L1Wk90Ad_Power40',
# 'Affiliate_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Video_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
'Search_IcmDigital_Clicks_AdStock6L1Wk90Ad_Power40',

# 'Social_IcmSocial_Imps_AdStock6L1Wk90Ad_Power40',

# 'Display_ASI_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_ASI_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_ASI_Clicks_AdStock6L1Wk90Ad_Power90',



# 'Display_MediaComBrand_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'TV_MediaComBrand_Imps_AdStock6L2Wk70Ad_Power70',
# 'Video_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_MediaComBrand_Clicks_AdStock6L1Wk90Ad_Power90'




# 'AARP_Rewards', 
# 'Online_Community', 


# 'AARP_Skills_Builder', 
# 'State Events', 
# 'Movies for Grownups: Event Registration',
# 'State Volunteer, Digital Fraud Fighter, Virtual Veterans Brigade, Wish of a Lifetime Volunteer, Disrupt Aging Classroom', 
# 'Volunteer: Submit an Interest Form', 
# 'On membership FAQ Page',
# 'AARP Money Map', 
# 'Games', 
# 'AARP Job Board', 

# 'PE_Total'
         
       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("PE_Total_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

while True:
  val=1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.05 Changing Targt Variable and Regressing against highest correlated Variables 

# COMMAND ----------

input_vars = [ 

# 'PromoEvnt_22WsjTodayShow',
# 'PromoEvnt_23RollingStone',
# 'Holiday_22Halloween',

# 'Trend',

## DM&P ##
# 'Display_IcmDigital_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Affiliate_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Video_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_IcmDigital_Clicks_AdStock6L1Wk90Ad_Power90',

# 'Social_IcmSocial_Imps_AdStock6L1Wk90Ad_Power90',

# 'Display_ASI_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_ASI_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_ASI_Clicks_AdStock6L1Wk90Ad_Power90',



'Display_MediaComBrand_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'TV_MediaComBrand_Imps_AdStock6L2Wk70Ad_Power70',
# 'Video_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
'Search_MediaComBrand_Clicks_AdStock6L1Wk90Ad_Power90'


         
       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("PE_Total_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.06 Brand Model for Registration

# COMMAND ----------

input_vars = [ 

# 'PromoEvnt_22WsjTodayShow',
# 'PromoEvnt_23RollingStone',
# 'Holiday_22Halloween',

## DM&P ##
# 'Display_IcmDigital_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Affiliate_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Video_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_IcmDigital_Clicks_AdStock6L1Wk90Ad_Power90',

# 'Social_IcmSocial_Imps_AdStock6L1Wk90Ad_Power90',

# 'Display_ASI_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_ASI_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_ASI_Clicks_AdStock6L1Wk90Ad_Power90',



# 'Display_MediaComBrand_Imps_AdStock6L1Wk80Ad_Power40',
'Social_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'TV_MediaComBrand_Imps_AdStock6L2Wk70Ad_Power70',
'Video_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
'Search_MediaComBrand_Clicks_AdStock6L1Wk90Ad_Power90',




# 'AARP_Rewards', 
# 'Online_Community', 


# 'AARP_Skills_Builder', 
# 'State Events', 
# 'Movies for Grownups: Event Registration',
# 'State Volunteer, Digital Fraud Fighter, Virtual Veterans Brigade, Wish of a Lifetime Volunteer, Disrupt Aging Classroom', 
# 'Volunteer: Submit an Interest Form', 
# 'On membership FAQ Page',
# 'AARP Money Map', 
# 'Games', 
# 'AARP Job Board', 

# 'PE_Total'
         
       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Reg_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.07 Changing Targt Variable and Regressing against DM&P Variables

# COMMAND ----------

input_vars = [ 

# 'PromoEvnt_22WsjTodayShow',
# 'PromoEvnt_23RollingStone',
# 'Holiday_22Halloween',

# 'Trend',

## DM&P ##
# 'Display_IcmDigital_Imps_AdStock6L1Wk80Ad_Power40',
'Social_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Affiliate_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Video_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_IcmDigital_Clicks_AdStock6L1Wk90Ad_Power90',

# 'Social_IcmSocial_Imps_AdStock6L1Wk90Ad_Power90',

# 'Display_ASI_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_ASI_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_ASI_Clicks_AdStock6L1Wk90Ad_Power90',



# 'Display_MediaComBrand_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'TV_MediaComBrand_Imps_AdStock6L2Wk70Ad_Power70',
# 'Video_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_MediaComBrand_Clicks_AdStock6L1Wk90Ad_Power90'


         
       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("PE_Total_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.08 Priorty ENgagement driving Registrations

# COMMAND ----------

input_vars = [ 


# 'Display_MediaComBrand_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'TV_MediaComBrand_Imps_AdStock6L2Wk70Ad_Power70',
# 'Video_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_MediaComBrand_Clicks_AdStock6L1Wk90Ad_Power90',


'PE_Total_norm'
         
       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Reg_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

# Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

## Saving Model ##

# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_Apr2_PE_Vo4_Reg.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_Apr2_PE_Vo4_PE.pkl'
model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_Apr2_PE_Vo5_Reg.pkl'
with open(model_path,"wb") as f:
  pickle.dump(model, f)


# ## Reading Model ##
# model_path = 
# with open(model_path, "rb") as f:
#   model = pickle.load(f)

# COMMAND ----------

## Creating Model Predict ##
joins = model_df[['DmaCode', 'Date','Reg_norm']]
pred = model.fittedvalues.rename('Pred')

if 'Reg' not in joins.columns:
  joins.rename(columns = {joins.columns[-1]:'Reg'},inplace=True)

pred_df = pd.concat([joins, pred],axis=1)


# Aggregate the data by 'Date'
pred_df_date = pred_df.groupby('Date').agg({'Reg':'sum', 'Pred':'sum'}).reset_index()

## Plots ##
plt.figure(figsize=(25,6))
plt.plot( pred_df_date.Reg, label='Actual')
plt.plot( pred_df_date.Pred, label='Predicted')

plt.axvline(x=0, color='r')
plt.axvline(x=51-27, color='r')
plt.axvline(x=103-27, color='r')

# Calculate and display residuals
residuals = pred_df_date.Reg - pred_df_date.Pred
# Create a figure with two subplots (1 row, 2 columns)
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(25, 8))

# Plot residuals over time
ax1.scatter(pred_df_date.Date, residuals, label='Residuals')
ax1.axhline(y=0, color='r', linestyle='--')
ax1.set_xlabel('Date')
ax1.set_ylabel('Residuals')
ax1.set_title('Residuals Over Time')

ax1.axvline(x=0, color='r')
ax1.axvline(x=51-27, color='r')
ax1.axvline(x=103-27, color='r')



ax1.legend()

# Plot histogram of residuals
ax2.hist(residuals, bins=30, edgecolor='black')
ax2.set_xlabel('Residuals')
ax2.set_ylabel('Frequency')
ax2.set_title('Histogram of Residuals')


# Calculate and display mean squared error (MSE) and R-squared
mape = mean_absolute_percentage_error(pred_df_date.Reg, pred_df_date.Pred)
r_squared = r2_score(pred_df_date.Reg, pred_df_date.Pred)
skewness = skew(residuals)
kurt = kurtosis(residuals)
dw_stat = sm.stats.stattools.durbin_watson(residuals)
jb_test = jarque_bera(residuals)


print(f"Mean Absolute Percentage Error (MAPE): {mape}")
print(f"R-squared: {r_squared}")
print(f"Skewness: {skewness}")
print(f"Kurtosis: {kurt}")
print(f"Jarque Bera: {jb_test}")
print(f"DW Stat: {dw_stat}")
print(f"Condition Number: {np.linalg.cond(model.model.exog)}")


plt.tight_layout()
plt.show()

# COMMAND ----------

random_effect_dict = {key:value.values[0]+model.fe_params['Intercept'] for key,value in model.random_effects.items()}
len([k for k,v in random_effect_dict.items() if v<0])

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



def colldiag(df, scale=True, center=False, add_intercept=True):
    """
    Performs collinearity diagnostics on a DataFrame.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing the data for collinearity diagnostics.

    scale : bool, optional (default=True)
        Whether to scale the variables. Default is True.

    center : bool, optional (default=False)
        Whether to center the variables. Default is False.

    add_intercept : bool, optional (default=True)
        Whether to add an intercept term to the model. Default is True.

    Returns:
    --------
    DataFrame
        A DataFrame containing the condition index and variance decomposition proportion.

    """

    # If centering, intercept should not be added
    if center:
        add_intercept = False

    # Initialize result dictionary
    result = {}

    # Copy DataFrame to avoid modifying original
    X = df.copy()

    # Drop rows with missing values
    X = X.dropna()

    # Scale and/or center the data
    X = scale_default(X, scale=scale, center=center)

    # Add intercept if requested
    if add_intercept:
        X.insert(0, 'intercept', 1)

    # Singular Value Decomposition (SVD)
    svdX = np.linalg.svd(X)

    # Calculate Condition Index
    condindx = svdX[1][0] / svdX[1]

    # Calculate Variance Decomposition Proportion
    Phi = np.square(svdX[2].T @ np.diag(1 / svdX[1]))
    pi = (Phi.T / np.sum(np.abs(Phi.T), axis=0))

    # Store results in dictionary
    result['cond_indx'] = condindx
    result['pi'] = pi

    # Create DataFrame to display results
    df_results = pd.DataFrame({'Condition Index': condindx})
    df_pi = pd.DataFrame(pi, columns=X.columns)

    # Concatenate condition index and variance decomposition proportion DataFrames
    df_results = pd.concat([df_results, df_pi], axis=1)

    return df_results


def scale_default(df, center=True, scale=True):
    """
    Scale and/or center the columns of a DataFrame.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing the data to be scaled and/or centered.

    center : bool or array-like, optional (default=True)
        If True, center the data. If array-like, it should contain the center to be used for each column.
        If False, no centering is performed.

    scale : bool or array-like, optional (default=True)
        If True, scale the data. If array-like, it should contain the scale to be used for each column.
        If False, no scaling is performed.

    Returns:
    --------
    pandas DataFrame
        A DataFrame with scaled and/or centered data.

    Raises:
    -------
    ValueError
        If the length of 'center' or 'scale' does not match the number of columns in the DataFrame.

    """

    # Convert DataFrame to numpy array
    x = df.values

    # Number of columns in the DataFrame
    nc = x.shape[1]

    # Centering
    if isinstance(center, bool):
        if center:
            # Calculate column-wise mean
            center = np.nanmean(x, axis=0)
            # Subtract column-wise mean from each value
            x = x - center
    elif isinstance(center, (int, float)):
        if len(center) == nc:
            # Subtract provided center from each value
            x = x - center
        else:
            raise ValueError("Length of 'center' must equal the number of columns of 'x'")

    # Scaling
    if isinstance(scale, bool):
        if scale:
            # Define function to calculate scale for each column
            def f(v):
                v = v[~np.isnan(v)]  # Remove NaN values
                # Calculate scale using unbiased estimator
                return np.sqrt(np.sum(v**2) / max(1, len(v) - 1))

            # Apply the function column-wise to calculate scale
            scale = np.apply_along_axis(f, axis=0, arr=x)
            # Divide each value by the calculated scale
            x = x / scale
    elif isinstance(scale, (int, float)):
        if len(scale) == nc:
            # Divide each value by the provided scale
            x = x / scale
        else:
            raise ValueError("Length of 'scale' must equal the number of columns of 'x'")

    # Convert scaled data back to DataFrame with original column names
    result_df = pd.DataFrame(x, columns=df.columns)

    # Store center and scale values as attributes of the DataFrame
    if isinstance(center, (int, float)):
        result_df._scaled_center = center
    if isinstance(scale, (int, float)):
        result_df._scaled_scale = scale

    return result_df



# COMMAND ----------

'''
* If you find conditon index exceeding 10, it indicates that the regression coeff. may be unstable due to multicollinearity.
* Focus on variable wuth high variation decomposition values, as they contribute more to the multicollinearity issue.
* Consider strategies to address multi-collinearity such as removing redundant variables, transforming variables or using regualrization  tech like ridge regression.
'''

################################################
####### START: Model Variable Defination #######
################################################  

input_vars = [



'PromoEvnt_22WsjTodayShow',
'PromoEvnt_23RollingStone',
'Holiday_22Halloween',

## DM&P ##
# 'Display_IcmDigital_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Affiliate_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Video_IcmDigital_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_IcmDigital_Clicks_AdStock6L1Wk90Ad_Power90',

# 'Social_IcmSocial_Imps_AdStock6L1Wk90Ad_Power90',

# 'Display_ASI_Imps_AdStock6L1Wk80Ad_Power40',
# 'Social_ASI_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_ASI_Clicks_AdStock6L1Wk90Ad_Power90',



# 'Display_MediaComBrand_Imps_AdStock6L1Wk80Ad_Power40',
'Social_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
'TV_MediaComBrand_Imps_AdStock6L2Wk70Ad_Power70',
# 'Video_MediaComBrand_Imps_AdStock6L1Wk90Ad_Power90',
'Search_MediaComBrand_Clicks_AdStock6L1Wk90Ad_Power90',




'AARP_Rewards', 
'Online_Community', 

# 'State Events', 
# 'Movies for Grownups: Event Registration',
# 'State Volunteer, Digital Fraud Fighter, Virtual Veterans Brigade, Wish of a Lifetime Volunteer, Disrupt Aging Classroom', 
# 'Volunteer: Submit an Interest Form', 
# 'On membership FAQ Page',
# 'AARP Money Map', 
# 'Games', 
# 'AARP Job Board', 




       


       ]



################################################
######### END: Model Variable Defination #######
################################################  

model_vars = input_vars


df_cond_indx = colldiag(model_df.groupby('Date')[model_vars].sum())
df_cond_indx.round(2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.0 Analyzing Residuals 

# COMMAND ----------

## Getting Residuals ##
pred_df_date['Residuals'] = pred_df_date['Reg'] - pred_df_date['Pred']
pred_df_date.display()

# COMMAND ----------

plt.figure(figsize=(20,8))
plt.plot(pred_df_date['Residuals'])

plt.axhline(y=0, color='r', linestyle='--')
plt.axvline(x=51-27, color='r')

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
plot_pacf(time_series, ax=ax[1], lags=30, method='ywm')
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

# MAGIC %md
# MAGIC # AdHoc Section

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

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"
c

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0 Model Development

# COMMAND ----------

'''
Saving and Reading Model Data
'''

#########################
##### Creating Data #####
#########################


## Reading Raw Data ##
# rawdata_df=spark.sql('select * from temp.ja_blend_mmm2_DateDmaAge_ChModelDf').toPandas()
# rawdata_df_join = rawdata_df.pivot(index = ['Date', 'DmaCode'], columns = 'AgeBucket', values = 'Joins').reset_index()

# rawdata_df_join = rawdata_df_join.rename(columns = { '1849' :'Joins_1849', '5059' :'Joins_5059', '6069' :'Joins_6069', '7079' :'Joins_7079', '8099':'Joins_8099' })
# rawdata_df_join.head().display()


## Reading Model Data ##
# model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_statsmodel.csv').drop('Unnamed: 0',axis=1)



## Merging Data ##
# model_df['DmaCode'] = model_df['DmaCode'].astype('int')
# model_df['Date'] = pd.to_datetime(model_df['Date'])

# rawdata_df_join['DmaCode'] = rawdata_df_join['DmaCode'].astype('int')
# rawdata_df_join['Date'] = pd.to_datetime(rawdata_df_join['Date'])

# model_df = model_df.merge(rawdata_df_join, on=['Date', 'DmaCode'])



## Min max Scaling Target Variable ##
# model_df['Joins_1849_norm'] = model_df.groupby('DmaCode')['Joins_1849'].transform(min_max_normalize)
# model_df['Joins_5059_norm'] = model_df.groupby('DmaCode')['Joins_5059'].transform(min_max_normalize)
# model_df['Joins_6069_norm'] = model_df.groupby('DmaCode')['Joins_6069'].transform(min_max_normalize)
# model_df['Joins_7079_norm'] = model_df.groupby('DmaCode')['Joins_7079'].transform(min_max_normalize)
# model_df['Joins_8099_norm'] = model_df.groupby('DmaCode')['Joins_8099'].transform(min_max_normalize)



## saving model dataframe ##
# model_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_AgeGroup_statsmodel.csv')


############################
###### Reading Model #######
############################

model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_AgeGroup_statsmodel.csv').drop('Unnamed: 0',axis=1)

model_df['Joins_7099'] = model_df['Joins_7079'] + model_df['Joins_8099']
model_df['Joins_7099_norm'] = model_df.groupby('DmaCode')['Joins_7099'].transform(min_max_normalize)

model_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Loading Frequentist Model

# COMMAND ----------

## Reading Model ##
model_path =  '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo16_updatedData.pkl'

with open(model_path, "rb") as f:
  model = pickle.load(f)

#Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.2 Best Model: Frequentist Approach

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

# MAGIC %md
# MAGIC #### 1.3 Bayesian Model

# COMMAND ----------

## Definig Some Utilities 
RANDOM_SEED = 12345


DmaCodes, mn_DmaCodes = model_df.DmaCode.factorize()
coords = {'DmaCodes': mn_DmaCodes}


# COMMAND ----------

with pm.Model(coords=coords) as varying_intercept:
       
       ################
       ## Input Data ##
       ################

       dma_idx = pm.MutableData('dma_idx', DmaCodes, dims='obs_id')

       ## Media Variables ##
       tv_idx = pm.MutableData('tv_idx', model_df['TV_Imps_AdStock6L2Wk70Ad_Power70'].values, dims='obs_id')
       search_idx = pm.MutableData('search_idx', model_df['Search_Clicks_AdStock6L1Wk90Ad_Power90'].values, dims='obs_id') 
       social_idx = pm.MutableData('social_idx', model_df['Social_Imps_AdStock6L1Wk90Ad_Power90'].values, dims='obs_id')

       leadgen_idx = pm.MutableData('leadgen_idx', model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80'].values, dims='obs_id')
       email_idx = pm.MutableData('email_idx', model_df['Email_Spend_AdStock6L1Wk20Ad_Power90'].values, dims='obs_id')

       dm_idx = pm.MutableData('dm_idx', model_df['DirectMail_Imps_AdStock6L3Wk70Ad_Power90'].values, dims='obs_id')
       AltMedia_idx = pm.MutableData('AltMedia_idx', model_df['AltMedia_Imps_AdStock6L3Wk90Ad_Power90'].values, dims='obs_id')



       ## Promotions and Seasonality  ##

       PromoEvnt_22MembDrive_idx = pm.MutableData('PromoEvnt_22MembDrive_idx', model_df['PromoEvnt_22MembDrive_Eng'].values, dims='obs_id')
       PromoEvnt_22BlackFriday_idx = pm.MutableData('PromoEvnt_22BlackFriday_idx', model_df['PromoEvnt_22BlackFriday_Eng'].values, dims='obs_id')

       PromoEvnt_23MembDrive_idx = pm.MutableData('PromoEvnt_23embDrive_idx', model_df['PromoEvnt_23MembDrive_Eng'].values, dims='obs_id')
       PromoEvnt_23BlackFriday_idx = pm.MutableData('PromoEvnt_23BlackFriday_idx', model_df['PromoEvnt_23BlackFriday_Eng'].values, dims='obs_id')
       

       Seasonality_Feb_idx = pm.MutableData('Seasonality_Feb_idx', model_df['Seasonality_Feb'].values, dims='obs_id')
       Seasonality_Mar_idx = pm.MutableData('Seasonality_Mar_idx', model_df['Seasonality_Mar'].values, dims='obs_id')
       Seasonality_Apr_idx = pm.MutableData('Seasonality_Apr_idx', model_df['Seasonality_Apr'].values, dims='obs_id')
       Seasonality_Dec_idx = pm.MutableData('Seasonality_Dec_idx', model_df['Seasonality_Dec'].values, dims='obs_id')

       ## Other Flags ##
       July0422_lag_idx = pm.MutableData('July0422_lag_idx', model_df['July0422_lag'].values, dims='obs_id')



       #############################
       ## Setup Data Distribution ##
       #############################

       ## Random Effect ##

       ### Priors ###
       mu_re = pm.LogNormal('mu_re', mu=0.2, sigma=0.5)
       sigma_re = pm.Exponential('sigma_re',1)

       intercept_re = pm.LogNormal('intercept_re', mu=mu_re, sigma=sigma_re, dims='DmaCodes')

       ## Fixed Effect ##
       
       ### Media Variables ###
       beta_tv =    pm.LogNormal('beta_tv', mu= 0.037,sigma=0.5)           #  pm.Beta('beta_tv', alpha=1.5, beta=10) 
       beta_search =  pm.LogNormal('beta_search', mu= 0.085,sigma=0.5)      #  pm.Beta('beta_search', alpha=1.5, beta=10)
       beta_social =  pm.LogNormal('beta_social', mu= 0.029,sigma=0.5)      #  pm.Beta('beta_social', alpha=1.5, beta=10)
       
       beta_leadgen =  pm.LogNormal('beta_leadgen', mu= 0.21,sigma=0.5)    #  pm.Beta('beta_leadgen', alpha=3, beta=10)  
       beta_email = pm.LogNormal('beta_email', mu= 0.059,sigma=0.5)         #  pm.Beta('beta_email', alpha=1.5, beta=10)

       beta_dm =   pm.LogNormal('beta_dm', mu= 0.082,sigma=0.5)             #  pm.Beta('beta_dm', alpha=1.5, beta=10)
       beta_AltMedia = pm.LogNormal('beta_AltMedia', mu= 0.084,sigma=0.5)  # pm.Beta('beta_AltMedia', alpha=1.5, beta=10)

       ## Promotions and Seasonality  ##
       beta_PromoEvnt_22MembDrive = pm.LogNormal('beta_PromoEvnt_22MembDrive', mu= 0.257,sigma=0.5) #  pm.Beta('beta_PromoEvnt_22MembDrive', alpha=3, beta=10)
       beta_PromoEvnt_22BlackFriday = pm.LogNormal('beta_PromoEvnt_22BlackFriday', mu= 0.416,sigma=0.5)  # pm.Beta('beta_PromoEvnt_22BlackFriday', alpha=7, beta=10)

       beta_PromoEvnt_23MembDrive = pm.LogNormal('beta_PromoEvnt_23MembDrive', mu= 0.306,sigma=0.5)  #  pm.Beta('beta_PromoEvnt_23MembDrive', alpha=4, beta=10)
       beta_PromoEvnt_23BlackFriday = pm.LogNormal('beta_PromoEvnt_23BlackFriday', mu= 0.370,sigma=0.5)  # pm.Beta('beta_PromoEvnt_23BlackFriday', alpha=6, beta=10)


       beta_Seasonality_Feb = pm.Normal('beta_Seasonality_Feb', mu= 0.117,sigma=10)       
       beta_Seasonality_Mar = pm.Normal('beta_Seasonality_Mar', mu= 0.086,sigma=10)       
       beta_Seasonality_Apr = pm.Normal('beta_Seasonality_Apr', mu= -0.038,sigma=10)       
       beta_Seasonality_Dec = pm.Normal('beta_Seasonality_Dec', mu= -0.128,sigma=10)       
       
       
       ### Other Flags ###
       beta_July0422_lag =  pm.LogNormal('beta_July0422_lag', mu= 0.316,sigma=1) #pm.Beta('beta_July0422_lag', alpha=3, beta=10)




       #################
       ## Setup Model ##
       #################


       ## Setup Error Model ##
       sd_y = pm.Normal("sd_y", mu=0, sigma = 2)

       ## Setup Expected Value ##
       y_hat = intercept_re[dma_idx] + beta_tv*tv_idx + beta_search*search_idx + beta_social*social_idx + beta_leadgen*leadgen_idx + beta_email*email_idx + beta_dm*dm_idx + beta_AltMedia*AltMedia_idx + beta_PromoEvnt_22MembDrive*PromoEvnt_22MembDrive_idx + beta_PromoEvnt_22BlackFriday*PromoEvnt_22BlackFriday_idx + beta_PromoEvnt_23MembDrive*PromoEvnt_23MembDrive_idx + beta_PromoEvnt_23BlackFriday*PromoEvnt_23BlackFriday_idx + beta_Seasonality_Mar*Seasonality_Mar_idx + beta_Seasonality_Apr*Seasonality_Apr_idx + beta_Seasonality_Feb*Seasonality_Feb_idx + beta_Seasonality_Dec*Seasonality_Dec_idx + beta_July0422_lag*July0422_lag_idx

       ## Setup Liklehood function ##
       y_like = pm.Normal('y_like', mu= y_hat, sigma=sd_y, observed=model_df['Joins_norm'], dims='obs_id')




# COMMAND ----------

## Running Bayesian Machinery ##
with varying_intercept:
  varying_intercept_trace = pm.sample(draws=1000,tune=100, chains=4, random_seed = RANDOM_SEED, progressbar=True)

# COMMAND ----------

# Model Summary #
summary_df = az.summary(varying_intercept_trace, round_to=2).reset_index()
summary_df.display()

# COMMAND ----------

# Plot Posterior Distribution #
pm.plot_trace(varying_intercept_trace)

# COMMAND ----------

### All Model Variables ###
map_dict = {

'beta_July0422_lag':'July0422_lag',

'beta_PromoEvnt_22BlackFriday':'PromoEvnt_22BlackFriday_Eng',
'beta_PromoEvnt_22MembDrive':'PromoEvnt_22MembDrive_Eng',
'beta_PromoEvnt_23BlackFriday':'PromoEvnt_23BlackFriday_Eng',
'beta_PromoEvnt_23MembDrive':'PromoEvnt_23MembDrive_Eng',

'beta_Seasonality_Feb':'Seasonality_Feb',
'beta_Seasonality_Apr':'Seasonality_Apr',
'beta_Seasonality_Mar':'Seasonality_Mar',
'beta_Seasonality_Dec':'Seasonality_Dec',

'beta_dm': 'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'beta_AltMedia':'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
'beta_email':'Email_Spend_AdStock6L1Wk20Ad_Power90',
'beta_leadgen':'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'beta_search':'Search_Clicks_AdStock6L1Wk90Ad_Power90',
'beta_social':'Social_Imps_AdStock6L1Wk90Ad_Power90',
'beta_tv': 'TV_Imps_AdStock6L2Wk70Ad_Power70',



}


### Geting Fixed Effects Variables ##
fe_coeff_df = summary_df[summary_df['index'].isin(map_dict.keys())]
re_coeff_df = summary_df[summary_df['index'].str.contains('intercept_re')]

### Geting Fixed Effects into Df ##
fe_coeff_df['input_vars'] = fe_coeff_df['index'].map(map_dict)

### Creating map for Random effect ###
re_coeff_df['DmaCode'] = re_coeff_df['index'].str.split('[').str[1].str.split(']').str[0]
re_coeff_df['DmaCode'] = re_coeff_df['DmaCode'].astype('int64')
re_dict =  re_coeff_df.set_index('DmaCode')['mean'].to_dict()



### Creating Contribution data ###
input_vars = list(fe_coeff_df['input_vars'].unique()) 


## Adding Variable Contribution ##
coeff_value = fe_coeff_df['mean'].values
coeff_names = fe_coeff_df['input_vars'].values

input_model = model_df[input_vars].values
## Variable contribution##
contribution = pd.DataFrame(coeff_value*input_model, columns=coeff_names)
contribution = pd.concat([model_df[['DmaCode', 'Date', 'Joins', 'Joins_norm']], contribution], axis=1) ## Change Target Var

contribution['Random Intercept'] = contribution['DmaCode'].map(re_dict)
contribution.insert(4, 'Pred', contribution[input_vars + ['Random Intercept'] ].sum(axis=1))

contribution['Fixed Dma Effect'] = contribution['Random Intercept'].mean()
contribution['Random Dma Effect'] = contribution['Random Intercept'] - contribution['Fixed Dma Effect']

contribution['Residuals'] = contribution['Joins_norm'] - contribution['Pred']  ## Change Target Var
contribution['ratio_resid_join'] =  contribution['Residuals']/contribution['Joins_norm'] ## Change Target Var
contribution['Year'] = pd.to_datetime(contribution['Date']).dt.year

## Update name ##
contribution.rename(columns={'Joins':'Joins_unnorm', 'Joins_norm':'Joins'}, inplace=True) ## Change Target Var

contribution.head()

# COMMAND ----------

analysis_df = contribution.groupby('Date')[['Joins', 'Pred' ]].sum()


## Plots ##
plt.figure(figsize=(25,6))
plt.plot( analysis_df.Joins, label='Actual')
plt.plot( analysis_df.Pred, label='Predicted')

plt.axvline(x=0, color='r')
plt.axvline(x=51, color='r')
plt.axvline(x=103, color='r')

# Calculate and display residuals
residuals = analysis_df.Joins - analysis_df.Pred
# Create a figure with two subplots (1 row, 2 columns)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(25, 6))

# Plot residuals over time
ax1.scatter(analysis_df.index, residuals, label='Residuals')
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
mape = mean_absolute_percentage_error(analysis_df.Joins, analysis_df.Pred)
r_squared = r2_score(analysis_df.Joins, analysis_df.Pred)
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

# MAGIC %md
# MAGIC ## Aggregated Analysis Dataframe 

# COMMAND ----------

######################################################################################
##################### Enter List of All Model Variables ##############################
######################################################################################

## Model Creation ##
seasoanlity_vars =  ['Seasonality_Feb', 'Seasonality_Mar', 'Seasonality_Apr', 'Seasonality_Dec']

# Holiday_vars = []
Holiday_vars = [ 'July0422_lag']

Promo_vars = ['PromoEvnt_22MembDrive_Eng', 'PromoEvnt_22BlackFriday_Eng', 'PromoEvnt_23MembDrive_Eng', 'PromoEvnt_23BlackFriday_Eng']

dummy_vars = []

##### Media Vars #####


media_vars = ['TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk90Ad_Power90',
'Social_Imps_AdStock6L1Wk90Ad_Power90',


'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'Email_Spend_AdStock6L1Wk20Ad_Power90',

'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90']


######################################################################################
##################### Creating Agg. Analysis Dataframe ###############################
######################################################################################

agg_analysis_list = []

#############################
### Adding Media Variable ###
#############################

for media in media_vars:
  dict_ = {'ChannelLever': 'Membership', 'Channel': media.split("_")[0], 'Support': "_".join(media.split("_")[:2]), 'Transformation': media, 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
  agg_analysis_list.append(dict_)

###############################
### Adding Holiday Variable ###
###############################

for Holiday in Holiday_vars:
  dict_ = {'ChannelLever': 'Holiday', 'Channel': Holiday.split("_")[1], 'Support': Holiday, 'Transformation': Holiday, 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
  agg_analysis_list.append(dict_)


##################################
### Adding Promotions Variable ###
##################################

for Promo in Promo_vars:
  dict_ = {'ChannelLever': 'Promotions', 'Channel': Promo.split("_")[1], 'Support': Promo, 'Transformation': Promo, 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
  agg_analysis_list.append(dict_)

##################################
### Adding Seasoanlity Variable ##
##################################

for season in seasoanlity_vars:
  dict_ = {'ChannelLever': 'Base', 'Channel': 'Seasonal', 'Support': season, 'Transformation': season, 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
  agg_analysis_list.append(dict_)

############################
### Adding Base Variable ###
############################

for dummy in dummy_vars:
  dict_ = {'ChannelLever': 'Base', 'Channel': 'Dummy', 'Support': dummy, 'Transformation': dummy, 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
  agg_analysis_list.append(dict_)

########################
### Adding Intercept ###
########################

## Fixed Effect ##
dict_ = {'ChannelLever': 'Base', 'Channel': 'Fixed Dma Effect', 'Support': 'Fixed Dma Effect', 'Transformation': 'Fixed Dma Effect', 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
agg_analysis_list.append(dict_)

## Random Effect ##
dict_ = {'ChannelLever': 'Base', 'Channel': 'Random Dma Effect', 'Support': 'Random Dma Effect', 'Transformation': 'Random Dma Effect', 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
agg_analysis_list.append(dict_)

##########################
### Creating Dataframe ###
##########################

agg_analysis_df = pd.DataFrame(agg_analysis_list)


#########################
### Filling Dataframe ###
#########################

## Adding Contribution ##
agg_analysis_df['Contrib_2022'] = agg_analysis_df.apply(lambda x: calc_contri(contribution, 2022, x['Transformation']), axis=1)
agg_analysis_df['Contrib_2023'] = agg_analysis_df.apply(lambda x: calc_contri(contribution, 2023, x['Transformation']), axis=1)

## Adding Contribution Proportion ##
agg_analysis_df['Contrib_2022_Prop'] = agg_analysis_df['Contrib_2022']/agg_analysis_df['Contrib_2022'].sum()
agg_analysis_df['Contrib_2023_Prop'] = agg_analysis_df['Contrib_2023']/agg_analysis_df['Contrib_2023'].sum() 

## Adding Support ##
agg_analysis_df['Support_2022'] = agg_analysis_df.apply(lambda x: calc_support(model_df, 2022, x['Support']), axis=1)
agg_analysis_df['Support_2023'] = agg_analysis_df.apply(lambda x: calc_support(model_df, 2023, x['Support']), axis=1)

## Adding Spend ##
agg_analysis_df['Spend_2022'] = agg_analysis_df.apply(lambda x: calc_spend(model_df, 2022, x['Channel']+'_Spend'), axis=1)
agg_analysis_df['Spend_2023'] = agg_analysis_df.apply(lambda x: calc_spend(model_df, 2023, x['Channel']+'_Spend'), axis=1)

agg_analysis_df = agg_analysis_df.round(2)
agg_analysis_df

# COMMAND ----------

## Save Agg. Analysis Dataframe ##
agg_analysis_df.to_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_main_model_combined.csv')

## Read Agg. Analysis Dataframe ##
agg_analysis_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_main_model_combined.csv').drop('Unnamed: 0',axis=1)
agg_analysis_df.display()

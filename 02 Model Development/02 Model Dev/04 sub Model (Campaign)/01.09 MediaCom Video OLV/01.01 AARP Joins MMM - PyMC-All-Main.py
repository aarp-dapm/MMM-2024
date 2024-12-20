# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0 Reading Data

# COMMAND ----------


############################
###### Reading Model #######
############################

model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_Campaign_MediaCom_VideoOLV.csv').drop('Unnamed: 0',axis=1).fillna(0)
model_df.head()

# COMMAND ----------

############################
### Creating Variables #####
#############################


## Creating Search Target Variable ##
coeff =  0.09 ## Getting this from Bayesian Model
model_df['Search_contri'] = model_df['Search_Clicks_AdStock6L1Wk90Ad_Power90']*coeff

## Re-Normalize variable ##
model_df['Search_contri_norm'] = model_df['Search_contri'].transform(min_max_normalize)


# Create the trend variable
model_df['Trend'] = model_df.groupby('DmaCode').cumcount() + 1
model_df['Trend'] = model_df['Trend'].transform(min_max_normalize)

### Creating New Variables ###
model_df['Social_Imps_norm'] = model_df.groupby('DmaCode')['Social_Imps'].transform(min_max_normalize)


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.01 Loading Frequentist Model

# COMMAND ----------

## Reading Model ##
model_path =  '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo16_updatedData.pkl'

with open(model_path, "rb") as f:
  model = pickle.load(f)

#Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.02 Best Model: Frequentist Approach

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
# MAGIC ### 1.03 Loading Frequentist Model

# COMMAND ----------

## Reading Model ##
model_path =  '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_submodel_SearchVo1.pkl'

with open(model_path, "rb") as f:
  model = pickle.load(f)

# Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.04 Search SubModel: Frequentist Approach

# COMMAND ----------

input_vars = [ 
'Trend', 
'Social_Imps_norm',
'TV_Imps_AdStock6L3Wk90Ad_Power30',
'Video_Imps_AdStock6L4Wk30Ad_Power30',

          ]

# COMMAND ----------

##########################################################
######## Anlaysing Column Spend to set Prior Values ######
##########################################################

model_cols = [

'Video_MediaCom_OLV_Brand_Imps_AdStock6L4Wk30Ad_Power30', 
'Video_MediaCom_OLV_NonBrand_Imps_AdStock6L4Wk30Ad_Power30' 
]

data_cols = [

'Video_MediaCom_OLV_Brand_Spend',
'Video_MediaCom_OLV_NonBrand_Spend'

]

sum_df = model_df.groupby(pd.to_datetime(model_df['Date']).dt.year)[data_cols].sum()
year_df = sum_df.sum(axis=1)

sum_df.div(year_df, axis=0).round(2).T.reset_index().display()
sum_df.round(2).T.reset_index().display()

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
       social_idx = pm.MutableData('social_idx', model_df['Social_Imps_norm'].values, dims='obs_id')
       tv_idx = pm.MutableData('tv_idx', model_df['TV_Imps_AdStock6L3Wk90Ad_Power30'].values, dims='obs_id')

       # video_idx = pm.MutableData('video_idx', model_df['Video_Imps_AdStock6L4Wk30Ad_Power30'].values, dims='obs_id')
       video_IcmDigital_Native_idx = pm.MutableData('video_idx', model_df['Video_IcmDigital_Native_Imps_AdStock6L4Wk30Ad_Power30'].values, dims='obs_id')
       video_IcmDigital_YouTube_idx = pm.MutableData('video_IcmDigital_YouTube_idx', model_df['Video_IcmDigital_YouTube_Imps_AdStock6L4Wk30Ad_Power30'].values, dims='obs_id')
       
       # video_MediaCom_OLV_idx = pm.MutableData('video_MediaCom_OLV_idx', model_df['Video_MediaCom_OLV_Imps_AdStock6L4Wk30Ad_Power30'].values, dims='obs_id')
       video_MediaCom_OLV_Brand_idx = pm.MutableData('video_MediaCom_OLV_Brand_idx', model_df['Video_MediaCom_OLV_Brand_Imps_AdStock6L4Wk30Ad_Power30'].values, dims='obs_id')
       video_MediaCom_OLV_NonBrand_idx = pm.MutableData('video_MediaCom_OLV_NonBrand_idx', model_df['Video_MediaCom_OLV_NonBrand_Imps_AdStock6L4Wk30Ad_Power30'].values, dims='obs_id')


       video_MediaCom_YouTube_idx = pm.MutableData('video_MediaCom_YouTube_idx', model_df['Video_MediaCom_YouTube_Imps_AdStock6L4Wk30Ad_Power30'].values, dims='obs_id')



       ## Promotions and Seasonality  ##
       Trend_idx = pm.MutableData('Trend_idx', model_df['Trend'].values, dims='obs_id')

       ## Other Flags ##
       



       #############################
       ## Setup Data Distribution ##
       #############################

       ## Random Effect ##

       ### Priors ###
       mu_re = pm.Normal('mu_re', mu=0, sigma=10)
       sigma_re = pm.Exponential('sigma_re',1)

       intercept_re = pm.Normal('intercept_re', mu=mu_re, sigma=sigma_re, dims='DmaCodes')

       ## Fixed Effect ##
       
       ### Media Variables ###
       beta_social =    pm.Normal('beta_social', mu= 0.14,sigma=0.01) 
       beta_tv =    pm.Normal('beta_tv', mu= 0.04,sigma=0.01) 

       # beta_video =    pm.Normal('beta_video', mu= 0.03,sigma=0.01)           
       beta_IcmDigital_Native_video =    pm.Uniform('beta_IcmDigital_Native_video', lower= 0.03*0.09-0.001,upper=0.03*0.09+0.001) #pm.Normal('beta_IcmDigital_Native_video', mu= 0.03/4,sigma=0.01)
       beta_IcmDigital_YouTube_video =    pm.Uniform('beta_IcmDigital_YouTube_video', lower= 0.03*0.01-0.0002,upper=0.03*0.01+0.001) #pm.Normal('beta_IcmDigital_YouTube_video', mu= 0.03/4,sigma=0.01)
       
       # beta_MediaCom_OLV_video =    pm.Uniform('beta_MediaCom_OLV_video',  lower= 0.03*0.75-0.015,upper=0.03*0.75+0.015) #pm.Normal('beta_MediaCom_OLV_video', mu= 0.03/4,sigma=0.01)
       beta_MediaCom_OLV_Brand_video =    pm.Uniform('beta_MediaCom_OLV_Brand_video',  lower= 0.03*0.75*0.73-0.015,upper=0.03*0.75*0.73+0.015) #pm.Normal('beta_MediaCom_OLV_video', mu= 0.03/4,sigma=0.01)
       beta_MediaCom_OLV_NonBrand_video =    pm.Uniform('beta_MediaCom_OLV_NonBrand_video',  lower= 0.03*0.75*0.27-0.006,upper=0.03*0.75*0.27+0.006) #pm.Normal('beta_MediaCom_OLV_video', mu= 0.03/4,sigma=0.01)

       beta_MediaCom_YouTube_video =    pm.Uniform('beta_MediaCom_YouTube_video',  lower= 0.03*0.15-0.003,upper=0.03*0.15+0.003) #pm.Normal('beta_MediaCom_YouTube_video', mu= 0.03/4,sigma=0.01)


       ## Promotions and Seasonality  ##
       beta_Trend = pm.Normal('beta_Trend', mu= 0.01,sigma=0.005)     
       
       
       ### Other Flags ###
       # beta_July0422_lag = pm.Beta('beta_July0422_lag', alpha=3, beta=10) #pm.Normal('beta_July0422_lag', mu= 0.316,sigma=1)




       #################
       ## Setup Model ##
       #################


       ## Setup Error Model ##
       sd_y = pm.Normal("sd_y", mu=0, sigma = 0.2)

       ## Setup Expected Value ##
       y_hat = intercept_re[dma_idx] + beta_tv*tv_idx + beta_social*social_idx + beta_IcmDigital_Native_video*video_IcmDigital_Native_idx + beta_IcmDigital_YouTube_video*video_IcmDigital_YouTube_idx + beta_MediaCom_YouTube_video*video_MediaCom_YouTube_idx + beta_Trend*Trend_idx + beta_MediaCom_OLV_Brand_video*video_MediaCom_OLV_Brand_idx + beta_MediaCom_OLV_NonBrand_video*video_MediaCom_OLV_NonBrand_idx

       ## Setup Liklehood function ##
       y_like = pm.Normal('y_like', mu= y_hat, sigma=sd_y, observed=model_df['Search_contri_norm'], dims='obs_id')




# COMMAND ----------

## Running Bayesian Machinery ##
with varying_intercept:
  varying_intercept_trace = pm.sample(draws=1000,tune=100, chains=4, random_seed = RANDOM_SEED, progressbar=True)

# COMMAND ----------

# Model Summary #
summary_df = az.summary(varying_intercept_trace, round_to=5).reset_index()
summary_df.display()

# COMMAND ----------

# Plot Posterior Distribution #
pm.plot_trace(varying_intercept_trace)

# COMMAND ----------

### All Model Variables ###
map_dict = {


  'beta_social': 'Social_Imps_norm',
  'beta_tv': 'TV_Imps_AdStock6L3Wk90Ad_Power30', 

  # 'beta_video': 'Video_Imps_AdStock6L4Wk30Ad_Power30',        
  'beta_IcmDigital_Native_video': 'Video_IcmDigital_Native_Imps_AdStock6L4Wk30Ad_Power30',
  'beta_IcmDigital_YouTube_video': 'Video_IcmDigital_YouTube_Imps_AdStock6L4Wk30Ad_Power30',
  
  # 'beta_MediaCom_OLV_video': 'Video_MediaCom_OLV_Imps_AdStock6L4Wk30Ad_Power30',
  'beta_MediaCom_OLV_Brand_video': 'Video_MediaCom_OLV_Brand_Imps_AdStock6L4Wk30Ad_Power30',
  'beta_MediaCom_OLV_NonBrand_video': 'Video_MediaCom_OLV_NonBrand_Imps_AdStock6L4Wk30Ad_Power30',

  'beta_MediaCom_YouTube_video': 'Video_MediaCom_YouTube_Imps_AdStock6L4Wk30Ad_Power30',

  'beta_Trend': 'Trend',  

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
contribution = pd.concat([model_df[['DmaCode', 'Date', 'Joins', 'Search_contri_norm']], contribution], axis=1) ## Change Target Var

contribution['Random Intercept'] = contribution['DmaCode'].map(re_dict)
contribution.insert(4, 'Pred', contribution[input_vars + ['Random Intercept'] ].sum(axis=1))

contribution['Fixed Dma Effect'] = contribution['Random Intercept'].mean()
contribution['Random Dma Effect'] = contribution['Random Intercept'] - contribution['Fixed Dma Effect']

contribution['Residuals'] = contribution['Search_contri_norm'] - contribution['Pred']  ## Change Target Var
contribution['ratio_resid_join'] =  contribution['Residuals']/contribution['Search_contri_norm'] ## Change Target Var
contribution['Year'] = pd.to_datetime(contribution['Date']).dt.year

## Update name ##
contribution.rename(columns={'Joins':'Joins_unnorm', 'Search_contri_norm':'Joins'}, inplace=True) ## Change Target Var

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
seasoanlity_vars =  ['Trend']

Holiday_vars = []

Promo_vars = []

dummy_vars = []

##### Media Vars #####


media_vars = [
  'Social_Imps_norm',
  'TV_Imps_AdStock6L3Wk90Ad_Power30',      
     
  'Video_IcmDigital_Native_Imps_AdStock6L4Wk30Ad_Power30',
  'Video_IcmDigital_YouTube_Imps_AdStock6L4Wk30Ad_Power30',

  # 'Video_MediaCom_OLV_Imps_AdStock6L4Wk30Ad_Power30',
  'Video_MediaCom_OLV_Brand_Imps_AdStock6L4Wk30Ad_Power30',
  'Video_MediaCom_OLV_NonBrand_Imps_AdStock6L4Wk30Ad_Power30',
  
  'Video_MediaCom_YouTube_Imps_AdStock6L4Wk30Ad_Power30'


]

ch_name = 'Video_MediaCom'


######################################################################################
##################### Creating Agg. Analysis Dataframe ###############################
######################################################################################

agg_analysis_list = []

#############################
### Adding Media Variable ###
#############################

for media in media_vars:
  dict_ = {'ChannelLever': 'Membership', 'Channel': "_".join(media.split("_")[:4]) if "_".join(media.split("_")[:2])==ch_name else media.split("_")[0], 'Support': "_".join(media.split("_")[:5]) if "_".join(media.split("_")[:2])==ch_name else "_".join(media.split("_")[:2]), 'Transformation': media, 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
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

agg_analysis_df = agg_analysis_df.round(5)
agg_analysis_df

# COMMAND ----------

## Creating Report Df ##
summary_df['Transformation'] = summary_df['index'].map(map_dict)
report_df = agg_analysis_df.merge(summary_df, on='Transformation', how='left') 

report_df = report_df[report_df['Transformation'].isin(model_cols)][['Channel', 'Transformation', 'Spend_2022', 'Spend_2023', 'Contrib_2022_Prop', 'Contrib_2023_Prop', 'Support_2022', 'Support_2023', 'r_hat', 'ess_bulk', 'ess_tail']]

##################
###### 2023 ######
##################
LTV = 95.19

ch23_contri = 11320.2390217613
report_df23 = report_df[['Channel', 'Transformation','Contrib_2023_Prop',  'Support_2023', 'Spend_2023', 'r_hat', 'ess_bulk', 'ess_tail']]
report_df23['Contrib_2023_Prop'] = report_df['Contrib_2023_Prop']/report_df['Contrib_2023_Prop'].sum()

report_df23.insert(2, 'Contrib_2023', report_df23['Contrib_2023_Prop']*ch23_contri, True)
report_df23.insert(5, 'Effectiveness_2023', report_df23['Contrib_2023']/(report_df23['Support_2023']/1000000), True)
report_df23.insert(7, 'CostPerSupport_2023', report_df23['Spend_2023']/(report_df23['Support_2023']/1000000), True)
report_df23.insert(8, 'TotalDollarContri_2023', report_df23['Contrib_2023']*LTV, True)
report_df23.insert(9, 'roi_2023', (report_df23['Contrib_2023']*LTV)/(report_df23['Spend_2023']), True)
report_df23.drop(columns= ['Transformation']) .display()

# COMMAND ----------

##################
###### 2022 ######
##################
LTV = 95.19

ch22_contri = 12022.3495571639
report_df22 = report_df[['Channel', 'Transformation','Contrib_2022_Prop',  'Support_2022', 'Spend_2022', 'r_hat', 'ess_bulk', 'ess_tail']]
report_df22['Contrib_2022_Prop'] = report_df['Contrib_2022_Prop']/report_df['Contrib_2022_Prop'].sum()

report_df22.insert(2, 'Contrib_2022', report_df22['Contrib_2022_Prop']*ch22_contri, True)
report_df22.insert(5, 'Effectiveness_2022', report_df22['Contrib_2022']/(report_df22['Support_2022']/1000000), True)
report_df22.insert(7, 'CostPerSupport_2022', report_df22['Spend_2022']/(report_df22['Support_2022']/1000000), True)
report_df22.insert(8, 'TotalDollarContri_2022', report_df22['Contrib_2022']*LTV, True)
report_df22.insert(9, 'roi_2022', (report_df22['Contrib_2022']*LTV)/(report_df22['Spend_2022']), True)
report_df22.drop(columns= ['Transformation']) .display()

# COMMAND ----------



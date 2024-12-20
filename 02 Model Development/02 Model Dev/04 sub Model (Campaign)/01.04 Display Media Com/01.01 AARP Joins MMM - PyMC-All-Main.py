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

model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_Campaign_Display_MediaCom.csv').drop('Unnamed: 0',axis=1).fillna(0)
model_df.head()

# COMMAND ----------

## Creating Leadgen Target Variable ##
coeff = 0.21 ## Getting this coeff from bayesian model
model_df['Leadgen_contri'] = model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80']*coeff

## Re-Normalize variable ##
model_df['Leadgen_contri_norm'] = model_df['Leadgen_contri'].transform(min_max_normalize)


## Adding Seasonal Flags ##
model_df['Seasonality_Mar23'] = model_df['Date'].apply( lambda x: 1 if ((pd.to_datetime(x).month == 3) and (pd.to_datetime(x).year == 2023)) else 0 ) 
model_df['Seasonality_Apr23'] = model_df['Date'].apply( lambda x: 1 if ((pd.to_datetime(x).month == 4) and (pd.to_datetime(x).year == 2023)) else 0 )

model_df['Seasonality_Sep23'] = model_df['Date'].apply( lambda x: 1 if ((pd.to_datetime(x).month == 9) and (pd.to_datetime(x).year == 2023)) else 0 )
model_df['Seasonality_Oct23'] = model_df['Date'].apply( lambda x: 1 if ((pd.to_datetime(x).month == 10) and (pd.to_datetime(x).year == 2023)) else 0 )


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
model_path =  '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_submodel_LeadgenVo1.pkl'

with open(model_path, "rb") as f:
  model = pickle.load(f)

# Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1.04 Search SubModel: Frequentist Approach

# COMMAND ----------

input_vars = [ 
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'Display_Imps_AdStock6L1Wk80Ad_Power40',
'Print_Imps_AdStock13L1Wk30Ad_Power30', 
'AltMedia_Imps_AdStock6L5Wk30Ad_Power70',

'Seasonality_Mar23',
'Seasonality_Apr23',

'Seasonality_Sep23',
'Seasonality_Oct23',

         
       ]

# COMMAND ----------



# COMMAND ----------

##########################################################
######## Anlaysing Column Spend to set Prior Values ######
##########################################################

model_cols = [
'Display_MediaCom_Display_Brand_Imps_AdStock6L1Wk80Ad_Power40', 
'Display_MediaCom_Display_NonBrand_Imps_AdStock6L1Wk80Ad_Power40'

]

data_cols = [

'Display_MediaCom_Display_Brand_Spend',
'Display_MediaCom_Display_NonBrand_Spend'

]

sum_df = model_df.groupby(pd.to_datetime(model_df['Date']).dt.year)[data_cols].sum()
year_df = sum_df.sum(axis=1)

sum_df.div(year_df, axis=0).round(2).T.reset_index().display()
sum_df.round(2).T.reset_index().display()

# COMMAND ----------



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
       dm_idx = pm.MutableData('dm_idx', model_df['DirectMail_Imps_AdStock6L3Wk70Ad_Power90'].values, dims='obs_id')



       # disp_idx = pm.MutableData('disp_idx', model_df['Display_Imps_AdStock6L1Wk80Ad_Power40'].values, dims='obs_id')
       disp_ASI_Display_idx = pm.MutableData('disp_ASI_Display_idx', model_df['Display_ASI_Display_Imps_AdStock6L1Wk80Ad_Power40'].values, dims='obs_id')
       disp_IcmDigital_Display_idx = pm.MutableData('disp_IcmDigital_Display_idx', model_df['Display_IcmDigital_Display_Imps_AdStock6L1Wk80Ad_Power40'].values, dims='obs_id')
       # disp_IcmDigital_Other_idx = pm.MutableData('disp_IcmDigital_Other_idx', model_df['Display_IcmDigital_Other_Imps_AdStock6L1Wk80Ad_Power40'].values, dims='obs_id')
       

       # disp_MediaCom_Display_idx = pm.MutableData('disp_MediaCom_Display_idx', model_df['Display_MediaCom_Display_Imps_AdStock6L1Wk80Ad_Power40'].values, dims='obs_id')
       disp_MediaCom_Display_Brand_idx = pm.MutableData('disp_MediaCom_Display_Brand_idx', model_df['Display_MediaCom_Display_Brand_Imps_AdStock6L1Wk80Ad_Power40'].values, dims='obs_id')
       disp_MediaCom_Display_NonBrand_idx = pm.MutableData('disp_MediaCom_Display_NonBrand_idx', model_df['Display_MediaCom_Display_NonBrand_Imps_AdStock6L1Wk80Ad_Power40'].values, dims='obs_id')
       
       
       
       
       disp_MembershipDigital_AmazonDSP_idx = pm.MutableData('disp_MembershipDigital_AmazonDSP_idx', model_df['Display_MembershipDigital_AmazonDSP_Imps_AdStock6L1Wk80Ad_Power40'].values, dims='obs_id')
       disp_MembershipDigital_DV360_idx = pm.MutableData('disp_MembershipDigital_DV360_idx', model_df['Display_MembershipDigital_DV360_Imps_AdStock6L1Wk80Ad_Power40'].values, dims='obs_id')
       # disp_MembershipDigital_Outbrain_idx = pm.MutableData('disp_MembershipDigital_Outbrain_idx', model_df['Display_MembershipDigital_Outbrain_Imps_AdStock6L1Wk80Ad_Power40'].values, dims='obs_id')
       disp_MembershipDigital_Yahoo_idx = pm.MutableData('disp_MembershipDigital_Yahoo_idx', model_df['Display_MembershipDigital_Yahoo_Imps_AdStock6L1Wk80Ad_Power40'].values, dims='obs_id')
       
       
       
       
       print_idx = pm.MutableData('print_idx', model_df['Print_Imps_AdStock13L1Wk30Ad_Power30'].values, dims='obs_id')
       AltMedia_idx = pm.MutableData('AltMedia_idx', model_df['AltMedia_Imps_AdStock6L5Wk30Ad_Power70'].values, dims='obs_id')



       ## Promotions and Seasonality  ##

       Seasonality_Mar_idx = pm.MutableData('Seasonality_Mar_idx', model_df['Seasonality_Mar23'].values, dims='obs_id')
       Seasonality_Apr_idx = pm.MutableData('Seasonality_Apr_idx', model_df['Seasonality_Apr23'].values, dims='obs_id')

       Seasonality_Sep_idx = pm.MutableData('Seasonality_Sep_idx', model_df['Seasonality_Sep23'].values, dims='obs_id')
       Seasonality_Oct_idx = pm.MutableData('Seasonality_Oct_idx', model_df['Seasonality_Oct23'].values, dims='obs_id')

       

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
       beta_tv =    pm.Normal('beta_tv', mu= 0.230,sigma=0.01)      
       beta_dm =   pm.Normal('beta_dm', mu= 0.030,sigma=0.01)       
       
       0.0411
       # beta_disp =   pm.Normal('beta_disp', mu= 0.140,sigma=0.01)
       beta_ASI_Display_disp =   pm.Uniform('beta_ASI_Display_disp', lower= 0.140*0.14 - 0.01, upper= 0.140*0.14 + 0.01) #pm.Normal('beta_ASI_Display_disp', mu= 0.140/8,sigma=0.01)
       beta_IcmDigital_Display_disp =   pm.Uniform('beta_IcmDigital_Display_disp', lower= 0.140*0.1 - 0.01, upper= 0.140*0.1 + 0.01) #pm.Normal('beta_IcmDigital_Display_disp', mu= 0.140/8,sigma=0.01)
       # beta_IcmDigital_Other_disp =   pm.Uniform('beta_IcmDigital_Other_disp', lower= 0.140*0.001 - 0.0001, upper= 0.140*0.001 + 0.0001) #pm.Normal('beta_IcmDigital_Other_disp', mu= 0.140/8,sigma=0.01)
       
       # beta_MediaCom_Display_disp =   pm.Uniform('beta_MediaCom_Display_disp', lower= 0.140*0.42 - 0.02, upper= 0.140*0.42 + 0.02) #pm.Normal('beta_MediaCom_Display_disp', mu= 0.140/8,sigma=0.01)
       beta_MediaCom_Display_Brand_disp =   pm.Uniform('beta_MediaCom_Display_Brand_disp', lower= 0.140*0.42*0.55 - 0.02, upper= 0.140*0.42*0.55 + 0.02) #pm.Normal('beta_MediaCom_Display_disp', mu= 0.140/8,sigma=0.01)
       beta_MediaCom_Display_NonBrand_disp =   pm.Uniform('beta_MediaCom_Display_NonBrand_disp', lower= 0.140*0.42*0.45 - 0.02, upper= 0.140*0.42*0.45 + 0.02) #pm.Normal('beta_MediaCom_Display_disp', mu= 0.140/8,sigma=0.01)
       
       
       beta_MembershipDigital_AmazonDSP_disp =   pm.Uniform('beta_MembershipDigital_AmazonDSP_disp', lower= 0.140*0.02-0.0005, upper= 0.140*0.02+0.0005) #pm.Normal('beta_MembershipDigital_AmazonDSP_disp', mu= 0.140/8,sigma=0.01)
       beta_MembershipDigital_DV360_disp =   pm.Uniform('beta_MembershipDigital_DV360_disp', lower= 0.140*0.27 - 0.02, upper= 0.140*0.27 + 0.004) #pm.Normal('beta_MembershipDigital_DV360_disp', mu= 0.140/8,sigma=0.01)
       # beta_MembershipDigital_Outbrain_disp =   pm.Uniform('beta_MembershipDigital_Outbrain_disp', lower= 0.140*0.001 - 0.0001, upper= 0.140*0.001 + 0.0001) #pm.Normal('beta_MembershipDigital_Outbrain_disp', mu= 0.140/8,sigma=0.01)
       beta_MembershipDigital_Yahoo_disp =   pm.Uniform('beta_MembershipDigital_Yahoo_disp', lower= 0.140*0.06 - 0.004, upper= 0.140*0.06 + 0.004) #pm.Normal('beta_MembershipDigital_Yahoo_disp', mu= 0.140/8,sigma=0.01)
       
       
       
       
       beta_print =   pm.Normal('beta_print', mu= 0.028,sigma=0.010)
       beta_AltMedia = pm.Normal('beta_AltMedia', mu= 0.037,sigma=0.01)  

       ## Promotions and Seasonality  ##
       beta_Seasonality_Mar = pm.Normal('beta_Seasonality_Mar', mu= 0.280,sigma=0.01)       
       beta_Seasonality_Apr = pm.Normal('beta_Seasonality_Apr', mu= 0.380,sigma=0.01)       
       
       beta_Seasonality_Sep = pm.Normal('beta_Seasonality_Sep', mu= -0.317,sigma=0.01)       
       beta_Seasonality_Oct = pm.Normal('beta_Seasonality_Oct', mu= -0.118,sigma=0.01)       
       
       
       ### Other Flags ###
       # beta_July0422_lag = pm.Beta('beta_July0422_lag', alpha=3, beta=10) #pm.Normal('beta_July0422_lag', mu= 0.316,sigma=1)




       #################
       ## Setup Model ##
       #################


       ## Setup Error Model ##
       sd_y = pm.Normal("sd_y", mu=0, sigma = 0.2)

       ## Setup Expected Value ##
       y_hat = intercept_re[dma_idx] + beta_tv*tv_idx + beta_dm*dm_idx + beta_ASI_Display_disp*disp_ASI_Display_idx + beta_IcmDigital_Display_disp*disp_IcmDigital_Display_idx + beta_MediaCom_Display_Brand_disp*disp_MediaCom_Display_Brand_idx + beta_MediaCom_Display_NonBrand_disp*disp_MediaCom_Display_NonBrand_idx + beta_MembershipDigital_AmazonDSP_disp*disp_MembershipDigital_AmazonDSP_idx + beta_MembershipDigital_DV360_disp*disp_MembershipDigital_DV360_idx + beta_MembershipDigital_Yahoo_disp*disp_MembershipDigital_Yahoo_idx + beta_print*print_idx + beta_AltMedia*AltMedia_idx + beta_Seasonality_Mar*Seasonality_Mar_idx + beta_Seasonality_Apr*Seasonality_Apr_idx + beta_Seasonality_Sep*Seasonality_Sep_idx + beta_Seasonality_Oct*Seasonality_Oct_idx

       ## Setup Liklehood function ##
       y_like = pm.Normal('y_like', mu= y_hat, sigma=sd_y, observed=model_df['Leadgen_contri_norm'], dims='obs_id')




# COMMAND ----------

## Running Bayesian Machinery ##
with varying_intercept:
  varying_intercept_trace = pm.sample(draws=1000,tune=100, chains=4, random_seed = RANDOM_SEED, progressbar=True, target_accept=1)

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


'beta_Seasonality_Apr':'Seasonality_Apr23',
'beta_Seasonality_Mar':'Seasonality_Mar23',
'beta_Seasonality_Sep':'Seasonality_Sep23',
'beta_Seasonality_Oct':'Seasonality_Oct23',


'beta_tv': 'TV_Imps_AdStock6L2Wk70Ad_Power70',
'beta_dm': 'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',

# 'beta_disp': 'Display_Imps_AdStock6L1Wk80Ad_Power40',
'beta_ASI_Display_disp':'Display_ASI_Display_Imps_AdStock6L1Wk80Ad_Power40',
'beta_IcmDigital_Display_disp':'Display_IcmDigital_Display_Imps_AdStock6L1Wk80Ad_Power40',
# 'beta_IcmDigital_Other_disp':'Display_IcmDigital_Other_Imps_AdStock6L1Wk80Ad_Power40',


# 'beta_MediaCom_Display_disp':'Display_MediaCom_Display_Imps_AdStock6L1Wk80Ad_Power40',
'beta_MediaCom_Display_Brand_disp':'Display_MediaCom_Display_Brand_Imps_AdStock6L1Wk80Ad_Power40',
'beta_MediaCom_Display_NonBrand_disp':'Display_MediaCom_Display_NonBrand_Imps_AdStock6L1Wk80Ad_Power40',


'beta_MembershipDigital_AmazonDSP_disp':'Display_MembershipDigital_AmazonDSP_Imps_AdStock6L1Wk80Ad_Power40',
'beta_MembershipDigital_DV360_disp':'Display_MembershipDigital_DV360_Imps_AdStock6L1Wk80Ad_Power40',
'beta_MembershipDigital_Outbrain_disp':'Display_MembershipDigital_Outbrain_Imps_AdStock6L1Wk80Ad_Power40', 
'beta_MembershipDigital_Yahoo_disp':'Display_MembershipDigital_Yahoo_Imps_AdStock6L1Wk80Ad_Power40',


'beta_print': 'Print_Imps_AdStock13L1Wk30Ad_Power30',
'beta_AltMedia':'AltMedia_Imps_AdStock6L5Wk30Ad_Power70',

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
contribution = pd.concat([model_df[['DmaCode', 'Date', 'Joins', 'Leadgen_contri_norm']], contribution], axis=1) ## Change Target Var

contribution['Random Intercept'] = contribution['DmaCode'].map(re_dict)
contribution.insert(4, 'Pred', contribution[input_vars + ['Random Intercept'] ].sum(axis=1))

contribution['Fixed Dma Effect'] = contribution['Random Intercept'].mean()
contribution['Random Dma Effect'] = contribution['Random Intercept'] - contribution['Fixed Dma Effect']

contribution['Residuals'] = contribution['Leadgen_contri_norm'] - contribution['Pred']  ## Change Target Var
contribution['ratio_resid_join'] =  contribution['Residuals']/contribution['Leadgen_contri_norm'] ## Change Target Var
contribution['Year'] = pd.to_datetime(contribution['Date']).dt.year

## Update name ##
contribution.rename(columns={'Joins':'Joins_unnorm', 'Leadgen_contri_norm':'Joins'}, inplace=True) ## Change Target Var

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
seasoanlity_vars =  ['Seasonality_Mar23', 'Seasonality_Apr23', 'Seasonality_Sep23', 'Seasonality_Oct23']

Holiday_vars = []

Promo_vars = []

dummy_vars = []

##### Media Vars #####


media_vars = [
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',

# 'Display_Imps_AdStock6L1Wk80Ad_Power40',
'Display_ASI_Display_Imps_AdStock6L1Wk80Ad_Power40', 
'Display_IcmDigital_Display_Imps_AdStock6L1Wk80Ad_Power40', 
# 'Display_IcmDigital_Other_Imps_AdStock6L1Wk80Ad_Power40', 

# 'Display_MediaCom_Display_Imps_AdStock6L1Wk80Ad_Power40', 
'Display_MediaCom_Display_Brand_Imps_AdStock6L1Wk80Ad_Power40',
'Display_MediaCom_Display_NonBrand_Imps_AdStock6L1Wk80Ad_Power40',


'Display_MembershipDigital_AmazonDSP_Imps_AdStock6L1Wk80Ad_Power40', 
'Display_MembershipDigital_DV360_Imps_AdStock6L1Wk80Ad_Power40', 
# 'Display_MembershipDigital_Outbrain_Imps_AdStock6L1Wk80Ad_Power40', 
'Display_MembershipDigital_Yahoo_Imps_AdStock6L1Wk80Ad_Power40',


'Print_Imps_AdStock13L1Wk30Ad_Power30',
'AltMedia_Imps_AdStock6L5Wk30Ad_Power70'
]

ch_name = 'Display_MediaCom_Display'


######################################################################################
##################### Creating Agg. Analysis Dataframe ###############################
######################################################################################

agg_analysis_list = []

#############################
### Adding Media Variable ###
#############################

for media in media_vars:
  dict_ = {'ChannelLever': 'Membership', 'Channel': "_".join(media.split("_")[:4]) if "_".join(media.split("_")[:3])==ch_name else media.split("_")[0], 'Support': "_".join(media.split("_")[:5]) if "_".join(media.split("_")[:3])==ch_name else "_".join(media.split("_")[:2]), 'Transformation': media, 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
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

ch23_contri = 76609.2375935985
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

ch22_contri = 41088.656465345
report_df22 = report_df[['Channel', 'Transformation','Contrib_2022_Prop',  'Support_2022', 'Spend_2022', 'r_hat', 'ess_bulk', 'ess_tail']]
report_df22['Contrib_2022_Prop'] = report_df['Contrib_2022_Prop']/report_df['Contrib_2022_Prop'].sum()

report_df22.insert(2, 'Contrib_2022', report_df22['Contrib_2022_Prop']*ch22_contri, True)
report_df22.insert(5, 'Effectiveness_2022', report_df22['Contrib_2022']/(report_df22['Support_2022']/1000000), True)
report_df22.insert(7, 'CostPerSupport_2022', report_df22['Spend_2022']/(report_df22['Support_2022']/1000000), True)
report_df22.insert(8, 'TotalDollarContri_2022', report_df22['Contrib_2022']*LTV, True)
report_df22.insert(9, 'roi_2022', (report_df22['Contrib_2022']*LTV)/(report_df22['Spend_2022']), True)
report_df22.drop(columns= ['Transformation']) .display()

# COMMAND ----------



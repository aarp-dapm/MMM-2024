# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0 Model Development

# COMMAND ----------

############################
###### Reading Data ########
############################

model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_AgeGroup_statsmodel.csv').drop('Unnamed: 0',axis=1)
model_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Loading Overall Frequentist Model

# COMMAND ----------

## Reading Model ##
model_path =  '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo16_updatedData.pkl'

with open(model_path, "rb") as f:
  model = pickle.load(f)

# Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

## Reading Model ##
model_path =  '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_submodel_LeadgenVo1.pkl'

with open(model_path, "rb") as f:
  model = pickle.load(f)

# Model Summary
model.summary().tables[1].reset_index().display()

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
# MAGIC #### 1.2 Best Leadgen SubModel: Frequentist Approach

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
       disp_idx = pm.MutableData('disp_idx', model_df['Display_Imps_AdStock6L1Wk80Ad_Power40'].values, dims='obs_id')
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

       ## Randomizing Email ##
       mu_re_ch = pm.Normal('mu_re_ch', mu=0.140, sigma=0.1)
       sigma_re_ch = pm.Exponential('sigma_re_ch',0.01)




       beta_tv =    pm.Normal('beta_tv', mu= 0.230,sigma=0.01)           
       beta_dm =   pm.Normal('beta_dm', mu= 0.030,sigma=0.01)            
       beta_disp_re =   pm.Normal('beta_disp_re', mu= mu_re_ch,sigma=sigma_re_ch, dims='DmaCodes')           # beta_disp =   pm.Normal('beta_disp', mu= 0.140,sigma=0.01)
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
       y_hat = intercept_re[dma_idx] + beta_tv*tv_idx + beta_dm*dm_idx + beta_disp_re[dma_idx]*disp_idx + beta_print*print_idx + beta_AltMedia*AltMedia_idx + beta_Seasonality_Mar*Seasonality_Mar_idx + beta_Seasonality_Apr*Seasonality_Apr_idx + beta_Seasonality_Sep*Seasonality_Sep_idx + beta_Seasonality_Oct*Seasonality_Oct_idx
       #   + beta_July0422_lag*July0422_lag_idx

       ## Setup Liklehood function ##
       y_like = pm.Normal('y_like', mu= y_hat, sigma=sd_y, observed=model_df['Leadgen_contri_norm'], dims='obs_id')




# COMMAND ----------

## Running Bayesian Machinery ##
with varying_intercept:
  varying_intercept_trace = pm.sample(draws=1000,tune=100, chains=2, random_seed = RANDOM_SEED, progressbar=True, target_accept=0.99)

# COMMAND ----------

# Model Summary #
summary_df = az.summary(varying_intercept_trace, round_to=10).reset_index()
summary_df.display()

# COMMAND ----------

# Plot Posterior Distribution #
pm.plot_trace(varying_intercept_trace)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Saving Data

# COMMAND ----------

#################################
### Enter Channel Coeff. Name ###
#################################

ch_name = 'beta_disp'

# COMMAND ----------

### Keeping Dict of Model Variables ###
map_dict = {

'beta_Seasonality_Apr':'Seasonality_Apr23',
'beta_Seasonality_Mar':'Seasonality_Mar23',
'beta_Seasonality_Sep':'Seasonality_Sep23',
'beta_Seasonality_Oct':'Seasonality_Oct23',


'beta_tv': 'TV_Imps_AdStock6L2Wk70Ad_Power70',
'beta_dm': 'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'beta_disp': 'Display_Imps_AdStock6L1Wk80Ad_Power40',
'beta_print': 'Print_Imps_AdStock13L1Wk30Ad_Power30',
'beta_AltMedia':'AltMedia_Imps_AdStock6L5Wk30Ad_Power70',


}

#########################################
### Preparing Channel Contribution Df ###
#########################################



## Channel Coeff. from Model ##
re_coeff_df = summary_df[summary_df['index'].str.contains(ch_name)][['index', 'mean']] #1

re_coeff_df['DmaCode'] = re_coeff_df['index'].str.split('[').str[1].str.split(']').str[0]
re_coeff_df['DmaCode'] = re_coeff_df['DmaCode'].astype('int64')
re_coeff_df = re_coeff_df[['DmaCode', 'mean']]

re_coeff_df[ch_name] = re_coeff_df['mean']


## Support Data from Model Data ##
ch_support = "_".join(map_dict[ch_name].split("_")[:2])
support_df = model_df[['Date', 'DmaCode', ch_support]]

## Merging Data ##
support_df = support_df.merge(re_coeff_df, on='DmaCode', how='left')
support_df['Contri'] = support_df['mean']*support_df[ch_support]

## DMA COntribution Proportion for 2023 ##
norm_df = support_df[pd.to_datetime(support_df['Date']).dt.year == 2023][['Date', 'DmaCode', 'Contri', ch_name]]
norm_df = norm_df.groupby('DmaCode').agg({'Contri':'sum', ch_name:'mean'}).reset_index()

norm_df[ch_name+'_Contri'] = (norm_df['Contri']/norm_df['Contri'].sum()).round(10) 
norm_df.drop(columns=['Contri'], inplace=True)

## Adding Pouplation Prop ##
norm_df['Population_Prop'] = norm_df['DmaCode'].astype('str').map(norm_ch_dma_code_prop)
norm_df.display()

# COMMAND ----------

## Save Dataframe ##
norm_df.to_csv(f'/dbfs/blend360/sandbox/mmm/model/dma_analysis_{ch_name}.csv')

## Read Dataframe ##
norm_df  = pd.read_csv(f'/dbfs/blend360/sandbox/mmm/model/dma_analysis_{ch_name}.csv').drop('Unnamed: 0',axis=1)
norm_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Plots

# COMMAND ----------

plt.figure(figsize=(25, 15))
plt.scatter(norm_df['Population_Prop'], norm_df[ch_name])

plt.axhline(y=0.140, color = 'r', linestyle= '--', label = 'Prior')
plt.axvline(x=0.009088006, color = 'g', linestyle= '--', label = '50% Population')

plt.xlabel('Popualtion Proportion')
plt.ylabel('Contribution Coeff')
# plt.ylabel('Contribution Proportion')

plt.legend(loc='best')
plt.show()

# COMMAND ----------

plt.figure(figsize=(25, 15))
plt.scatter(norm_df['Population_Prop'], norm_df[ch_name+'_Contri'])

plt.axvline(x=0.009088006, color = 'g', linestyle= '--', label = '50% Population')

plt.xlabel('Popualtion Proportion')
plt.ylabel('Contribution Proportion')

plt.legend(loc='best')
plt.show()

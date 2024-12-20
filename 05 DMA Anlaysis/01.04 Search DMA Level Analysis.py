# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Model Development

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
# MAGIC ## 1.1 Loading Frequentist Model

# COMMAND ----------

## Reading Model ##
model_path =  '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo16_updatedData.pkl'

with open(model_path, "rb") as f:
  model = pickle.load(f)

#Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Best Model: Frequentist Approach

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
# MAGIC ## 1.3 Bayesian Model

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
       mu_re = pm.Normal('mu_re', mu=0, sigma=10)
       sigma_re = pm.Exponential('sigma_re',1)

       intercept_re = pm.Normal('intercept_re', mu=mu_re, sigma=sigma_re, dims='DmaCodes')

       ## Fixed Effect ##
       
       ### Media Variables ###

       ## Randomizing Search ##
       mu_re_ch = pm.Normal('mu_re_ch', mu=0.085, sigma=0.1)
       sigma_re_ch = pm.Exponential('sigma_re_ch',0.005)

       
       beta_tv =    pm.Normal('beta_tv', mu= 0.037,sigma=0.005)  
       beta_search_re = pm.Normal('beta_search_re', mu = mu_re_ch, sigma=sigma_re_ch, dims='DmaCodes')   # beta_search =  pm.Normal('beta_search', mu= 0.085,sigma=0.005)      
       beta_social =  pm.Normal('beta_social', mu= 0.029,sigma=0.007)     
       
       beta_leadgen =  pm.Normal('beta_leadgen', mu= 0.21,sigma=0.007)      
       beta_email = pm.Normal('beta_email', mu= 0.059,sigma=0.006)        
       beta_dm =   pm.Normal('beta_dm', mu= 0.082,sigma=0.005)            
       beta_AltMedia = pm.Normal('beta_AltMedia', mu= 0.084,sigma=0.004)  

       ## Promotions and Seasonality  ##
       beta_PromoEvnt_22MembDrive = pm.Normal('beta_PromoEvnt_22MembDrive', mu= 0.257,sigma=0.011)  
       beta_PromoEvnt_22BlackFriday = pm.Normal('beta_PromoEvnt_22BlackFriday', mu= 0.416,sigma=0.011) 
       beta_PromoEvnt_23MembDrive = pm.Normal('beta_PromoEvnt_23MembDrive', mu= 0.306,sigma=0.007)  
       beta_PromoEvnt_23BlackFriday = pm.Normal('beta_PromoEvnt_23BlackFriday', mu= 0.370,sigma=0.01) 


       beta_Seasonality_Feb = pm.Normal('beta_Seasonality_Feb', mu= 0.117,sigma=0.004)       
       beta_Seasonality_Mar = pm.Normal('beta_Seasonality_Mar', mu= 0.086,sigma=0.004)       
       beta_Seasonality_Apr = pm.Normal('beta_Seasonality_Apr', mu= -0.038,sigma=0.004)       
       beta_Seasonality_Dec = pm.Normal('beta_Seasonality_Dec', mu= -0.128,sigma=0.004)       
       
       
       ### Other Flags ###
       beta_July0422_lag = pm.Beta('beta_July0422_lag', alpha=3, beta=10)




       #################
       ## Setup Model ##
       #################


       ## Setup Error Model ##
       sd_y = pm.Normal("sd_y", mu=0, sigma = 0.2)

       ## Setup Expected Value ##
       y_hat = intercept_re[dma_idx] + beta_tv*tv_idx + beta_search_re[dma_idx]*search_idx + beta_social*social_idx + beta_leadgen*leadgen_idx + beta_email*email_idx + beta_dm*dm_idx + beta_AltMedia*AltMedia_idx + beta_PromoEvnt_22MembDrive*PromoEvnt_22MembDrive_idx + beta_PromoEvnt_22BlackFriday*PromoEvnt_22BlackFriday_idx + beta_PromoEvnt_23MembDrive*PromoEvnt_23MembDrive_idx + beta_PromoEvnt_23BlackFriday*PromoEvnt_23BlackFriday_idx + beta_Seasonality_Mar*Seasonality_Mar_idx + beta_Seasonality_Apr*Seasonality_Apr_idx + beta_Seasonality_Feb*Seasonality_Feb_idx + beta_Seasonality_Dec*Seasonality_Dec_idx + beta_July0422_lag*July0422_lag_idx

       ## Setup Liklehood function ##
       y_like = pm.Normal('y_like', mu= y_hat, sigma=sd_y, observed=model_df['Joins_norm'], dims='obs_id')




# COMMAND ----------

## Running Bayesian Machinery ##
with varying_intercept:
  varying_intercept_trace = pm.sample(draws=1000,tune=100, chains=2, random_seed = RANDOM_SEED, progressbar=True)

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

ch_name = 'beta_search'

# COMMAND ----------

### Keeping Dict of Model Variables ###
map_dict = {

'beta_dm': 'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'beta_AltMedia':'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
'beta_email':'Email_Spend_AdStock6L1Wk20Ad_Power90',
'beta_leadgen':'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'beta_search':'Search_Clicks_AdStock6L1Wk90Ad_Power90',
'beta_social':'Social_Imps_AdStock6L1Wk90Ad_Power90',
'beta_tv': 'TV_Imps_AdStock6L2Wk70Ad_Power70',

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
# plt.scatter(norm_df['Population_Prop'], norm_df[ch_name+'_Contri'])
plt.scatter(norm_df['Population_Prop'], norm_df[ch_name])

plt.axhline(y=0.029, color = 'r', linestyle= '--', label = 'Prior')
plt.axvline(x=0.0085, color = 'g', linestyle= '--', label = '50% Population')

plt.xlabel('Popualtion Proportion')
plt.ylabel('Contribution Coeff')
# plt.ylabel('Contribution Proportion')

plt.legend(loc='best')
plt.show()

# COMMAND ----------

plt.figure(figsize=(25, 15))
plt.scatter(norm_df['Population_Prop'], norm_df[ch_name+'_Contri'])
# plt.scatter(norm_df['Population_Prop'], norm_df[ch_name])

# plt.axhline(y=0.029, color = 'r', linestyle= '--', label = 'Prior')
plt.axvline(x=0.085, color = 'g', linestyle= '--', label = '50% Population')

plt.xlabel('Popualtion Proportion')
# plt.ylabel('Contribution Coeff')
plt.ylabel('Contribution Proportion')

plt.legend(loc='best')
plt.show()

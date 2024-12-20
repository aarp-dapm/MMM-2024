# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Data and Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Reading Data

# COMMAND ----------

'''
Saving and Reading Model Data
'''

## Reading Model ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_reg_df_MembershipFlag.csv').drop('Unnamed: 0',axis=1)
model_df.head()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Creating Feature List

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Top 4 features for variables

# COMMAND ----------

## Model Building Columns Partial Inventory ##
primary_cols = ['DmaCode', 'Date']
target_col = ['Joins']
AllMediaModel_col = ['Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps','LeadGen_Imps',
       'Radio_Spend', 'OOH_Imps', 'Print_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps'] 


## Fixing Some Media Cols from above ##
VarMediaCol = ['AltMedia_Imps', 'DirectMail_Imps', 'Display_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps']
FixedMediaCol = ['Affiliate_Imps', 'Audio_Imps', 'DMP_Imps', 'Email_Imps', 'LeadGen_Imps', 'Radio_Spend', 'OOH_Imps', 'Print_Imps', 'Video_Imps']



'''

Getting channels transformation with best correlation to start with

'''

## Getting best variable for each channel ##
non_transformed_cols_list = AllMediaModel_col
transformed_cols_key_value_pair = {col:[col2 for col2 in list(model_df.columns) if col2.startswith(col+"_") ]  for col in non_transformed_cols_list} 

## Getting Correlation of Each Tranformation with Target Variable ##
corr_dict = {}
for key in transformed_cols_key_value_pair:

  idx = transformed_cols_key_value_pair[key]
  corr_ch = model_df[idx+['Joins']].corr().loc[idx,'Joins'].sort_values(ascending=False)
  corr_dict[key] = corr_ch.to_dict() 

## Getting Best Correlation One ##
corr_dict_best = {}
for key in corr_dict:
  
  if key in FixedMediaCol:
    corr_dict_best[key] = list(corr_dict[key].keys())[0]

  if key in VarMediaCol:
    corr_dict_best[key] = list(corr_dict[key].keys())[:4] ## Getting top4 for channels needed to be searched exhaustively

corr_dict_best.values()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Creating combination for feature variations

# COMMAND ----------

## Geeting only Varying Media Cols ##
VarMediaCol_dict = {

  'AltMedia_Imps_AdStock6L1Wk90Ad_Power90':['AltMedia_Imps_AdStock6L1Wk90Ad_Power90', 'AltMedia_Imps_AdStock6L2Wk90Ad_Power90', 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90', 'AltMedia_Imps_AdStock6L4Wk90Ad_Power90'], 

  'DirectMail_Imps_AdStock6L1Wk90Ad_Power70':['DirectMail_Imps_AdStock6L1Wk90Ad_Power70', 'DirectMail_Imps_AdStock6L2Wk90Ad_Power70', 'DirectMail_Imps_AdStock6L3Wk90Ad_Power70', 'DirectMail_Imps_AdStock6L4Wk90Ad_Power70'], 
  
  'Display_Imps_AdStock6L1Wk30Ad_Power30':['Display_Imps_AdStock6L1Wk30Ad_Power30', 'Display_Imps_AdStock6L2Wk30Ad_Power30', 'Display_Imps_AdStock6L3Wk30Ad_Power30', 'Display_Imps_AdStock6L4Wk30Ad_Power30'], 
  
  'Social_Imps_AdStock6L1Wk30Ad_Power30':['Social_Imps_AdStock6L1Wk30Ad_Power30', 'Social_Imps_AdStock6L2Wk30Ad_Power30', 'Social_Imps_AdStock6L3Wk30Ad_Power30','Social_Imps_AdStock6L4Wk30Ad_Power30'], 
  
  'TV_Imps_AdStock6L1Wk90Ad_Power30':['TV_Imps_AdStock6L1Wk90Ad_Power30', 'TV_Imps_AdStock6L2Wk90Ad_Power30', 'TV_Imps_AdStock6L3Wk90Ad_Power30', 'TV_Imps_AdStock6L4Wk90Ad_Power30'], 
  
  'Video_Imps_AdStock6L1Wk30Ad_Power30':['Video_Imps_AdStock6L1Wk30Ad_Power30', 'Video_Imps_AdStock6L2Wk30Ad_Power30', 'Video_Imps_AdStock6L3Wk30Ad_Power30', 'Video_Imps_AdStock6L4Wk30Ad_Power30']
  
  }

##Creating list of combinations ##
lists = VarMediaCol_dict.values()
combinations = list(itertools.product(*lists))
len(combinations)

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2.3 Running Models: With All Channels

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3.1 Running Models: With All Channels

# COMMAND ----------

####################
## Model Creation ##
####################

seasoanlity_vars =  ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6'] # Double Check on Fourier Frequencies 
Holiday_vars = ['Holiday_22Christams', 'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'Holiday_23Christams']
Promo_vars = ['PromoEvnt_22MembDrive','PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow', 'PromoEvnt_22TikTok', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23RollingStone']
dummy_vars = ['dummy_20220711']

FixedMediaCol = ['Affiliate_Imps_AdStock6L5Wk90Ad_Power90', 'DMP_Imps_AdStock13L1Wk30Ad_Power90', 'Radio_Spend_AdStock13L1Wk30Ad_Power30', 'OOH_Imps_AdStock13L1Wk30Ad_Power30', 'Print_Imps_AdStock13L1Wk30Ad_Power30', 'Search_Clicks_AdStock6L1Wk80Ad_Power30', 'Audio_Imps_AdStock6L1Wk90Ad_Power30', 'Email_Imps_AdStock6L1Wk80Ad_Power30', 'LeadGen_Imps_AdStock6L1Wk90Ad_Power90']



input_vars = seasoanlity_vars + Holiday_vars + Promo_vars + dummy_vars + FixedMediaCol 

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}
for idx,combination in enumerate(combinations):

  input_vars_str = " + ".join(input_vars+list(combination))
  fit_model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit()

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Combination'] = combination
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()

  ## Saving Model ##
  model_path = f'/dbfs/blend360/sandbox/mmm/model/6_14_MMM_StatsModel/6_14_MMM_StatsModel_{idx}.pkl'
  contri_dict[idx]['Path'] = model_path
  with open(model_path,"wb") as f:
    pickle.dump(fit_model, f)

 
  
  

  

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_14_MMM_StatsModel/6_14_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)



# COMMAND ----------

# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_14_MMM_StatsModel/6_14_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2.4 Running Models: Removing --> Radio, Audio, Affiliate, Print, OOH, DMP

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4.1 Running Models: Removing --> Radio, Audio, Affiliate, Print, OOH, DMP
# MAGIC

# COMMAND ----------

####################
## Model Creation ##
####################

seasoanlity_vars =  ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6'] # Double Check on Fourier Frequencies 
Holiday_vars = ['Holiday_22Christams', 'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'Holiday_23Christams']
Promo_vars = ['PromoEvnt_22MembDrive','PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow', 'PromoEvnt_22TikTok', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23RollingStone']
dummy_vars = ['dummy_20220711']

FixedMediaCol = [ 'Search_Clicks_AdStock6L1Wk80Ad_Power30', 'Email_Imps_AdStock6L1Wk80Ad_Power30', 'LeadGen_Imps_AdStock6L1Wk90Ad_Power90']



input_vars = seasoanlity_vars + Holiday_vars + Promo_vars + dummy_vars + FixedMediaCol 

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}
for idx,combination in enumerate(combinations):

  input_vars_str = " + ".join(input_vars+list(combination))
  fit_model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit()

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Combination'] = combination
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()

  ## Saving Model ##
  model_path = f'/dbfs/blend360/sandbox/mmm/model/6_15_MMM_StatsModel/6_15_MMM_StatsModel_{idx}.pkl'
  contri_dict[idx]['Path'] = model_path
  with open(model_path,"wb") as f:
    pickle.dump(fit_model, f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_15_MMM_StatsModel/6_15_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_15_MMM_StatsModel/6_15_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2.5 Exhaustive Search on Model Vo5 for Search

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5.1 Exhaustive Search on Model Vo5 for Search

# COMMAND ----------

#######################
## Collecting Search ##
#######################

search_keyword = 'Search_Clicks_AdStock'
search_list = []

for cols in model_df.columns:
  if cols.startswith(search_keyword):
    search_list.append(cols)

# COMMAND ----------

####################
## Model Creation ##
####################

seasoanlity_vars =  ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6'] # Double Check on Fourier Frequencies 

Holiday_vars = ['Holiday_22Christams', 'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'Holiday_23Christams']

Promo_vars = ['PromoEvnt_22MembDrive','PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow', 'PromoEvnt_23RollingStone'] # Removed 'PromoEvnt_22TikTok', 'PromoEvnt_23KaylaCoupon'

dummy_vars = ['dummy_20220711', 'Peak_20220228', 'dummy_20221010', 'Peak_20221128', 'Peak_20230227', 'Peak_20231127']

##### Media Vars #####

'''
Removing Search and Video
'''

fixed_media_vars = ['Email_Imps_AdStock6L1Wk80Ad_Power30', 'LeadGen_Imps_AdStock6L1Wk90Ad_Power90','AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
'DirectMail_Imps_AdStock6L4Wk90Ad_Power70', 'Social_Imps_AdStock6L4Wk30Ad_Power30', 'TV_Imps_AdStock6L1Wk90Ad_Power30', 'Video_Imps_AdStock6L2Wk30Ad_Power30']

media_vars = fixed_media_vars


########################
## Running all models ##
########################


# input_vars = seasoanlity_vars + Holiday_vars + Promo_vars + dummy_vars + media_vars 
input_vars =Promo_vars + dummy_vars + media_vars 


model_dict = {}
contri_dict = {}
for idx,combination in enumerate(search_list):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit()

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Combination'] = combination
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()

  ## Saving Model ##
  model_path = f'/dbfs/blend360/sandbox/mmm/model/6_19_MMM_StatsModel_Search/6_19_MMM_StatsModel_{idx}.pkl'
  contri_dict[idx]['Path'] = model_path
  with open(model_path,"wb") as f:
    pickle.dump(fit_model, f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_19_MMM_StatsModel_Search/6_19_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_19_MMM_StatsModel_Search/6_19_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2.6 Exhaustive Search on Model Vo5 for Video

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6.1 Exhaustive Search on Model Vo5 for Video

# COMMAND ----------

#######################
## Collecting Video ##
#######################

video_keyword = 'Video_Imps_AdStock'
video_list = []

for cols in model_df.columns:
  if cols.startswith(video_keyword):
    video_list.append(cols)

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay',
       'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday',
       'PromoEvnt_22WsjTodayShow', 'PromoEvnt_23RollingStone',
       'dummy_20220711', 
      #  'Peak_20220228', 
      #  'dummy_20221010',
      #  'Peak_20221128', 
      #  'Peak_20230227', 
      #  'Peak_20231127',
       'Email_Imps_AdStock6L1Wk80Ad_Power30',
       'LeadGen_Imps_AdStock6L1Wk90Ad_Power90',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
       'DirectMail_Imps_AdStock6L4Wk90Ad_Power70',
       'Social_Imps_AdStock6L4Wk30Ad_Power30',
       'TV_Imps_AdStock6L1Wk90Ad_Power30',
       'Search_Clicks_AdStock13L1Wk90Ad_Power90',]



########################
## Running all models ##
########################


# input_vars = seasoanlity_vars + Holiday_vars + Promo_vars + dummy_vars + media_vars 
# input_vars =Promo_vars + dummy_vars + media_vars 


model_dict = {}
contri_dict = {}
for idx,combination in enumerate(video_list):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit()

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Combination'] = combination
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()

  ## Saving Model ##
  model_path = f'/dbfs/blend360/sandbox/mmm/model/6_19_MMM_StatsModel_Video/6_19_MMM_StatsModel_{idx}.pkl'
  contri_dict[idx]['Path'] = model_path
  with open(model_path,"wb") as f:
    pickle.dump(fit_model, f)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_19_MMM_StatsModel_Video/6_19_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_19_MMM_StatsModel_Video/6_19_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2.7 Exhaustive Search on Model Vo5 for Video : Removed all Dummies

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7.1 Exhaustive Search on Model Vo5 for Video : Removed all Dummies

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay',
       'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday',
       'PromoEvnt_22WsjTodayShow', 'PromoEvnt_23RollingStone',
      #  'dummy_20220711', 
      #  'Peak_20220228', 
      #  'dummy_20221010',
      #  'Peak_20221128', 
      #  'Peak_20230227', 
      #  'Peak_20231127',
       'Email_Imps_AdStock6L1Wk80Ad_Power30',
       'LeadGen_Imps_AdStock6L1Wk90Ad_Power90',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
       'DirectMail_Imps_AdStock6L4Wk90Ad_Power70',
       'Social_Imps_AdStock6L4Wk30Ad_Power30',
       'TV_Imps_AdStock6L1Wk90Ad_Power30',
       'Search_Clicks_AdStock13L1Wk90Ad_Power90',]



########################
## Running all models ##
########################


# input_vars = seasoanlity_vars + Holiday_vars + Promo_vars + dummy_vars + media_vars 
# input_vars =Promo_vars + dummy_vars + media_vars 


model_dict = {}
contri_dict = {}
for idx,combination in enumerate(video_list):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit()

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Combination'] = combination
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()

  ## Saving Model ##
  model_path = f'/dbfs/blend360/sandbox/mmm/model/6_20_MMM_StatsModel_Video/6_20_MMM_StatsModel_{idx}.pkl'
  contri_dict[idx]['Path'] = model_path
  with open(model_path,"wb") as f:
    pickle.dump(fit_model, f)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_20_MMM_StatsModel_Video/6_20_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_20_MMM_StatsModel_Video/6_20_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2.8 Removng Video and Searching over Social

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.8.1 Removng Video and Searching over Social

# COMMAND ----------

#######################
## Collecting Social ##
#######################

social_keyword = 'Social_Imps_AdStock'
social_list = []

for cols in model_df.columns:
  if cols.startswith(social_keyword):
    social_list.append(cols)

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay',
       'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday',
       'PromoEvnt_22WsjTodayShow', 'PromoEvnt_23RollingStone',

       'Email_Imps_AdStock6L1Wk80Ad_Power30',
       'LeadGen_Imps_AdStock6L1Wk90Ad_Power90',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
       'DirectMail_Imps_AdStock6L4Wk90Ad_Power70',
       'TV_Imps_AdStock6L1Wk90Ad_Power30',
       'Search_Clicks_AdStock13L1Wk90Ad_Power90',]


########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}
for idx,combination in enumerate(social_list):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit()

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Combination'] = combination
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()

  ## Saving Model ##
  model_path = f'/dbfs/blend360/sandbox/mmm/model/6_20_MMM_StatsModel_Social/6_20_MMM_StatsModel_{idx}.pkl'
  contri_dict[idx]['Path'] = model_path
  with open(model_path,"wb") as f:
    pickle.dump(fit_model, f)  


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.8.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_20_MMM_StatsModel_Social/6_20_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_20_MMM_StatsModel_Social/6_20_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2.9 Grid Search for Social. Removed Email

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.9.1 Grid Search for Social. Removed Email

# COMMAND ----------

#######################
## Collecting Social ##
#######################

social_keyword = 'Social_Imps_AdStock'
social_list = []

for cols in model_df.columns:
  if cols.startswith(social_keyword):
    social_list.append(cols)

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay',
       'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday',
       'PromoEvnt_22WsjTodayShow', 'PromoEvnt_23RollingStone',

       'dummy_20220711','Peak_20220228', 'Peak_20230227',
        'dummy_20221010','Peak_20231127',
         'Peak_20221128', 
       
      #  'Email_Imps_AdStock6L1Wk80Ad_Power30',
       'LeadGen_Imps_AdStock6L1Wk90Ad_Power90',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
       'DirectMail_Imps_AdStock6L4Wk90Ad_Power70',
       'TV_Imps_AdStock6L1Wk90Ad_Power30',
       'Search_Clicks_AdStock13L1Wk90Ad_Power90',
       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}
for idx,combination in enumerate(social_list):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit()

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Combination'] = combination
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()

  ## Saving Model ##
  model_path = f'/dbfs/blend360/sandbox/mmm/model/6_20_MMM_StatsModel_Social2/6_20_MMM_StatsModel_{idx}.pkl'
  contri_dict[idx]['Path'] = model_path
  with open(model_path,"wb") as f:
    pickle.dump(fit_model, f)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.9.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_20_MMM_StatsModel_Social2/6_20_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_20_MMM_StatsModel_Social2/6_20_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2.10 Checking for Combination of Socials with less VIF
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.10.1 Checking for Combination of Socials with less VIF

# COMMAND ----------

#######################
## Collecting Social ##
#######################

social_keyword1 = 'Social_Imps_AdStock'
social_keyword2 = 'Social_Imps_AdStock13'
social_list = []

for cols in model_df.columns:
  if cols.startswith(social_keyword1) & ~(cols.endswith('UnitMean')) :
    social_list.append(cols)

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 
              # 'PromoEvnt_22MemorialDay', 
              'PromoEvnt_22LaborDay', 
              'PromoEvnt_22BlackFriday',
              # 'PromoEvnt_22WsjTodayShow', 
              'PromoEvnt_23RollingStone',
              
              # 'Seasonality_Sin5', 
              # 'Seasonality_Cos5',
              # 'Seasonality_Sin6', 
              # 'Seasonality_Cos6',

              'dummy_20220711',


              # 'Peak_20230227',
              # 'dummy_20221010',

              

        


            'LeadGen_Imps_AdStock6L1Wk90Ad_Power30',
            'AltMedia_Imps_AdStock6L2Wk90Ad_Power50',
            'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
            'TV_Imps_AdStock6L2Wk60Ad_Power60',
            'Search_Clicks_AdStock6L1Wk90Ad_Power30',
            # 'Social_Imps_AdStock6L2Wk90Ad_Power70'

       ]
########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}
for idx,combination in tqdm.tqdm(enumerate(social_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data





# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.10.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_26_MMM_StatsModel_Social/6_26_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_26_MMM_StatsModel_Social/6_26_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2.11 checking TV on previous best iteration. Removed Labor day as well
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.11.1 checking TV on previous best iteration. Removed Labor day as well

# COMMAND ----------

#######################
## Collecting TV ##
#######################

tv_keyword = 'TV_Imps_AdStock'
tv_list = []

for cols in model_df.columns:
  if cols.startswith(tv_keyword) & ~(cols.endswith('UnitMean')) :
    tv_list.append(cols)

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 
              # 'PromoEvnt_22LaborDay',
              
       'PromoEvnt_22BlackFriday', 
       'PromoEvnt_23RollingStone',
       'dummy_20220711', 'LeadGen_Imps_AdStock6L1Wk90Ad_Power30',
       'AltMedia_Imps_AdStock6L2Wk90Ad_Power50',
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
      #  'TV_Imps_AdStock6L2Wk60Ad_Power60',
       'Search_Clicks_AdStock6L1Wk90Ad_Power30',
       'Social_Imps_AdStock6L1Wk60Ad_Power90'

       ]
########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(tv_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data







# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.11.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_26_MMM_StatsModel_TV/6_26_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_26_MMM_StatsModel_TV/6_26_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)



'''

Best Model Chosen 81 and  326

'''

# COMMAND ----------

# MAGIC %md 
# MAGIC # 2.12 Searching for Best Search from 2.11

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.12.1 Searching for Best Search from 2.11

# COMMAND ----------

#######################
## Collecting Search ##
#######################

s_keyword = 'Search_Clicks_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 
              # 'PromoEvnt_22LaborDay',
              
       'PromoEvnt_22BlackFriday', 
       'PromoEvnt_23RollingStone',
       'dummy_20220711', 'LeadGen_Imps_AdStock6L1Wk90Ad_Power30',
       'AltMedia_Imps_AdStock6L2Wk90Ad_Power50',
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
      #  'Search_Clicks_AdStock6L1Wk90Ad_Power30',
       'Social_Imps_AdStock6L1Wk60Ad_Power90'

       ]
########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data





# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.12.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_26_MMM_StatsModel_Search/6_26_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_26_MMM_StatsModel_Search/6_26_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)



'''
Model Number 39

'''

# COMMAND ----------

'''

Best_model from 6/26/2024


input_vars = ['PromoEvnt_22MembDrive', 
              
       'PromoEvnt_22BlackFriday', 
       'PromoEvnt_23RollingStone',
       'dummy_20220711', 
       'LeadGen_Imps_AdStock6L1Wk90Ad_Power30',
       'AltMedia_Imps_AdStock6L2Wk90Ad_Power50',
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
       'Search_Clicks_AdStock6L1Wk80Ad_Power70',
       'Social_Imps_AdStock6L1Wk60Ad_Power90'

       ]

'''


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # 2.13 : Reducing Leadgen Contribution

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.13.1 Model Search

# COMMAND ----------

#######################
## Collecting Leadgen ##
#######################

s_keyword = 'LeadGen_Imps_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 
       'PromoEvnt_22BlackFriday', 
       'PromoEvnt_23RollingStone',
       'dummy_20220711', 

      #  'LeadGen_Imps_AdStock6L1Wk90Ad_Power30',
       
       'AltMedia_Imps_AdStock6L2Wk90Ad_Power50',
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
       'Search_Clicks_AdStock6L1Wk80Ad_Power70',
       'Social_Imps_AdStock6L1Wk60Ad_Power90'

       ]


########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.13.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_26_MMM_StatsModel_Leadgen/6_27_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_26_MMM_StatsModel_Leadgen/6_27_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

'''
Comments:
Selecting Model Number 26, not much reduction in leadgen coefficient but getting better intercept. 


Model:

{

'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
'PromoEvnt_23RollingStone', 'dummy_20220711',
'AltMedia_Imps_AdStock6L2Wk90Ad_Power50',
'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk80Ad_Power70',
'Social_Imps_AdStock6L1Wk60Ad_Power90',
'LeadGen_Imps_AdStock6L1Wk60Ad_Power80'

}

'''

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.14 : Try splitting Leadgen

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.14.1 Model Search

# COMMAND ----------

## Splitting Var ##
model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2022'] = model_df.apply(lambda x: x['LeadGen_Imps_AdStock6L1Wk60Ad_Power80'] if pd.to_datetime(x['Date']).year==2022 else 0, axis=1)
model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2023'] = model_df.apply(lambda x: x['LeadGen_Imps_AdStock6L1Wk60Ad_Power80'] if pd.to_datetime(x['Date']).year==2023 else 0, axis=1)

## Normalizing Variable ##
model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2022']  = model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2022'] .transform(min_max_normalize)
model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2023']  = model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2023'] .transform(min_max_normalize)


# COMMAND ----------

input_vars = [

'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
'PromoEvnt_23RollingStone', 'dummy_20220711',
'AltMedia_Imps_AdStock6L2Wk90Ad_Power50',
'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk80Ad_Power70',
'Social_Imps_AdStock6L1Wk60Ad_Power90',


# 'LeadGen_Imps_AdStock6L1Wk60Ad_Power80'
'LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2022',
'LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2023'


       ]




input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

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
# MAGIC # 2.15 : Try to regualrize model with leadgen not splitted

# COMMAND ----------

input_vars = [

'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
'PromoEvnt_23RollingStone', 'dummy_20220711',
'AltMedia_Imps_AdStock6L2Wk90Ad_Power50',
'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk80Ad_Power70',
'Social_Imps_AdStock6L1Wk60Ad_Power90',


'LeadGen_Imps_AdStock6L1Wk60Ad_Power80'
# 'LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2022',
# 'LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2023'


       ]




input_vars_str = " + ".join(input_vars)


## Model Fit ##
Regularization = True
model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit_regularized(penalty= 'L1', alpha=np.asarray([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 10])) ## Min-Max Normalization

# COMMAND ----------

#Model Summary
if Regularization:
  model_summary = pd.concat([model.params.rename('Coeff.'), model.pvalues.rename('P Values'), model.bse.rename('Std. Err.'),model.conf_int(alpha=0.05, cols=None).rename(columns={0:'[.025', 1:'.975]'})], axis=1).round(3).reset_index()
  model_summary.display()
else:
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
# MAGIC # 2.16 : Alt Media Grid Search

# COMMAND ----------

#######################
## Collecting Leadgen ##
#######################

s_keyword = 'AltMedia_Imps_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

# COMMAND ----------

input_vars = [

'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
'PromoEvnt_23RollingStone',
#  'dummy_20220711',
# 'AltMedia_Imps_AdStock6L2Wk90Ad_Power50',
'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk80Ad_Power70',
'Social_Imps_AdStock6L1Wk60Ad_Power90',


'LeadGen_Imps_AdStock6L1Wk60Ad_Power80'


       ]



########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.16.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_27_MMM_StatsModel_AltMedia/6_27_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_27_MMM_StatsModel_AltMedia/6_27_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

'''

Comments:  


Best Model: 391, 146

input_vars = [ 
       'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
       'PromoEvnt_23RollingStone',
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
       'Search_Clicks_AdStock6L1Wk80Ad_Power70',
       'Social_Imps_AdStock6L1Wk60Ad_Power90',
       'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'
       ]




'''



# input_vars = [ 
#        'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
#        'PromoEvnt_23RollingStone',
#        'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
#        'TV_Imps_AdStock6L2Wk70Ad_Power70',
#        'Search_Clicks_AdStock6L1Wk80Ad_Power70',
#        'Social_Imps_AdStock6L1Wk60Ad_Power90',
#        'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
#        'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'
#        ]

# input_vars_str = " + ".join(input_vars)
# model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization
# model.summary().tables[1].reset_index()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.17 : Finetuning 2.16 with least correlated leadgen

# COMMAND ----------

#######################
## Collecting Leadgen ##
#######################

s_keyword = 'LeadGen_Imps_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

# COMMAND ----------

input_vars = [ 
       'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
       'PromoEvnt_23RollingStone',
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
       'Search_Clicks_AdStock6L1Wk80Ad_Power70',
       'Social_Imps_AdStock6L1Wk60Ad_Power90',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',

       #'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.17.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_28_MMM_StatsModel_Leadgen/6_28_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_28_MMM_StatsModel_Leadgen/6_28_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------



'''

Comments: Chossing Model Number 153 as best

Best Model:


input_var = [

'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
'PromoEvnt_23RollingStone',
'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk80Ad_Power70',
'Social_Imps_AdStock6L1Wk60Ad_Power90',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
'LeadGen_Imps_AdStock6L4Wk30Ad_Power90',

]




'''

# input_vars = [ 

#   'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
# 'PromoEvnt_23RollingStone',
# 'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
# 'TV_Imps_AdStock6L2Wk70Ad_Power70',
# 'Search_Clicks_AdStock6L1Wk80Ad_Power70',
# 'Social_Imps_AdStock6L1Wk60Ad_Power90',
# 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
# 'LeadGen_Imps_AdStock6L4Wk30Ad_Power90',
#        
#        ]

# input_vars_str = " + ".join(input_vars)
# model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization
# model.summary().tables[1].reset_index()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.18 : Searching on Social for positive intercept and les VIF

# COMMAND ----------

#######################
## Collecting Social ##
#######################

s_keyword = 'Social_Imps_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

# COMMAND ----------

input_vars = [ 
       'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
'PromoEvnt_23RollingStone',
'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk80Ad_Power70',
# 'Social_Imps_AdStock6L1Wk60Ad_Power90',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
'LeadGen_Imps_AdStock6L4Wk30Ad_Power90',
       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.18.2 Saving and Reading Results

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_28_MMM_StatsModel_Social/6_28_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_28_MMM_StatsModel_Social/6_28_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------



'''

Comments: Chossing Model Number 48 as best. Still Social is negative, but chose model with Less VIFs

Best Model:


input_var = [

'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
'PromoEvnt_23RollingStone',
'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk80Ad_Power70',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
'LeadGen_Imps_AdStock6L4Wk30Ad_Power90',
'Social_Imps_AdStock6L1Wk90Ad_Power90',

]




'''

# input_vars = [ 
# 'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
# 'PromoEvnt_23RollingStone',
# 'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
# 'TV_Imps_AdStock6L2Wk70Ad_Power70',
# 'Search_Clicks_AdStock6L1Wk80Ad_Power70',
# 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
# 'LeadGen_Imps_AdStock6L4Wk30Ad_Power90',
# 'Social_Imps_AdStock6L1Wk90Ad_Power90',
#        ]

# input_vars_str = " + ".join(input_vars)
# model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization
# model.summary().tables[1].reset_index()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.19 : Searching on DirectMail again

# COMMAND ----------

#######################
## Collecting DirectMail ##
#######################

s_keyword = 'DirectMail_Imps_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

input_vars = [ 
              
              'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
'PromoEvnt_23RollingStone',
# 'DirectMail_Imps_AdStock6L3Wk90Ad_Power70', 
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk80Ad_Power70',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
'LeadGen_Imps_AdStock6L4Wk30Ad_Power90',
'Social_Imps_AdStock6L1Wk90Ad_Power90',
      
       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data


  # if idx > 10:
  #   break

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.19.2 Saving and Reading Results

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC mkdir /dbfs/blend360/sandbox/mmm/model/6_28_MMM_StatsModel_DirectMail

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_28_MMM_StatsModel_DirectMail/6_28_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_28_MMM_StatsModel_DirectMail/6_28_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

# MAGIC %md
# MAGIC # 2.20 : Adding More Promo Events

# COMMAND ----------

input_vars = [ 
       'PromoEvnt_22MembDrive', 
       'PromoEvnt_22BlackFriday',
       
       # 'PromoEvnt_23RollingStone',

       'PromoEvnt_23MembDrive',
       'PromoEvnt_23BlackFriday',

       'Seasonality_Sin8',
       'Seasonality_Cos8',



       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
       'Search_Clicks_AdStock6L1Wk80Ad_Power70',
       'Social_Imps_AdStock6L1Wk60Ad_Power90',
       'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'
       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

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
# MAGIC # 2.21 : Searching over Social

# COMMAND ----------

###########################
## Collecting Social ##
###########################

s_keyword = 'Social_Imps_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

input_vars = [ 
              
              'PromoEvnt_22MembDrive',
       'PromoEvnt_22BlackFriday',

       'July0422_lag',
       'Seasonality_Year23',



       'PromoEvnt_23MembDrive', 
       'PromoEvnt_23BlackFriday',




       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
       'Search_Clicks_AdStock6L1Wk80Ad_Power70',
      #  'Social_Imps_AdStock6L1Wk60Ad_Power90',
       'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'  
      
       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data


  # if idx > 10:
  #   break

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_30_MMM_StatsModel_Social/6_30_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_30_MMM_StatsModel_Social/6_30_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

'''
Comments:


Best Model:

input_var = [

  'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
       'July0422_lag', 'Seasonality_Year23', 'PromoEvnt_23MembDrive',
       'PromoEvnt_23BlackFriday',
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
       'Search_Clicks_AdStock6L1Wk80Ad_Power70',
       'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
       'Social_Imps_AdStock6L1Wk90Ad_Power90', 
]



'''

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.22 : Searching over Search for lesser VIF

# COMMAND ----------

###########################
#### Collecting Search ####
###########################

s_keyword = 'Search_Clicks_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

input_vars = [ 
              
              'PromoEvnt_22MembDrive',
       'PromoEvnt_22BlackFriday',

       'July0422_lag',
       'Seasonality_Year23',



       'PromoEvnt_23MembDrive', 
       'PromoEvnt_23BlackFriday',




       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
      #  'Search_Clicks_AdStock6L1Wk80Ad_Power70',
        'Social_Imps_AdStock6L1Wk90Ad_Power90',
       'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'  
      
       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data


  # if idx > 10:
  #   break

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/6_30_MMM_StatsModel_Search/6_30_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/6_30_MMM_StatsModel_Search/6_30_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

'''
Comments:


Best Model:

input_var = [

  'PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
       'July0422_lag', 'Seasonality_Year23', 'PromoEvnt_23MembDrive',
       'PromoEvnt_23BlackFriday',
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
       'Search_Clicks_AdStock6L1Wk90Ad_Power90',
       'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
       'Social_Imps_AdStock6L1Wk90Ad_Power90', 
]



'''

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.23 : Checking for robustness of cofficients

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
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
       'Search_Clicks_AdStock6L1Wk90Ad_Power90',
       'Social_Imps_AdStock6L1Wk90Ad_Power90',
       'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'  
       ]



########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}


s_list = [1, 0.95, 0.90, 0.80, 0.75]
for idx,factor in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars)
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df.sample(n=int(factor*21840)), groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars].values, i) for i in range(model_df[input_vars].shape[1])]
  contri_dict[idx]['VIF'] =vif_data

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/7_1_MMM_StatsModel_RobustnessCheck/7_1_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/7_1_MMM_StatsModel_RobustnessCheck/7_1_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

# MAGIC %md
# MAGIC # 2.22 : Searching over Social for positive coeff

# COMMAND ----------

###########################
#### Collecting Social ####
###########################

s_keyword = 'Social_Imps_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

input_vars = [ 
              
'PromoEvnt_22MembDrive_Eng',
'PromoEvnt_22BlackFriday_Eng',

'July0422_lag',
'Seasonality_Feb', 
'Seasonality_Mar', 
'Seasonality_Apr',
'Seasonality_Jun',
'Seasonality_Dec',

'PromoEvnt_23MembDrive_Eng', 
'PromoEvnt_23BlackFriday_Eng',

'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk90Ad_Power90',
# 'Social_Imps_AdStock6L1Wk90Ad_Power90',
'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'  

       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data


  # if idx > 10:
  #   break

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/7_1_MMM_StatsModel_Social/7_1_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/7_1_MMM_StatsModel_Social/7_1_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

'''
Comment: Did not result in good search. Not updting model

Best model:
input_vars = [
'PromoEvnt_22MembDrive_Eng',
'PromoEvnt_22BlackFriday_Eng',

'July0422_lag',
'Seasonality_Feb', 
'Seasonality_Mar', 
'Seasonality_Apr',
'Seasonality_Jun',
'Seasonality_Dec',

'PromoEvnt_23MembDrive_Eng', 
'PromoEvnt_23BlackFriday_Eng',

'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk90Ad_Power90',
'Social_Imps_AdStock6L1Wk90Ad_Power90',
'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'  

       ]




'''

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.22 : Searching over ALt Media for positive coeff -  Removing June Flag as well

# COMMAND ----------

##############################
#### Collecting Alt Media ####
##############################

s_keyword = 'AltMedia_Imps_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

len(s_list), s_list[:3]

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

'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk90Ad_Power90',
'Social_Imps_AdStock6L1Wk90Ad_Power90',
'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
# 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'  

       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data


  # if idx > 10:
  #   break

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/7_2_MMM_StatsModel_AltMedia/7_2_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/7_2_MMM_StatsModel_AltMedia/7_2_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

'''
Comment: Not updating Model, same Alt Media Variable came out to be good one

Best Model:

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

'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk90Ad_Power90',
'Social_Imps_AdStock6L1Wk90Ad_Power90',
'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'  

       ]




'''

# COMMAND ----------

while True:
  val=1

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.23 : Splitting leadgen and searching over decay rate and saturation rate 

# COMMAND ----------

##############################
#### Collecting Leadgen ####
##############################

s_keyword = 'LeadGen_Imps_AdStock6L1'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

len(s_list), s_list[:3]

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

# 'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',


'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'

       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



### Splitting Leadgen ###
model_df['Date'] = pd.to_datetime(model_df['Date']) 
mask_2022 = model_df['Date'].dt.year==2022




for idx,var in tqdm.tqdm(enumerate(s_list)):


  ### Splitting Leadgen ###
  
  
  model_df[f'{var}_2022'] = model_df[var]*mask_2022
  model_df[f'{var}_2023'] = model_df[var]*(1-mask_2022)

  var_1 = f'{var}_2022'
  var_2 = f'{var}_2023'
  combination = [var_1, var_2]




  ### Model ###
  input_vars_str = " + ".join(input_vars+combination)
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+combination].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+combination].values, i) for i in range(model_df[input_vars+combination].shape[1])]
  contri_dict[idx]['VIF'] =vif_data


# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/7_8_MMM_StatsModel_Leadgen/7_8_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/7_8_MMM_StatsModel_Leadgen/7_8_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

# MAGIC %md
# MAGIC # 2.24 : Not splittig Leadgen and Grid Search over Search

# COMMAND ----------

##############################
#### Collecting Search ####
##############################

s_keyword = 'MembershipDigitalSocial_Imps'
s_list = []

for cols in model_df.columns:
  if (cols.startswith(s_keyword)) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

#'ASISocial_Imps', 'IcmDigitalSocial_Imps', 'IcmSocialSocial_Imps', 'MediaComSocial_Imps', 'MembershipDigitalSocial_Imps',


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
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',


       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data


  # if idx > 10:
  #   break

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/7_2_MMM_StatsModel_Search/7_2_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/7_2_MMM_StatsModel_Search/7_2_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Analysis 

# COMMAND ----------



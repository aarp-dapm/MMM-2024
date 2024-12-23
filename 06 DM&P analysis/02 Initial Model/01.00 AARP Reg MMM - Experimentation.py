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

## Min max Scaling Target Variable ##
model_df['Reg_norm'] = model_df.groupby('DmaCode')['Reg'].transform(min_max_normalize)



## Adding Indicator for Registration Wall ##
model_df['Reg_Wall'] = model_df['Date'].apply(lambda x: 1 if x >= '2022-07-25' else 0)


## Adding Halloween ##
model_df['Holiday_22Halloween'] = model_df.apply(lambda x: 1 if x.Date == '2022-10-31' else 0, axis=1)



## Adding Joins for Qtr 4 only ##
model_df['Qtr'] = pd.to_datetime(model_df['Date']).dt.quarter
model_df['year'] = pd.to_datetime(model_df['Date']).dt.year


# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Experiments

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.01 : Email

# COMMAND ----------

###########################
#### Collecting Social ####
###########################

s_keyword = 'Email_Spend_AdStock6'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) :
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

input_vars = [ 
              
# 'Reg_Wall',



'Reg_Wall',
# 'Trend',


'PromoEvnt_22WsjTodayShow',
'PromoEvnt_23RollingStone',

'Seasonality_Cos3',
'Seasonality_Cos7_x',

'Holiday_22Halloween',



'TV_Imps_AdStock6L2Wk70Ad_Power70',

'Search_Membership_Clicks_AdStock6L1Wk90Ad_Power90',
'Search_NonMembership_Clicks_AdStock6L1Wk90Ad_Power90',

'Social_Membership_Imps_AdStock6L1Wk90Ad_Power90',
'Social_NonMembership_Imps_AdStock6L1Wk90Ad_Power90',

# 'Display_Imps_AdStock6L1Wk80Ad_Power40',
# 'Display_Membership_Imps_AdStock6L1Wk80Ad_Power40',
# 'Display_NonMembership_Imps_AdStock6L1Wk80Ad_Power40',


'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'Email_Spend_AdStock6L1Wk10Ad_Power20',


'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'



         
       ]
########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Reg_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

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
# MAGIC ## 2.02 Direct Mail

# COMMAND ----------

###########################
#### Collecting Social ####
###########################

s_keyword = 'Email_Spend_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) :
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

input_vars = [ 
             
'Reg_Wall',
# 'Trend',


'PromoEvnt_22WsjTodayShow',
'PromoEvnt_23RollingStone',

# 'Seasonality_Cos3',
# 'Seasonality_Cos7_x',

'Holiday_22Halloween',



'TV_Imps_AdStock6L2Wk70Ad_Power70',

'Search_Membership_Clicks_AdStock6L1Wk90Ad_Power90',
'Search_NonMembership_Clicks_AdStock6L1Wk90Ad_Power90',

'Social_Membership_Imps_AdStock6L1Wk90Ad_Power90',
'Social_NonMembership_Imps_AdStock6L1Wk90Ad_Power90',


'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
# 'Email_Spend_AdStock6L1Wk10Ad_Power20',


'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'



         
       ]
########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Reg_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

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
# MAGIC ## 2.03 TV

# COMMAND ----------

###########################
#### Collecting Social ####
###########################

s_keyword = 'TV_NonMembership_Imps_AdStock6L'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword):
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

input_vars = [ 
             
'Trend',


'PromoEvnt_22WsjTodayShow',
'PromoEvnt_23RollingStone',


'Holiday_22Halloween',

'Search_Membership_Clicks_AdStock6L1Wk90Ad_Power90',
'Search_NonMembership_Clicks_AdStock6L1Wk90Ad_Power90',
'Social_Imps_AdStock6L1Wk90Ad_Power90',


'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'Email_Spend_AdStock6L1Wk50Ad_Power90',




         
       ]
########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Reg_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

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
# MAGIC ## 2.03 Alt Media

# COMMAND ----------

###########################
#### Collecting Social ####
###########################

s_keyword = 'AltMedia_Imps_AdStock6L1'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword):
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

input_vars = [ 
             

'Trend',


'PromoEvnt_22WsjTodayShow',
'PromoEvnt_23RollingStone',


'Holiday_22Halloween',
'TV_Imps_AdStock6L1Wk30Ad_Power90',

'Search_Membership_Clicks_AdStock6L1Wk90Ad_Power90',
'Search_NonMembership_Clicks_AdStock6L1Wk90Ad_Power90',

'Social_Imps_AdStock6L1Wk90Ad_Power90',


'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'Email_Spend_AdStock6L1Wk50Ad_Power90',




         
       ]
########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Reg_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

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
# MAGIC ## 2.04 Display

# COMMAND ----------

###########################
#### Collecting Social ####
###########################

s_keyword = 'Display_Imps_AdStock6L1'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword):
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

## Filtering Data ##
model_df = model_df[model_df['Date']>='2022-07-25']

# COMMAND ----------

input_vars = [ 
             

'PromoEvnt_22WsjTodayShow',
'PromoEvnt_23RollingStone',


'Holiday_22Halloween',



'Search_Membership_Clicks_AdStock6L1Wk90Ad_Power90',
'Search_NonMembership_Clicks_AdStock6L1Wk90Ad_Power90',



'Social_Imps_AdStock6L1Wk90Ad_Power90',
'Video_NonMembership_Imps_AdStock6L1Wk90Ad_Power90',
'TV_NonMembership_Imps_AdStock6L2Wk40Ad_Power90',




         
       ]
########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Reg_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

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
# MAGIC ## 2.51 Saving Data

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/model/Reg/11222024_Display.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/Reg/11222024_Display.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Analysis 

# COMMAND ----------

while True:
  val=1

# COMMAND ----------



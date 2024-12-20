# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Data and Transformation

# COMMAND ----------

#################
##### Data #####
#################


## Reading Data ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_Search_SubModal_df.csv').drop('Unnamed: 0',axis=1)
model_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Grid Search Experiment

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.01 : Searching over Video for positive coeff.

# COMMAND ----------

##############################
#### Collecting Video ####
##############################

s_keyword = 'Video_Imps_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

input_vars = [ 
              

'Trend',
'Social_Imps_norm',
'TV_Imps_AdStock6L3Wk90Ad_Power30'


# 'Video_Imps_AdStock6L2Wk50Ad_Power80',

       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Search_contri_norm   ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

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
file_path = '/dbfs/blend360/sandbox/mmm/SearchSubmodal/7_9_MMM_StatsModel_Video/7_9_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/SearchSubmodal/7_9_MMM_StatsModel_Video/7_9_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

'''
Comment: CHossing Model Number 148

Best Model:

input_vars = [ 
              

'Trend', 
'Social_Imps_norm',
'TV_Imps_AdStock6L3Wk90Ad_Power30',
'Video_Imps_AdStock6L4Wk30Ad_Power40'




       ]


'''

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.02 : Searching for positive coeff of TV

# COMMAND ----------

##############################
#### Collecting TV ####
##############################

s_keyword = 'TV_Imps_AdStock'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) & ~(cols.endswith('UnitMean')) :
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

input_vars = [ 
              

'Trend',
'Social_Imps_norm',
# 'Video_Imps_AdStock6L3Wk70Ad_Power40'

       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.mixedlm("Search_contri_norm  ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

  ## Saving Contribution ##
  contri_dict[idx] = {}
  contri_dict[idx]['Summary'] =fit_model.summary().tables[1].reset_index()


  ## Saving VIF ##
  vif_data = pd.DataFrame()
  vif_data["feature"] = model_df[input_vars+[combination]].columns
  vif_data["VIF"] = [variance_inflation_factor(model_df[input_vars+[combination]].values, i) for i in range(model_df[input_vars+[combination]].shape[1])]
  contri_dict[idx]['VIF'] =vif_data


#   if idx > 10:
#     break

# COMMAND ----------

# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/SearchSubmodal/7_9_MMM_StatsModel_TV/7_9_exp_data.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/SearchSubmodal/7_9_MMM_StatsModel_TV/7_9_exp_data.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

'''
Comment: Chose model number 140, Variable is 'TV_Imps_AdStock6L3Wk90Ad_Power30'

Best Model:

[


'Trend',
'Social_Imps_norm',
'TV_Imps_AdStock6L3Wk90Ad_Power30'



]





'''

# COMMAND ----------

while True:
  val=1

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Analysis 

# COMMAND ----------



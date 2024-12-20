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
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed__updated_reg_df_BuyingGroup.csv').drop('Unnamed: 0',axis=1)
model_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Experiments

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.01 : Search

# COMMAND ----------

###########################
#### Collecting Social ####
###########################

s_keyword = 'Search_ASI_Clicks_AdStock6L1'
s_list = []

for cols in model_df.columns:
  if cols.startswith(s_keyword) :
    s_list.append(cols)

len(s_list), s_list[:3]

# COMMAND ----------

input_vars = [ 
              


'Seasonality_Q1',
'Seasonality_Dec',
'Seasonality_Nov',
'Seasonality_Jul23',


'PromoEvnt_22BlackFriday',
'PromoEvnt_23BlackFriday',

'Seasonality_Year23',


'Seasonality_10162023',
'Seasonality_11062023',

## ASI ##
'Social_ASI_Imps_AdStock6L1Wk90Ad_Power90',
# 'Search_ASI_Clicks_AdStock6L1Wk70Ad_Power90',


## Membership ##
'Display_MembershipDigital_Imps_AdStock6L1Wk90Ad_Power90',

       ]

########################
## Running all models ##
########################

model_dict = {}
contri_dict = {}



for idx,combination in tqdm.tqdm(enumerate(s_list)):

  input_vars_str = " + ".join(input_vars+[combination])
  fit_model = smf.ols( f'BenRefAll ~ {input_vars_str}', data = model_df ).fit()

  ## Saving Contribution ##
  contri_dict[idx] = {}

 
  ########################################
  ## Extract the summary as a DataFrame ##
  ########################################
  summary_df = pd.DataFrame({
       "index": fit_model.params.index,  # Adding coefficient names
       "Coeff.": fit_model.params.values,
       "Standard Errors": fit_model.bse.values,
       "t-values": fit_model.tvalues.values,
       "P>|z|": fit_model.pvalues.values
       })

  # Add confidence intervals
  conf_int = fit_model.conf_int()
  conf_int.columns = ["CI Lower", "CI Upper"]
  summary_df = pd.concat([summary_df, conf_int.reset_index(drop=True)], axis=1)
  summary_df = summary_df.merge(pd.DataFrame(( (fit_model.params)*fit_model.model.exog.sum(axis=0) ).rename('Contri')).reset_index(), on='index')
  summary_df['Target_Var'] = fit_model.model.endog.sum()
  contri_dict[idx]['Summary'] = summary_df


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
file_path = '/dbfs/blend360/sandbox/mmm/model/Reg/BenRef_12122024_Search.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(contri_dict, f)


# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/Reg/BenRef_12122024_Search.json'
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict)




# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Analysis 

# COMMAND ----------

contri_dict[47]['Summary']

# COMMAND ----------



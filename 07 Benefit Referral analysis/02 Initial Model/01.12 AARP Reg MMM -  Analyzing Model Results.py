# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Data

# COMMAND ----------

## Reading Model ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed__updated_reg_df_BuyingGroup.csv').drop('Unnamed: 0',axis=1)
model_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Results Summary

# COMMAND ----------

# Read the dictionary from the JSON file
file_path = '/dbfs/blend360/sandbox/mmm/model/Reg/BenRef_12122024_Search.json'

with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)


# COMMAND ----------

## Appending only Summary ##
df_summary_list = []
model_object_list = []
for key, value in tqdm.tqdm(loaded_dict.items()):
  value['Summary'].insert(0, 'ModelNumber', key)
  df_summary_list.append(value['Summary'])


## Concat Data ##
analysis_df = pd.concat(df_summary_list, axis=0)


## creating new column name ##
analysis_df['new_name'] = np.where( analysis_df['index'].str.contains('AdStock'), analysis_df['index'].apply(lambda x: "_".join(x.split("_")[:2])),analysis_df['index'])
analysis_df.head()

## Pivot Table ##
pivot_coeff_df = analysis_df.pivot(index = 'new_name', columns = 'ModelNumber', values = 'Coeff.').reset_index()
pivot_coeff_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Results P Value

# COMMAND ----------

## Appending only PValue ##
df_pvalue_list = []
model_object_list = []
for key, value in tqdm.tqdm(loaded_dict.items()):
  # value['Summary'].insert(0, 'ModelNumber', key)
  df_pvalue_list.append(value['Summary'])


## Concat Data ##
analysis_df = pd.concat(df_pvalue_list, axis=0)


## creating new column name ##
analysis_df['new_name'] = np.where( analysis_df['index'].str.contains('AdStock'), analysis_df['index'].apply(lambda x: "_".join(x.split("_")[:2])),analysis_df['index'])
analysis_df.head()

## Pivot Table ##
pivot_pvalue_df = analysis_df.pivot(index = 'new_name', columns = 'ModelNumber', values = 'P>|z|').reset_index()
pivot_pvalue_df['new_name'] = 'PValue_' + pivot_pvalue_df['new_name']

pivot_pvalue_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Results VIF

# COMMAND ----------

## Appending only VIF ##
df_vif_list = []
model_object_list = []
for key, value in tqdm.tqdm(loaded_dict.items()):
  value['VIF'].insert(0, 'ModelNumber', key)
  df_vif_list.append(value['VIF'])


## Concat Data ##
analysis_vif_df = pd.concat(df_vif_list, axis=0)


## creating new column name ##
analysis_vif_df ['new_name'] = np.where( analysis_vif_df ['feature'].str.contains('AdStock'), analysis_vif_df ['feature'].apply(lambda x: "_".join(x.split("_")[:2])),analysis_vif_df ['feature'])
analysis_vif_df .head()

## Pivot Table ##
pivot_vif_df = analysis_vif_df .pivot(index = 'new_name', columns = 'ModelNumber', values = 'VIF').reset_index()
pivot_vif_df['new_name'] = 'VIF_' + pivot_vif_df['new_name']

pivot_vif_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Contribution Analysis

# COMMAND ----------

## Appending only PValue ##
df_contri_list = []
model_object_list = []
for key, value in tqdm.tqdm(loaded_dict.items()):
  # value['Summary'].insert(0, 'ModelNumber', key)
  df_contri_list.append(value['Summary'])


## Concat Data ##
analysis_df = pd.concat(df_contri_list, axis=0)


## creating new column name ##
analysis_df['new_name'] = np.where( analysis_df['index'].str.contains('AdStock'), analysis_df['index'].apply(lambda x: "_".join(x.split("_")[:2])),analysis_df['index'])
analysis_df.head()

## Pivot Table ##
pivot_contri_df = analysis_df.pivot(index = 'new_name', columns = 'ModelNumber', values = 'Contri').reset_index()
pivot_contri_df['new_name'] = 'Contri_' + pivot_contri_df['new_name']

pivot_contri_df.display()

# COMMAND ----------

transform_df = analysis_df[analysis_df['new_name']=='Search_ASI'][['ModelNumber', 'index']]
transform_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Single Report

# COMMAND ----------

report = pd.concat([pivot_coeff_df, pivot_pvalue_df, pivot_vif_df, pivot_contri_df]).set_index('new_name').T.reset_index()
report = report.merge(transform_df, on='ModelNumber')
report.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Analysis

# COMMAND ----------



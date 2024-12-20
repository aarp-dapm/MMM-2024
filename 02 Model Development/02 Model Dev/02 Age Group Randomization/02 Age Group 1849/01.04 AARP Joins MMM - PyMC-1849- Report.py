# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading in all Data

# COMMAND ----------

## Reading Main Model Report ##
main_model = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_main_model_1849.csv').drop('Unnamed: 0',axis=1).query("Channel != 'Random Dma Effect'")[['Channel', 'Contrib_2022', 'Contrib_2023']]
main_model['Contrib_2022_Prop'] = main_model['Contrib_2022']/main_model['Contrib_2022'].sum()
main_model['Contrib_2023_Prop'] = main_model['Contrib_2023']/main_model['Contrib_2023'].sum()


## Renaming Column Values ##
main_model['Channel'] = main_model['Channel'].replace({'Seasonal':'Base', 'Fixed Dma Effect':'Base', '22MembDrive':'Promotion', '23MembDrive':'Promotion', '22BlackFriday':'Promotion', '23BlackFriday':'Promotion'})

assert main_model['Contrib_2022_Prop'].sum() == 1
assert main_model['Contrib_2023_Prop'].sum() == 1

main_model.display()

# COMMAND ----------

## Reading Sub Model Leadgen Report ##
submodel_leadgen = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_SubModel_Leadgen_1849.csv').drop('Unnamed: 0',axis=1).query("Channel != 'Random Dma Effect'")[['Channel', 'Contrib_2022', 'Contrib_2023']]
submodel_leadgen['Contrib_2022_Prop'] = submodel_leadgen['Contrib_2022']/submodel_leadgen['Contrib_2022'].sum()
submodel_leadgen['Contrib_2023_Prop'] = submodel_leadgen['Contrib_2023']/submodel_leadgen['Contrib_2023'].sum()

assert submodel_leadgen['Contrib_2022_Prop'].sum() == 1
assert submodel_leadgen['Contrib_2023_Prop'].sum() == 1


submodel_leadgen['Contrib_2022_Prop'] = submodel_leadgen['Contrib_2022_Prop']*main_model[main_model['Channel']=='LeadGen']['Contrib_2022_Prop'].values[0]
submodel_leadgen['Contrib_2023_Prop'] = submodel_leadgen['Contrib_2023_Prop']*main_model[main_model['Channel']=='LeadGen']['Contrib_2023_Prop'].values[0]




## Renaming Column Values ##
submodel_leadgen['Channel'] = submodel_leadgen['Channel'].replace({'Seasonal':'LeadGen', 'Fixed Dma Effect':'LeadGen'})
submodel_leadgen.display()

# COMMAND ----------

## Reading Sub Model Search Report ##
submodel_search = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_SubModel_Search_1849.csv').drop('Unnamed: 0',axis=1).query("Channel != 'Random Dma Effect'")[['Channel', 'Contrib_2022', 'Contrib_2023']]
submodel_search['Contrib_2022_Prop'] = submodel_search['Contrib_2022']/submodel_search['Contrib_2022'].sum()
submodel_search['Contrib_2023_Prop'] = submodel_search['Contrib_2023']/submodel_search['Contrib_2023'].sum()

assert abs(submodel_search['Contrib_2022_Prop'].sum()-1) < 0.0001
assert abs(submodel_search['Contrib_2023_Prop'].sum()-1) < 0.0001




submodel_search['Contrib_2022_Prop'] = submodel_search['Contrib_2022_Prop']*main_model[main_model['Channel']=='Search']['Contrib_2022_Prop'].values[0]
submodel_search['Contrib_2023_Prop'] = submodel_search['Contrib_2023_Prop']*main_model[main_model['Channel']=='Search']['Contrib_2023_Prop'].values[0]


## Renaming Column Values ##
submodel_search['Channel'] = submodel_search['Channel'].replace({'Fixed Dma Effect':'Search', 'Seasonal':'Search'})
submodel_search.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Merging Datasets

# COMMAND ----------

final_report_df = pd.concat([main_model[~main_model['Channel'].isin(['Search', 'LeadGen'])], submodel_leadgen, submodel_search], axis=0)[['Channel', 'Contrib_2022_Prop', 'Contrib_2023_Prop']]
final_report_df['Channel'] = final_report_df['Channel'].replace({'lag':'Base'}) 
final_report_df = final_report_df.groupby('Channel').sum().reset_index()


assert abs(final_report_df['Contrib_2022_Prop'].sum()-1) < 0.0001
assert abs(final_report_df['Contrib_2023_Prop'].sum()-1) < 0.0001

final_report_df.display()

# COMMAND ----------

## Save Agg. Analysis Dataframe ##
final_report_df.to_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_contrib_report_1849.csv')

# COMMAND ----------

## Read Agg. Analysis Dataframe ##
final_report_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_contrib_report_1849.csv').drop('Unnamed: 0',axis=1)
final_report_df.display()

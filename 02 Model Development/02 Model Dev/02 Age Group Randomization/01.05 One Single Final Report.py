# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading in all Reports

# COMMAND ----------

## reading data ##
report_all_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_contrib_report_combined.csv').drop('Unnamed: 0',axis=1)[['Channel', 'Contrib_2023_Prop']]
report_all_df = report_all_df.rename(columns={'Contrib_2023_Prop': 'Contrib_2023_Prop_all'})

report_1849_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_contrib_report_1849.csv').drop('Unnamed: 0',axis=1)[['Channel', 'Contrib_2023_Prop']]
report_1849_df  = report_1849_df .rename(columns={'Contrib_2023_Prop': 'Contrib_2023_Prop_1849'})

report_5059_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_contrib_report_5059.csv').drop('Unnamed: 0',axis=1)[['Channel', 'Contrib_2023_Prop']]
report_5059_df  = report_5059_df .rename(columns={'Contrib_2023_Prop': 'Contrib_2023_Prop_5059'})

report_6069_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_contrib_report_6069.csv').drop('Unnamed: 0',axis=1)[['Channel', 'Contrib_2023_Prop']]
report_6069_df  = report_6069_df .rename(columns={'Contrib_2023_Prop': 'Contrib_2023_Prop_6069'})

report_7099_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_contrib_report_7099.csv').drop('Unnamed: 0',axis=1)[['Channel', 'Contrib_2023_Prop']]
report_7099_df  = report_7099_df .rename(columns={'Contrib_2023_Prop': 'Contrib_2023_Prop_7099'})

# report_6099_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/agg_analysis_df_contrib_report_6099.csv').drop('Unnamed: 0',axis=1)[['Channel', 'Contrib_2023_Prop']]
# report_6099_df  = report_6099_df .rename(columns={'Contrib_2023_Prop': 'Contrib_2023_Prop_6099'})
## Joining them all ##
# report = report_all_df.merge(report_1849_df, on='Channel', how='left').merge(report_5059_df, on='Channel', how='left').merge(report_6099_df, on='Channel', how='left')

## Joining them all ##
report = report_all_df.merge(report_1849_df, on='Channel', how='left').merge(report_5059_df, on='Channel', how='left').merge(report_6069_df, on='Channel', how='left').merge(report_7099_df, on='Channel', how='left')


order_map = {'Base':1, 'Promotion':2, 'TV':3, 'DirectMail':4, 'AltMedia':5, 'Social':6, 'Search':7, 'LeadGen':8, 'Video':9, 'Display':10, 'Email':11, 'Print':12}
report['rank'] = report['Channel'].map(order_map)


report.sort_values('rank').display()

# COMMAND ----------



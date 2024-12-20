# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading Compiled Contribution

# COMMAND ----------

## Read Dataframe ##
report_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_initialmodel_results.csv').drop('Unnamed: 0',axis=1)
report_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Creating Spend Data

# COMMAND ----------

### Channel Columns ###
spend_cols = ['Affiliate_Spend', 'AltMedia_Spend', 'Audio_Spend', 'DMP_Spend', 'DirectMail_Spend', 'Display_Spend', 'Email_Spend', 'LeadGen_Spend', 'Radio_Spend', 'OOH_Spend', 'Print_Spend', 'Search_Spend', 'Social_Spend', 'TV_Spend', 'Video_Spend']

spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_statsmodel.csv').groupby('Date')[spend_cols].sum().reset_index()
spend_df.head(5)

# COMMAND ----------

## Save Dataframe ##
spend_df.to_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_initialmodel_spend.csv')

## Read Dataframe ##
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_initialmodel_spend.csv').drop('Unnamed: 0',axis=1)
spend_df.display()

# COMMAND ----------



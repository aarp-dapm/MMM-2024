# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Reading Data

# COMMAND ----------

### Reading Data ###
ch_list = ['beta_dm', 'beta_tv', 'beta_AltMedia', 'beta_search', 'beta_social', 'beta_leadgen', 'beta_email', 'beta_disp']

report_df = pd.DataFrame()
for ch_name in ch_list:
  ch_df  = pd.read_csv(f'/dbfs/blend360/sandbox/mmm/model/dma_analysis_{ch_name}.csv').drop('Unnamed: 0',axis=1)
  
  if report_df.empty:
    report_df = ch_df.copy()
  else:
    report_df = report_df.merge(ch_df[['DmaCode', ch_name, ch_name+'_Contri']], on = ['DmaCode'], how='left')

report_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 DMA Report

# COMMAND ----------

##############
## Report 1 ##
##############

contri_cols = ['beta_dm_Contri', 'beta_search_Contri', 'beta_social_Contri', 'beta_disp_Contri']
coeff_cols = ['beta_AltMedia', 'beta_leadgen', 'beta_email']

DmaAnalysis_df = report_df[['DmaCode']+contri_cols+coeff_cols+['Population_Prop']]
DmaAnalysis_df.display()

# COMMAND ----------

##############
## Report 2 ##
##############

contri_cols = []
coeff_cols = ['beta_dm', 'beta_search', 'beta_social', 'beta_AltMedia', 'beta_leadgen', 'beta_email']

DmaAnalysis_df_2 = report_df[['DmaCode']+contri_cols+coeff_cols+['Population_Prop']]
DmaAnalysis_df_2.display()

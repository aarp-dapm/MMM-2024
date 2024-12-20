# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Final DataFrame

# COMMAND ----------

cols = ['Date', 'DmaCode', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'MailVolume', 'Response']

## Loading Dataframe ##
df_mail = load_saved_table("temp.ja_blend_mmm2_AcqMail_DirectWinBackMail")
df_mail = df_mail.withColumn('Channel', f.lit('DirectMail')).select(cols)

df_email = load_saved_table("temp.ja_blend_mmm2_AcqMail_WinbackEmail")
df_email = df_email.withColumn('Channel', f.lit('Email')).select(cols)


## Readinf CTV Data for Acq Mail Campaigns ##
table_name = "temp.ja_blend_mmm2_DRTV_CTV" 
df_fromDRTV = read_table(table_name)
df_fromDRTV = df_fromDRTV.filter(f.col('Campaign').isin(['DirectMailCampaign']))

## Rename Columns ##
rename_dict = {'Imps':'MailVolume','Clicks':'Response'}
df_fromDRTV = rename_cols_func(df_fromDRTV, rename_dict)

## Replace Column Values ##
df_fromDRTV = df_fromDRTV.withColumn('Channel', when(f.col('Channel')=='TV', f.lit('DirectMail')).otherwise(f.col('Channel')) )

# COMMAND ----------

## Stitch DataFrame ##
df_name_list = [df_mail, df_email, df_fromDRTV]
df = stitch_df(df_name_list)

## Adding Channel Column ##
df = df.withColumn("MediaName", f.lit('AcqMail'))

## Rearranging Cols ##
cols = ['Date', 'DmaCode', 'MediaName', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'MailVolume', 'Response']
df = df.select(cols)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File 

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_AcqMail" 
save_df_func(df, table_name)

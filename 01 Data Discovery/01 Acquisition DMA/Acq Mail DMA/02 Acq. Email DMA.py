# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

#######################################################################################################################################################
#################################################### Before H2 2023 ###################################################################################
#######################################################################################################################################################


df = spark.sql("select * from default.acq_mail___winback_model_attrubution_data_output_2020_2022_1_csv")

### Filtering for Columns ###
cols = ['Mail_Date__DMAM_', 'Job_Number', 'Package_Code', 'Package_Demo', 'List_Number', 'List_Name', 'Major_Select', 'Minor_Select', 'Touch',   'Premium_Cost', 'List_Cost', 'Package_Cost', 'Total_Cost', 'Mail_Quantity', 'Actual_Responses']

df = df.select(cols).dropDuplicates()
df = df.withColumn('Date_Mail', f.to_date(f.col('Mail_Date__DMAM_'), "M/d/y")).drop(f.col('Mail_Date__DMAM_'))


############################################################################################################################################
############################################################################################################################################
############################################################################################################################################

# #### Append Df for H1 2023 ####
# append_df = spark.sql("select * from default.acq_mail_multi_media_model_csv")
# append_df = append_df.select(cols).dropDuplicates()
# append_df = append_df.withColumn('Date_Mail', f.to_date(f.col('Mail_Date__DMAM_'), "M/d/y")).drop(f.col('Mail_Date__DMAM_'))
# append_df = append_df.filter(f.col('Date_Mail')<=f.to_date(f.lit('2023-06-30')))


# #### Append Df for H2 2023 ####

# '''
# This Dataframe is missing Winback Email Data 
# '''


# append_df2 = spark.sql("select * from default.model_attribution_data_pull_acquisition_mail_jul23___dec23_csv")
# append_df2 = append_df2.select(cols).dropDuplicates()
# append_df2 = append_df2.withColumn('Date_Mail', f.to_date(f.col('Mail_Date__DMAM_'), "M/d/y")).drop(f.col('Mail_Date__DMAM_'))
# append_df2 = append_df2.filter(f.col('Date_Mail')<=f.to_date(f.lit('2023-12-31')))

# #### Adding Both Data Source ####
# df = df.union(append_df)
# df = df.union(append_df2)

############################################################################################################################################
############################################################################################################################################
############################################################################################################################################

## creating winback flag ##
df = df.withColumn('job_type', when(f.col('Date_Mail')<f.to_date(f.lit('2020-07-01')),\
                                    when(f.substring(f.col('Job_Number'), 3, 2)=='AE','WinbackEmail').\
                                    when(f.substring(f.col('Job_Number'), -1, 1)=='W','Wb').otherwise('DM')).\
                               otherwise(when(f.substring(f.col('Package_Code'), 4, 1)=='W','Wb').\
                                        when(f.substring(f.col('Package_Code'), 4, 1)=='E','WinbackEmail').otherwise('DM')))

## Filterimg for Email and selecting required cols ##
cols = ['Date_Mail', 'Mail_Quantity', 'Actual_Responses' ]
df = df.filter(f.col('job_type')=='WinbackEmail').select(cols)

#######################################################################################################################################################
#################################################### H2 2023 ##########################################################################################
#######################################################################################################################################################

#### Appending Winback Email Df for H2 2023 ####
'''
Seperate Df for H2 2023 Winback EMail 
'''
folder_path = 'dbfs:/blend360/sandbox/mmm/acq_mail'
file_path = 'winback_email_data_pull_2023_H2_for_mmm.xlsx'


## Loading Data and Selecting relevant cols ##
cols = ['Mail Date (DMAM)', 'Mail Quantity', 'Actual Responses']
append_df_email = read_excel(folder_path+'/'+file_path).select(cols)

## creating new Date column ##
append_df_email = append_df_email.withColumnRenamed('Mail Date (DMAM)','Mail_Date_DMAM')
append_df_email = append_df_email.withColumn('Date_Mail', f.to_date(f.col('Mail_Date_DMAM'), "M/d/y")).drop(f.col('Mail_Date_DMAM')).select('Date_Mail','Mail Quantity', 'Actual Responses').filter(f.col('Date_Mail').isNotNull())

#######################################################################################################################################################
#################################################### Union Data #######################################################################################
#######################################################################################################################################################

df = df.union(append_df_email)

############ Agg Values #########
df = df.withColumn('Date', f.date_format(f.date_sub(f.col('Date_Mail'),f.dayofweek(f.col('Date_Mail'))-2),'yyyy-MM-dd'))
df = df.groupBy('Date').agg(f.sum(f.col('Mail_Quantity')).alias('MailVolume'),f.sum(f.col('Actual_Responses')).alias('Response'))
df.display()

# COMMAND ----------

### Making Data Structre aligned for downstream code ###

### Adding More Columns ###
df = df.withColumn('SubChannel', f.lit('WinbackEmail'))
df = df.withColumn('Campaign', f.col('SubChannel'))
df = df.withColumn('DmaName', f.lit('National'))

### Estimating Spend ###
df = df.withColumn('Spend', f.col('Response')*6.43 + f.col('MailVolume')*0.005)

### Re-Ordering Columns ###
cols  =['Date', 'DmaName', 'SubChannel', 'Campaign', 'Spend', 'MailVolume', 'Response']
final_model_df = df.select(cols)
final_model_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

'''
Data at National Level Already
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

## Creating Key-Value Dataframe ##
data_tuples = [(key,value) for key, value in norm_ch_dma_code_prop.items()]
norm_pop_prop_df = spark.createDataFrame(data_tuples, schema = ['DmaCode', 'Prop'])

## Joining above df with final_model_df ##
final_model_df = final_model_df.crossJoin(norm_pop_prop_df)

## Updating column Values ##
cols = ['Spend', 'MailVolume', 'Response']
for col in cols:
  final_model_df = final_model_df.withColumn(col, f.col(col)*f.col('Prop') )

select_cols = ['Date', 'DmaCode',  'SubChannel', 'Campaign','Spend', 'MailVolume', 'Response']
final_model_df = final_model_df.select(select_cols)
final_model_df.display()

# COMMAND ----------

final_model_df.groupBy(f.year(f.col('Date')).alias('Year')).agg(f.sum(f.col('MailVolume')).alias('MailVolume'), f.sum(f.col('Response')).alias('Response') ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_AcqMail_WinbackEmail" 
save_df_func(final_model_df, table_name)

# COMMAND ----------

# df = read_table("temp.ja_blend_mmm2_AcqMail_WinbackEmail")
# df.groupBy(f.year(f.col('Date')).alias('Year'), f.col('SubChannel')).agg(f.sum(f.col('MailVolume')).alias('MailVolume'), f.sum(f.col('Response')).alias('Response') ).display()

# COMMAND ----------



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
# df = read_table("uc.journey.sandbox__common_assets__agg_pe_data")
df = read_table("default.priortyengagement20222023_csv")

## Filtering PE ##
filter_cols = [
'Games', 'AARP Job Board', 'AARP Rewards', 'AARP Skills Builder', 'AARP Money Map', 'Online Community', 'State Events', 'Movies for Grownups: Event Registration',
'State Volunteer, Digital Fraud Fighter, Virtual Veterans Brigade, Wish of a Lifetime Volunteer, Disrupt Aging Classroom', 'Volunteer: Submit an Interest Form', 'On membership FAQ Page'
]

df = df.filter(f.col('PE_Name').isin(filter_cols))

### Pivoting Dataset ###
df = df.groupBy('Date').pivot('PE_Name').sum('UniqueVisitors')
df = df.fillna(0)

## Row Sum ##
df = df.withColumn('Total', reduce(lambda a, b: a + b, [f.col(c) for c in df.columns if c != 'Date']))

## Filling NA ###
df = df.toPandas()
df.display()

# COMMAND ----------

while True:
  val=1

# COMMAND ----------

df[['Date', 'Total']].display()

# COMMAND ----------

## Reading Model ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_reg_df_BuyingGroup.csv').drop('Unnamed: 0',axis=1)

## Re-Ordering DataFrame ##
reorder_df = model_df[['DmaCode', 'Date',  'Joins', 'Reg', 'BenRefAll']]
model_df = model_df.drop(columns=['DmaCode', 'Date',  'Joins', 'Reg', 'BenRefAll'])


## Merge Re-Ordered Df ##
model_df = reorder_df.join(model_df)
model_df.head()

# COMMAND ----------

####################################
##### Creating Agg. Dataframe ######
####################################



## Selecting columns ##
primary_cols = ['DmaCode', 'Date']
target_col = ['Joins', 'Reg', 'BenRefAll']
model_cols = ['TV_Imps', 'Search_Clicks', 'Social_Imps', 'LeadGen_Imps', 'Email_Spend', 'DirectMail_Imps', 'AltMedia_Imps', 'Display_Imps', 'Video_Imps' , 'Print_Imps']


Mem_cols = [
'Email_AcqMail_Spend',
'Email_LeadGen_Spend',
'AltMedia_AltMedia_Imps',
'DirectMail_AcqMail_Imps',
'Display_MembershipDigital_Imps',
'Social_MembershipDigital_Imps',
'Search_MembershipDigital_Clicks',
'LeadGen_LeadGen_Imps',
'TV_DRTV_Imps'
]

OtherEM_cols = [
'Email_MediaComOtherEM_Spend',
'Radio_MediaComOtherEM_Spend',
'Audio_MediaComOtherEM_Imps',
'Display_MediaComOtherEM_Imps',
'OOH_MediaComOtherEM_Imps',
'Print_MediaComOtherEM_Imps',
'Social_MediaComOtherEM_Imps',
'TV_MediaComOtherEM_Imps',
'Video_MediaComOtherEM_Imps',
'Search_MediaComOtherEM_Clicks'
]


IcmDigital_cols = [
'Affiliate_IcmDigital_Imps',
'DMP_IcmDigital_Imps',
'Display_IcmDigital_Imps',
'Social_IcmDigital_Imps',
'Video_IcmDigital_Imps',
'Search_IcmDigital_Clicks'
]


AarpBrandSocial_cols = [
'Social_IcmSocial_Imps'
]

MediaComBrand_cols = [
'Audio_MediaComBrand_Imps',
'Display_MediaComBrand_Imps',
'Social_MediaComBrand_Imps',
'TV_MediaComBrand_Imps',
'Video_MediaComBrand_Imps',
'Search_MediaComBrand_Clicks'
]


asi_cols = [
'Display_ASI_Imps',
'Social_ASI_Imps',
'Search_ASI_Clicks'
]


analysis_df = model_df.groupby('Date')[target_col+model_cols+Mem_cols+OtherEM_cols+IcmDigital_cols+AarpBrandSocial_cols+MediaComBrand_cols+asi_cols].sum().reset_index()
analysis_df.display()

# COMMAND ----------

## Getting Reg Data ##
reg_df = analysis_df[['Date', 'Reg']].groupby('Date').sum().reset_index()

## Merging ##
df = df[['Date', 'Total']].merge(analysis_df, on='Date', how='left')
df.display()

# COMMAND ----------

## Utility Function ##
def get_corr(df):

  if 'Date' in df.columns:
    df = df.drop(columns=['Date'])

  correlation_matrix = df.corr().sort_values(by='Total', ascending=False)

  # Create a heatmap
  plt.figure(figsize=(25, 12))
  sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap='coolwarm')
  # sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap='crest')
  plt.title('Correlation Matrix')
  plt.show()
  return correlation_matrix

# COMMAND ----------

### Media and Registrations for filtered date ramge##
t = get_corr(df[['Reg', 'Total']+Mem_cols+OtherEM_cols+IcmDigital_cols+AarpBrandSocial_cols+MediaComBrand_cols+asi_cols])

# COMMAND ----------

t.reset_index().display()

# COMMAND ----------



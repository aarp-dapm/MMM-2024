# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Reading Data

# COMMAND ----------

df = read_table("default.priortyengagement20222023_csv")
df.select('Date').dropDuplicates().display()

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

## Reading Model ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_reg_df_BuyingGroup.csv').drop('Unnamed: 0',axis=1)

## Re-Ordering DataFrame ##
reorder_df = model_df[['DmaCode', 'Date',  'Joins', 'Reg', 'BenRefAll']]
model_df = model_df.drop(columns=['DmaCode', 'Date',  'Joins', 'Reg', 'BenRefAll'])


## Merge Re-Ordered Df ##
model_df = reorder_df.join(model_df)
model_df.head()

# COMMAND ----------

## Getting Reg Data ##
reg_df = model_df[['Date', 'Reg']].groupby('Date').sum().reset_index()

## Merging ##
df = df.merge(reg_df, on='Date', how='left')
df.display()

# COMMAND ----------

## Utility Function ##
def get_corr(df):

  if 'Date' in df.columns:
    df = df.drop(columns=['Date'])

  correlation_matrix = df.corr().sort_values(by='Reg', ascending=False)

  # Create a heatmap
  plt.figure(figsize=(25, 12))
  sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap='coolwarm')
  # sns.heatmap(correlation_matrix, annot=True, fmt=".2f", cmap='crest')
  plt.title('Correlation Matrix')
  plt.show()
  return correlation_matrix

# COMMAND ----------

get_corr(df)

# COMMAND ----------



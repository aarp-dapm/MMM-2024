# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data 

# COMMAND ----------

## Reading Data ##
df = spark.sql("select * from temp.ja_blend_mmm_dma_JOIN")
df = df.filter(f.year(f.col('Date')).isin(['2022', '2023'])).na.fill(0)

## Aggreagting atWeek Level ##
df = df.groupBy('Date').agg(f.sum(f.col('Joins')).alias('Total'))

## Converting to Pandas ##
pd_df = df.toPandas().sort_values(by='Date')
pd_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Finding Fourier Features 

# COMMAND ----------

## Fourier Analysis ##
from scipy.fftpack import fft

y = (pd_df["Total"]).values

plt.figure(figsize=(15,6))
plt.plot(y)
plt.xlabel("Weeks")
plt.ylabel("Joins")
plt.title("Time Domain")

## Yearly line ## 
plt.axvline(51, color='r')

## Quarterly Line 2022 ##
for val in [0, 12,25,38]:
  plt.axvline(val, color='y', linestyle='dashed')

## Quarterly Line 2023 ##
for val in [64,77,90,103]:
  plt.axvline(val, color='g', linestyle='dashed')

# COMMAND ----------

years = len(y)/52 
n = len(y)//2

ff = fft(y)[:n]
x = [i/years for i in list(range(n))]

plt.figure(figsize=(15,6))
plt.plot(x[1:], np.abs(ff)[1:])
plt.xlabel("Frequency")
plt.ylabel("Magnitude")
plt.title("Frequency Domain")

plt.xticks(ticks=[1, 2, 4, 6, 12, 26],labels=["Annual", "Half Yearly", "Qurterly", "BiMonthly", "Monthly", "Weekly"])
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating Fourier Features 

# COMMAND ----------

## Writing Function for Fourier Features ##

def ff(index, n , order):
  
  time = np.arange(len(index), dtype=np.float32)
  k = 2*np.pi*(1/n)*time
  features = {}
  
  for i in order:
    
    features.update({f"sin_{i}": np.sin(i*k), f"cos_{i}": np.cos(i*k),})
    
  return pd.DataFrame(features, index=index)


## Creating Fourier Features ##

order = [1,2,3,4,5,6,7,8,9,10]
index = pd_df['Date'].values
n = len(y[:104])

ff_df = ff(index, n, order).reset_index().sort_values(by='index')
ff_df = ff_df.rename(columns={'index': 'Date'})
ff_df

# COMMAND ----------

## Creating Pandas Dataframe ##
seasonality_df = spark.createDataFrame(ff_df[['Date','sin_1', 'cos_1','sin_2', 'cos_2','sin_3', 'cos_3', 'sin_4', 'cos_4', 'sin_5', 'cos_5', 'sin_6', 'cos_6','sin_7', 'cos_7', 'sin_8', 'cos_8', 'sin_9', 'cos_9', 'sin_10', 'cos_10']])

## Renaming Columns ##
name_map = {'sin_1':'Seasonality_Sin1', 'cos_1':'Seasonality_Cos1','sin_2':'Seasonality_Sin2', 'cos_2':'Seasonality_Cos2', 'sin_3':'Seasonality_Sin3', 'cos_3':'Seasonality_Cos3', 'sin_4':'Seasonality_Sin4', 'cos_4':'Seasonality_Cos4', 'sin_5':'Seasonality_Sin5', 'cos_5':'Seasonality_Cos5', 'sin_6':'Seasonality_Sin6', 'cos_6':'Seasonality_Cos6', 'sin_7':'Seasonality_Sin7', 'cos_7':'Seasonality_Cos7', 'sin_8':'Seasonality_Sin8', 'cos_8':'Seasonality_Cos8', 'sin_9':'Seasonality_Sin9', 'cos_9':'Seasonality_Cos9', 'sin_10':'Seasonality_Sin10', 'cos_10':'Seasonality_Cos10'}
seasonality_df = rename_cols_func(seasonality_df, name_map)
seasonality_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving Dataframe

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_seasonality"  
save_df_func(seasonality_df, table_name)

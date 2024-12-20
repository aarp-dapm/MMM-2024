# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading Data

# COMMAND ----------

## Reading Data ##
init_brand_equity = spark.table("temp.ja_blend_mmm2_BrandMetrics_YouGovVo2").filter( f.year(f.col('Date')).isin([2022, 2023])).toPandas() ## Loading Brand Equity 
init_brand_equity['Date'] = init_brand_equity['Date'].astype('str') 

brand_media = spark.table("temp.mm_blend_mmm_MediaCom_BrandMedia_Pivoted").toPandas() ## Loading Brand Spend
joins_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_initialmodel_results.csv').drop('Unnamed: 0',axis=1) ## Loading Join Data



## Creating New Dataframe ##

joins_df["base_joins"] = joins_df[['Seasonality_Feb','Seasonality_Mar', 'Seasonality_Apr', 'Seasonality_Dec','Fixed Dma Effect','July0422_lag']].sum(axis=1)
model_df = pd.merge(joins_df[['Date','base_joins']], brand_media, on='Date')
model_df.display()

# COMMAND ----------

## Ploting Target Variable ##

model_df["base_joins_rolling_avg"] = model_df["base_joins"].rolling(window=2).mean()


plt.figure(figsize=(20,6))
plt.plot(model_df['Date'], model_df['base_joins'])
plt.plot(model_df['Date'], model_df['base_joins_rolling_avg'])
plt.tight_layout()
plt.show()

# COMMAND ----------

## FFT Analysis ##
y = model_df["base_joins"]

## Deocmposing the signal ##
coeff = np.fft.fft(y)
N = len(coeff)

## Storing each frequency of Signal ##
freq_sig = {}
n = np.arange(N)

for freq, val in enumerate(coeff):
  freq_sig[freq] = (val.real)*np.cos(2*np.pi*freq*n/N) - (val.imag)*np.sin(2*np.pi*freq*n/N)



## Recreating Signal ##

def recreate_signal(k, freq_sig):
  
  top_k_coeff_ind = np.argsort(np.abs(coeff))[-k:][::-1]

  keys = top_k_coeff_ind
  subset_sig = {key:freq_sig[key] for key in keys}


  y_hat = np.asarray(list(subset_sig.values())).sum(axis=0)*(1/N)
  y_hat = pd.Series(y_hat)


  ## Plot ##
  plt.figure(figsize=(20,10))

  plt.plot(y)
  plt.plot(y_hat,label=f'K={k}')

  plt.show()

  return y_hat


### New Target Variable ###
new_sig = recreate_signal(80, freq_sig)

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 EDA and Variable Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 EDA

# COMMAND ----------

## Visualizing  Base Joins and Purchase Intent ##


## PLotting on Primary Axis ##
fig, ax1 = plt.subplots(figsize=(20,10))
ax1.plot(model_df["base_joins"], label = 'Base Joins')


## PLotting on Second Axis ##
ax2 = ax1.twinx()
ax2.plot(init_brand_equity['Purchase_Intent'], label='Purchase_Intent', color='g')

init_brand_equity['Purchase_Intent_rolling'] = init_brand_equity['Purchase_Intent'].rolling(window=3).mean()
ax2.plot(init_brand_equity['Purchase_Intent_rolling'], label='Purchase_Intent Moving Avg', color='r')

## Show the plot ##
plt.show()

# COMMAND ----------

## Checking Corrleation ##
corr1 = model_df["base_joins"].corr(init_brand_equity['Purchase_Intent'])
corr2 = joins_df["Joins_unnorm"].corr(init_brand_equity['Purchase_Intent'])
corr3 = model_df["base_joins"].corr(init_brand_equity['Purchase_Intent_rolling'])
corr4 = model_df["base_joins_rolling_avg"].corr(init_brand_equity['Purchase_Intent_rolling'])
corr4


# COMMAND ----------

'''
Value of Corr1 is 0.03 (very low)
Value of Corr3 is 0.07 (Little better)
Value of Corr4 is 0.03 (Again Low)

Conclusion: Regress "base_joins" on "Purchase_Intent_rolling"
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Variable Transformation (and Creation)

# COMMAND ----------

## Creating Dataframe ##
target_var = ['Date'] + ['base_joins_rolling_avg']
channel_cols = ['Audio_Imps', 'Display_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps']

df = model_df[target_var + channel_cols]


## Merging "Purchase Intent" in it ##
df = df.merge(init_brand_equity[['Date', 'Purchase_Intent_rolling', 'Purchase_Intent']], on='Date', how='left')

# COMMAND ----------

## Creating Holiday Var ##
df['Holiday_July4'] = df['Date'].apply(lambda x: 1 if x == '2022-07-11' else 0)

## Adding Intercept ##
df['Intercept'] = 1

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Model Development

# COMMAND ----------

## Target Var ##
target_var = 'base_joins_rolling_avg'

## Brand Equity Var ##
equity_var =['Purchase_Intent_rolling']

## Other Vars  ##
holiday_cols = ['Holiday_July4']

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Iteration 1

# COMMAND ----------

## Creating Model ##
input_vars_string = '+'.join(equity_var + holiday_cols)
model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + equity_var + holiday_cols + ['Intercept']].dropna() ).fit()


# COMMAND ----------

model.summary().tables[1]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Iteration 2 (Modelling Differences)

# COMMAND ----------

## Adding New Target Variable ##
df['new_signal'] = new_sig


## Getting Differences wrt to previous value ##
df['Target_Var_Diff'] = df['new_signal'] - df['new_signal'].shift(-1) 
df['Purchase_Intent_Diff'] = df['Purchase_Intent'] - df['Purchase_Intent'].shift(-1)


## EDA ##
fig, ax1 = plt.subplots(figsize=(20,10))
ax1.plot(df['Target_Var_Diff'], label = 'Target_Var_Diff')

ax2 = ax1.twinx()
ax2.plot(df['Purchase_Intent_Diff'], label = 'Purchase_Intent_Diff', color = 'g')

fig.show()

# COMMAND ----------

## Plotting on Scatter ##
plt.figure(figsize=(20,10))
plt.scatter(df['Target_Var_Diff'], df['Purchase_Intent_Diff'])

plt.xlabel("Target_Var_Diff")
plt.ylabel("Purchase_Intent_Diff")
plt.show()

# COMMAND ----------

## Correlation on Differences ##
corr = df['Target_Var_Diff'].corr(df['Purchase_Intent_Diff'])
corr

# COMMAND ----------

## Target Var ##
# target_var = 'base_joins_rolling_avg'
target_var = 'Target_Var_Diff'

## Brand Equity Var ##
equity_var =['Purchase_Intent_Diff']

## Other Vars  ##
holiday_cols = ['Holiday_July4']

# COMMAND ----------

## Creating Model ##
input_vars_string = '+'.join(equity_var + holiday_cols)
model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + equity_var + holiday_cols + ['Intercept']].dropna() ).fit()

# COMMAND ----------

model.summary().tables[1]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Iteration 3 (Modelling Differences without Holiday Flag)

# COMMAND ----------

## Creating Model ##
input_vars_string = '+'.join(equity_var)
model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + equity_var + holiday_cols + ['Intercept']].dropna() ).fit()

# COMMAND ----------

model.summary().tables[1]

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.0 Analyzing Results

# COMMAND ----------



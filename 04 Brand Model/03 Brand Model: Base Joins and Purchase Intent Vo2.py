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

### Functions ####

def adstock_func_week_cut(var_data, decay_rate, cutoff_weeks):
    adstocked_data = np.zeros_like(var_data)
    
    for t in range(len(var_data)):
        for i in range(cutoff_weeks):
            if t - i >= 0:
                adstocked_data[t] += var_data[t - i] * (decay_rate ** i)
    
    return pd.Series(adstocked_data, index=var_data.index)

# COMMAND ----------

## Creating Dataframe ##
target_var = ['Date'] + ['base_joins_rolling_avg']
channel_cols = ['Audio_Imps', 'Display_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps']

df = model_df[target_var + channel_cols]


## Merging "Purchase Intent" in it ##
df = df.merge(init_brand_equity[['Date', 'Purchase_Intent_rolling', 'Purchase_Intent']], on='Date', how='left')

# COMMAND ----------

#############################
## Variable Transformation ##
#############################

brand_metric = 'Purchase_Intent'

## Creating Lags ##
lag_list = [1, 2, 3, 4, 5, 6, 7, 8]
for ch_cols in df.columns:
  if ch_cols == brand_metric:
    ct = 0
    while ct<len(lag_list):
      df[ch_cols+f'_Lag{lag_list[ct]}'] = df[ch_cols].shift(lag_list[ct])
      ct+=1 


## Adding Adstock ##
adstock_list = [0.8, 0.7, 0.5, 0.3]
for ch_cols in df.columns:
  if ch_cols.startswith(brand_metric) :
    ct=0
    while ct<len(adstock_list):

      df[ch_cols + f'_AdStock{int(adstock_list[ct]*100)}'] = adstock_func_week_cut(df[ch_cols], adstock_list[ct], 12)
      ct+=1


# COMMAND ----------

## Creating Holiday Var ##
df['Holiday_July4'] = df['Date'].apply(lambda x: 1 if x == '2022-07-11' else 0)

## Adding Intercept ##
df['Intercept'] = 1

# COMMAND ----------

## Defining Dictionary to save data ##
exp_dict = {}

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Model Development

# COMMAND ----------

## Target Var ##
target_var = 'base_joins_rolling_avg'

## Brand Equity Var ##
equity_var_list =['Purchase_Intent_Lag1', 'Purchase_Intent_Lag2', 'Purchase_Intent_Lag3', 'Purchase_Intent_Lag4', 'Purchase_Intent_Lag5', 'Purchase_Intent_Lag6', 'Purchase_Intent_Lag7', 'Purchase_Intent_Lag8', 'Purchase_Intent_rolling_AdStock80', 'Purchase_Intent_rolling_AdStock70', 'Purchase_Intent_rolling_AdStock50', 'Purchase_Intent_rolling_AdStock30', 'Purchase_Intent_AdStock80', 'Purchase_Intent_AdStock70', 'Purchase_Intent_AdStock50',
'Purchase_Intent_AdStock30', 'Purchase_Intent_Lag1_AdStock80', 'Purchase_Intent_Lag1_AdStock70', 'Purchase_Intent_Lag1_AdStock50', 'Purchase_Intent_Lag1_AdStock30', 'Purchase_Intent_Lag2_AdStock80', 'Purchase_Intent_Lag2_AdStock70', 'Purchase_Intent_Lag2_AdStock50', 'Purchase_Intent_Lag2_AdStock30', 'Purchase_Intent_Lag3_AdStock80', 'Purchase_Intent_Lag3_AdStock70', 'Purchase_Intent_Lag3_AdStock50', 'Purchase_Intent_Lag3_AdStock30', 'Purchase_Intent_Lag4_AdStock80',
'Purchase_Intent_Lag4_AdStock70', 'Purchase_Intent_Lag4_AdStock50', 'Purchase_Intent_Lag4_AdStock30', 'Purchase_Intent_Lag5_AdStock80', 'Purchase_Intent_Lag5_AdStock70', 'Purchase_Intent_Lag5_AdStock50', 'Purchase_Intent_Lag5_AdStock30', 'Purchase_Intent_Lag6_AdStock80', 'Purchase_Intent_Lag6_AdStock70', 'Purchase_Intent_Lag6_AdStock50', 'Purchase_Intent_Lag6_AdStock30', 'Purchase_Intent_Lag7_AdStock80', 'Purchase_Intent_Lag7_AdStock70', 'Purchase_Intent_Lag7_AdStock50',
'Purchase_Intent_Lag7_AdStock30', 'Purchase_Intent_Lag8_AdStock80', 'Purchase_Intent_Lag8_AdStock70', 'Purchase_Intent_Lag8_AdStock50', 'Purchase_Intent_Lag8_AdStock30']

## Other Vars  ##
holiday_cols = ['Holiday_July4']

# COMMAND ----------

## Creating Model ##

for idx, equity_var_ch in tqdm.tqdm(enumerate(equity_var_list)):
  input_vars_string = '+'.join([equity_var_ch] + holiday_cols)
  model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + [equity_var_ch] + holiday_cols + ['Intercept']].dropna() ).fit()

  ## Getting Summary ##
  summary_df = pd.DataFrame(model.summary().tables[1].data[1:], columns=model.summary().tables[1].data[0]).set_index('')
  summary_df = summary_df.join(( (model.params)*model.model.exog.sum(axis=0) ).rename('Contri'))
  summary_df['Target_Var'] = model.model.endog.sum()
  

  ## Logging Results ##
  exp_dict[idx] = summary_df

  # if idx == 10:
  #   break


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Saving Experiment Data

# COMMAND ----------

## Saving Results ##
# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/brand_model/brandmodel_11_02_OrganicJoins_PurchaseIntent.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(exp_dict, f)


# Read the dictionary from the JSON file
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict[0])

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.0 Analyzing Results

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Coeff. Analysis

# COMMAND ----------

##################
## Reading Data ##
##################

file_path = '/dbfs/blend360/sandbox/mmm/brand_model/brandmodel_11_02_OrganicJoins_PurchaseIntent.json'

# Read the dictionary from the JSON file
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)




########################
## CReating Dataframe ##
########################


## Appending only Summary ##
df_summary_list = []
model_object_list = []
for key, value in tqdm.tqdm(loaded_dict.items()):
  value.insert(0, 'ModelNumber', key)
  df_summary_list.append(value)


## Concat Data ##
analysis_df = pd.concat(df_summary_list, axis=0)


## creating new column name ##
analysis_df['new_name'] = np.where( analysis_df.index.str.contains(brand_metric), analysis_df.index.to_series().apply(lambda x: "_".join(x.split("_")[:2])),analysis_df.index)

## Coeff Table ##
pivot_coeff_df = analysis_df.pivot(index = 'new_name', columns = 'ModelNumber', values = 'coef').T.reset_index()
pivot_coeff_df = pivot_coeff_df.astype('float')
pivot_coeff_df['ModelNumber'] = pivot_coeff_df['ModelNumber'].astype('int')


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Pvals. Analysis

# COMMAND ----------

## PValue Table ##
pivot_pval_df = analysis_df.pivot(index = 'new_name', columns = 'ModelNumber', values = 'P>|t|').T.reset_index()
pivot_pval_df  = pivot_pval_df.astype('float')
pivot_pval_df['ModelNumber'] = pivot_pval_df['ModelNumber'].astype('int')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Contribution Analysis

# COMMAND ----------

## Contri Table ##
pivot_contri_df = analysis_df.pivot(index = 'new_name', columns = 'ModelNumber', values = 'Contri').T.reset_index()
pivot_contri_df  = pivot_contri_df.astype('float')
pivot_contri_df['ModelNumber'] = pivot_contri_df['ModelNumber'].astype('int')
pivot_contri_df['TotalContri'] = pivot_contri_df.sum(axis=1)-pivot_contri_df['ModelNumber']

### Add Target Variable ###
pivot_target_var_df = analysis_df[['ModelNumber', 'Target_Var']].drop_duplicates()
pivot_contri_df = pivot_contri_df.merge(pivot_target_var_df, on='ModelNumber', how='inner')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.9 Joining all Analysis

# COMMAND ----------

## Changing Column Names ##
pivot_coeff_df.columns  = [i+"_coeff" if i!='ModelNumber' else i  for i in pivot_coeff_df.columns]
pivot_pval_df.columns  = [i+"_pval" if i!='ModelNumber' else i  for i in pivot_pval_df.columns]
pivot_contri_df.columns  = [i+"_contri" if i!='ModelNumber' else i  for i in pivot_contri_df.columns]

## Joining Dataframe ##
pivot_coeff_pval_df = pivot_coeff_df.merge(pivot_pval_df, on='ModelNumber', how='inner').merge(pivot_contri_df, on='ModelNumber', how='inner')

# COMMAND ----------

pivot_coeff_pval_df.display()

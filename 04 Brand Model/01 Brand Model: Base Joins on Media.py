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
init_brand_equity = spark.table("temp.ja_blend_mmm2_BrandMetrics_YouGovVo2") ## Loading Brand Equity 
brand_media = spark.table("temp.mm_blend_mmm_MediaCom_BrandMedia_Pivoted").toPandas() ## Loading Brand Spend
joins_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_initialmodel_results.csv').drop('Unnamed: 0',axis=1) ## Loading Jo9in Data



## Creating New Dataframe ##

joins_df["base_joins"] = joins_df[['Seasonality_Feb','Seasonality_Mar', 'Seasonality_Apr', 'Seasonality_Dec','Fixed Dma Effect','July0422_lag']].sum(axis=1)
model_df = pd.merge(joins_df[['Date','base_joins']], brand_media, on='Date')
model_df.display()

# COMMAND ----------

model_df[[]]

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
# MAGIC # 2.0 Variable Transformation

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

################
## Model Vars ##
################



target_var = ['Date'] + ['base_joins_rolling_avg']
channel_cols = ['Audio_Imps', 'Display_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps']

df = model_df[target_var + channel_cols]


#############################
## Preprocessing Variables ##
#############################

for ch in channel_cols:
  if ch not in target_var:
    df[ch] = (df[ch] - df[ch].min() )/(df[ch].max() - df[ch].min())




#############################
## Variable Transformation ##
#############################

## Creating Lags ##
lag_list = [13, 26, 52]
for ch_cols in df.columns:
  if ch_cols not in target_var:
    ct = 0
    while ct<len(lag_list):
      df[ch_cols+f'_Lag{lag_list[ct]}'] = df[ch_cols].shift(lag_list[ct])
      ct+=1 


## Creating Saturation ##
sat_list =  [ 0.5, 0.3, 0.1]
for ch_cols in df.columns:
  if ch_cols not in target_var:
    ct=0
    while ct<len(sat_list):

      df[ch_cols + f'_Sat{int(sat_list[ct]*100)}'] = df[ch_cols]**sat_list[ct]
      ct+=1

## Adding Adstock ##
adstock_list = [0.8, 0.7, 0.6]
for ch_cols in df.columns:
  if ch_cols.startswith('TV') :
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

## Columns Available for Model Develpoment ##
video_cols = ['Video_Imps_Lag13_Sat50', 'Video_Imps_Lag13_Sat30', 'Video_Imps_Lag13_Sat10', 'Video_Imps_Lag26_Sat50', 'Video_Imps_Lag26_Sat30', 'Video_Imps_Lag26_Sat10', 'Video_Imps_Lag52_Sat50', 'Video_Imps_Lag52_Sat30', 'Video_Imps_Lag52_Sat10']

# tv_cols = ['TV_Imps_Lag13_Sat50', 'TV_Imps_Lag13_Sat30', 'TV_Imps_Lag13_Sat10', 'TV_Imps_Lag26_Sat50', 'TV_Imps_Lag26_Sat30', 'TV_Imps_Lag26_Sat10', 'TV_Imps_Lag52_Sat50', 'TV_Imps_Lag52_Sat30', 'TV_Imps_Lag52_Sat10']
tv_cols = ['TV_Imps_Lag13_Sat50_AdStock80', 'TV_Imps_Lag13_Sat50_AdStock70', 'TV_Imps_Lag13_Sat50_AdStock60', 'TV_Imps_Lag13_Sat30_AdStock80', 'TV_Imps_Lag13_Sat30_AdStock70', 'TV_Imps_Lag13_Sat30_AdStock60', 'TV_Imps_Lag13_Sat10_AdStock80', 'TV_Imps_Lag13_Sat10_AdStock70', 'TV_Imps_Lag13_Sat10_AdStock60', 'TV_Imps_Lag26_Sat50_AdStock80', 'TV_Imps_Lag26_Sat50_AdStock70', 'TV_Imps_Lag26_Sat50_AdStock60', 'TV_Imps_Lag26_Sat30_AdStock80', 'TV_Imps_Lag26_Sat30_AdStock70', 'TV_Imps_Lag26_Sat30_AdStock60', 'TV_Imps_Lag26_Sat10_AdStock80', 'TV_Imps_Lag26_Sat10_AdStock70', 'TV_Imps_Lag26_Sat10_AdStock60', 'TV_Imps_Lag52_Sat50_AdStock80', 'TV_Imps_Lag52_Sat50_AdStock70', 'TV_Imps_Lag52_Sat50_AdStock60', 'TV_Imps_Lag52_Sat30_AdStock80', 'TV_Imps_Lag52_Sat30_AdStock70', 'TV_Imps_Lag52_Sat30_AdStock60', 'TV_Imps_Lag52_Sat10_AdStock80', 'TV_Imps_Lag52_Sat10_AdStock70', 'TV_Imps_Lag52_Sat10_AdStock60']


social_cols = ['Social_Imps_Lag13_Sat50', 'Social_Imps_Lag13_Sat30', 'Social_Imps_Lag13_Sat10', 'Social_Imps_Lag26_Sat50', 'Social_Imps_Lag26_Sat30', 'Social_Imps_Lag26_Sat10', 'Social_Imps_Lag52_Sat50', 'Social_Imps_Lag52_Sat30',
       'Social_Imps_Lag52_Sat10']
display_cols = ['Display_Imps_Lag13_Sat50', 'Display_Imps_Lag13_Sat30', 'Display_Imps_Lag13_Sat10', 'Display_Imps_Lag26_Sat50', 'Display_Imps_Lag26_Sat30', 'Display_Imps_Lag26_Sat10', 'Display_Imps_Lag52_Sat50', 'Display_Imps_Lag52_Sat30', 'Display_Imps_Lag52_Sat10']

audio_cols = ['Audio_Imps_Lag13_Sat50', 'Audio_Imps_Lag13_Sat30', 'Audio_Imps_Lag13_Sat10', 'Audio_Imps_Lag26_Sat50', 'Audio_Imps_Lag26_Sat30', 'Audio_Imps_Lag26_Sat10', 'Audio_Imps_Lag52_Sat50', 'Audio_Imps_Lag52_Sat30', 'Audio_Imps_Lag52_Sat10']

search_cols =['Search_Clicks_Lag13_Sat50', 'Search_Clicks_Lag13_Sat30', 'Search_Clicks_Lag13_Sat10', 'Search_Clicks_Lag26_Sat50', 'Search_Clicks_Lag26_Sat30', 'Search_Clicks_Lag26_Sat10', 'Search_Clicks_Lag52_Sat50','Search_Clicks_Lag52_Sat30', 'Search_Clicks_Lag52_Sat10']

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Model Development

# COMMAND ----------

## Target Var ##
target_var = 'base_joins_rolling_avg'

## Media Var ##
video_cols = ['Video_Imps_Lag13_Sat50', 'Video_Imps_Lag13_Sat30', 'Video_Imps_Lag13_Sat10', 'Video_Imps_Lag26_Sat50', 'Video_Imps_Lag26_Sat30', 'Video_Imps_Lag26_Sat10', 'Video_Imps_Lag52_Sat50', 'Video_Imps_Lag52_Sat30', 'Video_Imps_Lag52_Sat10']

# tv_cols = ['TV_Imps_Lag13_Sat50', 'TV_Imps_Lag13_Sat30', 'TV_Imps_Lag13_Sat10', 'TV_Imps_Lag26_Sat50', 'TV_Imps_Lag26_Sat30', 'TV_Imps_Lag26_Sat10', 'TV_Imps_Lag52_Sat50', 'TV_Imps_Lag52_Sat30', 'TV_Imps_Lag52_Sat10']
tv_cols = ['TV_Imps_Lag13_Sat50_AdStock80', 'TV_Imps_Lag13_Sat50_AdStock70', 'TV_Imps_Lag13_Sat50_AdStock60', 'TV_Imps_Lag13_Sat30_AdStock80', 'TV_Imps_Lag13_Sat30_AdStock70', 'TV_Imps_Lag13_Sat30_AdStock60', 'TV_Imps_Lag13_Sat10_AdStock80', 'TV_Imps_Lag13_Sat10_AdStock70', 'TV_Imps_Lag13_Sat10_AdStock60', 'TV_Imps_Lag26_Sat50_AdStock80', 'TV_Imps_Lag26_Sat50_AdStock70', 'TV_Imps_Lag26_Sat50_AdStock60', 'TV_Imps_Lag26_Sat30_AdStock80', 'TV_Imps_Lag26_Sat30_AdStock70', 'TV_Imps_Lag26_Sat30_AdStock60', 'TV_Imps_Lag26_Sat10_AdStock80', 'TV_Imps_Lag26_Sat10_AdStock70', 'TV_Imps_Lag26_Sat10_AdStock60', 'TV_Imps_Lag52_Sat50_AdStock80', 'TV_Imps_Lag52_Sat50_AdStock70', 'TV_Imps_Lag52_Sat50_AdStock60', 'TV_Imps_Lag52_Sat30_AdStock80', 'TV_Imps_Lag52_Sat30_AdStock70', 'TV_Imps_Lag52_Sat30_AdStock60', 'TV_Imps_Lag52_Sat10_AdStock80', 'TV_Imps_Lag52_Sat10_AdStock70', 'TV_Imps_Lag52_Sat10_AdStock60']

social_cols = ['Social_Imps_Lag13_Sat50', 'Social_Imps_Lag13_Sat30', 'Social_Imps_Lag13_Sat10', 'Social_Imps_Lag26_Sat50', 'Social_Imps_Lag26_Sat30', 'Social_Imps_Lag26_Sat10', 'Social_Imps_Lag52_Sat50', 'Social_Imps_Lag52_Sat30',
       'Social_Imps_Lag52_Sat10']
display_cols = ['Display_Imps_Lag13_Sat50', 'Display_Imps_Lag13_Sat30', 'Display_Imps_Lag13_Sat10', 'Display_Imps_Lag26_Sat50', 'Display_Imps_Lag26_Sat30', 'Display_Imps_Lag26_Sat10', 'Display_Imps_Lag52_Sat50', 'Display_Imps_Lag52_Sat30', 'Display_Imps_Lag52_Sat10']

search_cols = ['Search_Clicks_Sat50', 'Search_Clicks_Sat30', 'Search_Clicks_Sat10']
# search_cols = ['Search_Imps_Sat50', 'Search_Imps_Sat30', 'Search_Imps_Sat10']

## Other Vars  ##
holiday_cols = ['Holiday_July4']

# COMMAND ----------

## Defining Dictionary to save data ##
exp_dict = {}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Iteration 1

# COMMAND ----------

## Creating Combinations ##
combinations = list(itertools.product(video_cols, tv_cols, social_cols, display_cols, search_cols))
combinations = [list(pair) for pair in combinations]


## Running Models ##
for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

  input_vars_string = '+'.join(channel_cols+ holiday_cols)
  model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols+ holiday_cols +['intercept']].dropna() ).fit()
  

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
# MAGIC ## 3.1 Iteration 2

# COMMAND ----------

## Creating Combinations ##
combinations = list(itertools.product(video_cols, tv_cols, social_cols, display_cols))
combinations = [list(pair) for pair in combinations]


## Running Models ##
for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

  input_vars_string = '+'.join(channel_cols+ holiday_cols)
  model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols+ holiday_cols +['Intercept']].dropna() ).fit()
  

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
# MAGIC ## 3.1 Iteration 3

# COMMAND ----------

## Creating Combinations ##
combinations = list(itertools.product(video_cols, tv_cols, social_cols))
combinations = [list(pair) for pair in combinations]
print(len(combinations))


## Running Models ##
for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

  input_vars_string = '+'.join(channel_cols+ holiday_cols)
  model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols+ holiday_cols +['Intercept']].dropna() ).fit()
  

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
# MAGIC ## 3.1 Iteration 4

# COMMAND ----------

## Creating Additional Seasonal Variables ##
df['Seasonal_Feb'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 2 else 0)
df['Seasonal_Mar'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 3 else 0)
df['Seasonal_Apr'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 4 else 0)
df['Seasonal_Dec'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 12 else 0)

seasonal_cols = ['Seasonal_Feb', 'Seasonal_Mar', 'Seasonal_Apr', 'Seasonal_Dec']

## Creating Combinations ##
combinations = list(itertools.product(video_cols, tv_cols, social_cols))
combinations = [list(pair) for pair in combinations]



## Running Models ##
for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

  input_vars_string = '+'.join(channel_cols+ holiday_cols + seasonal_cols)
  model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols+ holiday_cols + seasonal_cols +['Intercept']].dropna() ).fit()
  

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
# MAGIC ## 3.1 Iteration 5

# COMMAND ----------

## Creating Additional Seasonal Variables ##
df['Seasonal_Feb'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 2 else 0)
df['Seasonal_Mar'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 3 else 0)
df['Seasonal_Apr'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 4 else 0)
df['Seasonal_Dec'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 12 else 0)

seasonal_cols = ['Seasonal_Feb', 'Seasonal_Mar', 'Seasonal_Apr', 'Seasonal_Dec']

## Creating Combinations ##

'''

In this iteration only lag 52 for TV was used 

'''


combinations = list(itertools.product(video_cols, tv_cols, social_cols))  
combinations = [list(pair) for pair in combinations]



## Running Models ##
for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

  input_vars_string = '+'.join(channel_cols+ holiday_cols + seasonal_cols)
  model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols+ holiday_cols + seasonal_cols +['Intercept']].dropna() ).fit()
  

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
# MAGIC ## 3.1 Iteration 6

# COMMAND ----------

## Creating Additional Seasonal Variables ##
df['Seasonal_Feb'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 2 else 0)
df['Seasonal_Mar'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 3 else 0)
df['Seasonal_Apr'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 4 else 0)
df['Seasonal_Dec'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 12 else 0)

seasonal_cols = ['Seasonal_Feb', 'Seasonal_Mar', 'Seasonal_Apr', 'Seasonal_Dec']

## Creating Combinations ##


'''

In this iteration all lags of TV were used 

'''


combinations = list(itertools.product(video_cols, tv_cols, social_cols))
combinations = [list(pair) for pair in combinations]



## Running Models ##
for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

  input_vars_string = '+'.join(channel_cols+ holiday_cols + seasonal_cols)
  model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols+ holiday_cols + seasonal_cols +['Intercept']].dropna() ).fit()
  

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
# MAGIC ## 3.1 Iteration 7

# COMMAND ----------

new_sig = recreate_signal(80, freq_sig)

# COMMAND ----------

##############################
## Variable Creation Starts ##
##############################

## Creating Additional Seasonal Variables ##
df['Seasonal_Feb'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 2 else 0)
df['Seasonal_Mar'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 3 else 0)
df['Seasonal_Apr'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 4 else 0)
df['Seasonal_Dec'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 12 else 0)

seasonal_cols = ['Seasonal_Feb', 'Seasonal_Mar', 'Seasonal_Apr', 'Seasonal_Dec']

## Creating New Target Variables ##
df['new_target_variable'] = new_sig
target_var = 'new_target_variable'



##############################
## Variable Creation Ends   ##
##############################


## Creating Combinations ##


'''

In this iteration all lags of TV were used 

'''


combinations = list(itertools.product(video_cols, tv_cols, social_cols))
combinations = [list(pair) for pair in combinations]



## Running Models ##
for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

  input_vars_string = '+'.join(channel_cols+ holiday_cols + seasonal_cols)
  model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols+ holiday_cols + seasonal_cols +['Intercept']].dropna() ).fit()
  

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
# MAGIC ## 3.1 Iteration 8

# COMMAND ----------

##############################
## Variable Creation Starts ##
##############################

## Creating Additional Seasonal Variables ##
df['Seasonal_Feb'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 2 else 0)
df['Seasonal_Mar'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 3 else 0)
df['Seasonal_Apr'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 4 else 0)
df['Seasonal_Dec'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 12 else 0)

seasonal_cols = ['Seasonal_Feb', 'Seasonal_Mar', 'Seasonal_Apr', 'Seasonal_Dec']

## Using old Target Variables ##
target_var = 'base_joins_rolling_avg'



##############################
## Variable Creation Ends   ##
##############################


## Creating Combinations ##


'''

In this iteration all lags of TV were used 

'''


combinations = list(itertools.product(display_cols, tv_cols, social_cols))
combinations = [list(pair) for pair in combinations]



## Running Models ##
for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

  input_vars_string = '+'.join(channel_cols+ holiday_cols + seasonal_cols)
  model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols+ holiday_cols + seasonal_cols +['Intercept']].dropna() ).fit()
  

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
# MAGIC ## 3.1 Iteration 9

# COMMAND ----------

new_sig = recreate_signal(80, freq_sig)

# COMMAND ----------

##############################
## Variable Creation Starts ##
##############################

## Creating Additional Seasonal Variables ##
df['Seasonal_Feb'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 2 else 0)
df['Seasonal_Mar'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 3 else 0)
df['Seasonal_Apr'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 4 else 0)
df['Seasonal_Dec'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 12 else 0)

seasonal_cols = ['Seasonal_Feb', 'Seasonal_Mar', 'Seasonal_Apr', 'Seasonal_Dec']

## Creating New Target Variables ##
df['new_target_variable'] = new_sig
target_var = 'new_target_variable'



##############################
## Variable Creation Ends   ##
##############################


## Creating Combinations ##


'''

In this iteration all lags of TV were used 

'''


combinations = list(itertools.product(display_cols, tv_cols, social_cols))
combinations = [list(pair) for pair in combinations]



## Running Models ##
for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

  input_vars_string = '+'.join(channel_cols+ holiday_cols + seasonal_cols)
  model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols+ holiday_cols + seasonal_cols +['Intercept']].dropna() ).fit()
  

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
# MAGIC ## 3.1 Iteration 10

# COMMAND ----------

##############################
## Variable Creation Starts ##
##############################

## Creating Additional Seasonal Variables ##
df['Seasonal_Feb'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 2 else 0)
df['Seasonal_Mar'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 3 else 0)
df['Seasonal_Apr'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 4 else 0)
df['Seasonal_Dec'] = df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 12 else 0)

seasonal_cols = ['Seasonal_Feb', 'Seasonal_Mar', 'Seasonal_Apr', 'Seasonal_Dec']

## Using old Target Variables ##
target_var = 'base_joins_rolling_avg'



##############################
## Variable Creation Ends   ##
##############################


## Creating Combinations ##


'''

In this iteration all lags of TV were used 

'''


combinations = list(itertools.product(display_cols, video_cols, tv_cols, social_cols))
combinations = [list(pair) for pair in combinations]



## Running Models ##
for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

  input_vars_string = '+'.join(channel_cols+ holiday_cols + seasonal_cols)
  model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols+ holiday_cols + seasonal_cols +['Intercept']].dropna() ).fit()
  

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
file_path = '/dbfs/blend360/sandbox/mmm/brand_model/brandmodel_10_15_Display_TV_Video_Social.json'

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

file_path = '/dbfs/blend360/sandbox/mmm/brand_model/brandmodel_10_15_Display_TV_Video_Social.json'


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
analysis_df['new_name'] = np.where( analysis_df.index.str.contains('Sat'), analysis_df.index.to_series().apply(lambda x: "_".join(x.split("_")[:2])),analysis_df.index)

## Coeff Table ##
pivot_coeff_df = analysis_df.pivot(index = 'new_name', columns = 'ModelNumber', values = 'coef').T.reset_index()
pivot_coeff_df = pivot_coeff_df.astype('float')
pivot_coeff_df['ModelNumber'] = pivot_coeff_df['ModelNumber'].astype('int')



pivot_coeff_df.display()

# COMMAND ----------

## Filtering for Models with Positive Coefficients ##

# pivot_coeff_df['flag'] = pivot_coeff_df.apply( lambda x: 1 if ( (x['Display_Imps'] > 0) and   (x['Intercept'] > 0) and (x['Search_Clicks'] > 0) and (x['Social_Imps'] > 0) and (x['TV_Imps'] > 0) and (x['Video_Imps'] > 0)) else 0, axis=1)
# pivot_coeff_df['flag'] = pivot_coeff_df.apply( lambda x: 1 if ( (x['Display_Imps'] > 0) and   (x['Intercept'] > 0) and (x['Social_Imps'] > 0) and (x['TV_Imps'] > 0) and (x['Video_Imps'] > 0)) else 0, axis=1)
# pivot_coeff_df[pivot_coeff_df['flag'] == 1].display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Pvals. Analysis

# COMMAND ----------

## PValue Table ##
pivot_pval_df = analysis_df.pivot(index = 'new_name', columns = 'ModelNumber', values = 'P>|t|').T.reset_index()
pivot_pval_df  = pivot_pval_df.astype('float')
pivot_pval_df['ModelNumber'] = pivot_pval_df['ModelNumber'].astype('int')

# COMMAND ----------

## Filtering for Models for significant Coefficients ##

# pivot_pval_df['flag'] = pivot_pval_df.apply( lambda x: 1 if ( (x['Display_Imps'] < 0.20 ) and   (x['Intercept'] < 0.20 ) and (x['Social_Imps'] < 0.20 ) and (x['TV_Imps'] < 0.20 ) and (x['Video_Imps'] < 0.20 )) else 0, axis=1)
# pivot_pval_df['flag'] = pivot_pval_df.apply( lambda x: 1 if ( (x['Display_Imps'] < 0.20 ) and   (x['Intercept'] < 0.20 ) and (x['Search_Clicks'] < 0.20 ) and (x['Social_Imps'] < 0.20 ) and (x['TV_Imps'] < 0.20 ) and (x['Video_Imps'] < 0.20 )) else 0, axis=1)
# pivot_pval_df[pivot_pval_df['flag'] == 1].display()

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

# COMMAND ----------

while True:
  val=1

# COMMAND ----------

loaded_dict[1601]

# COMMAND ----------

loaded_dict[1610]

# COMMAND ----------



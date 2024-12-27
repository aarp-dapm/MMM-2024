# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------



# COMMAND ----------

#importing libraries
import pandas as pd
import numpy as np
import re
import patsy
from pyspark.sql.functions import *
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from pyspark.sql.types import *
from pyspark.ml.feature import PCA
import plotly.graph_objects as go
from pyspark.sql import Window
from pyspark.sql.functions import sum as spark_sum
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import statsmodels.formula.api as smf
import itertools
import tqdm
import pickle
import plotly.graph_objects as go

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 100)

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading Data

# COMMAND ----------

## Reading Data ##
brand_equity = spark.table("temp.ja_blend_mmm2_BrandMetrics_YouGovVo2").toPandas() ## Loading Brand Equity 
brand_equity['Date'] = pd.to_datetime(brand_equity['Date'])

brand_media = spark.table("temp.mm_blend_mmm_MediaCom_BrandMedia_Pivoted").toPandas() ## Loading Brand Spend
brand_media['Date'] = pd.to_datetime(brand_media['Date'])

joins_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_initialmodel_results.csv').drop('Unnamed: 0',axis=1) ## Loading Jo9in Data

model_df = pd.merge(brand_equity[['Date','Purchase_Intent']], brand_media, on='Date')
model_df.display()

# COMMAND ----------

model_df[['Purchase_Intent','TV_Spend', 'Search_Spend', 'Social_Spend', 'Display_Spend', 'Audio_Spend', 'Video_Spend']].corr()

# COMMAND ----------

## Ploting Target Variable ##

# Create line chart
fig = px.line(model_df, x='Date', y='Purchase_Intent', title='Purchase_Intent')
fig.show()


# COMMAND ----------

from plotly.subplots import make_subplots
fig = go.Figure()
fig = make_subplots(specs=[[{"secondary_y": True}]])

fig.add_trace(go.Scatter(x=model_df['Date'], y=model_df['Audio_Imps'],
                            mode='lines',name='Audio_Imps',line = dict(color='green', width=2)))

fig.add_trace(go.Scatter(x=model_df['Date'], y=model_df['Display_Imps'],
                            mode='lines',name='Display_Imps',line = dict(color='blue', width=2)))       

fig.add_trace(go.Scatter(x=model_df['Date'], y=model_df['Search_Imps'],
                            mode='lines',name='Search_Imps',line = dict(width=2)))  

fig.add_trace(go.Scatter(x=model_df['Date'], y=model_df['Social_Imps'],
                            mode='lines',name='Social_Imps',line = dict(width=2)))  

fig.add_trace(go.Scatter(x=model_df['Date'], y=model_df['Video_Imps'],
                            mode='lines',name='Video_Imps',line = dict(width=2)))  

# fig.add_trace(go.Scatter(x=new_df['Date'], y=new_df['TV_Imps'],
#                             mode='lines',name='TV_Imps',line = dict(width=2)))  

fig.add_trace(go.Scatter(x=model_df['Date'], y=model_df['Purchase_Intent'],
                            mode='lines',name='Purchase_Intent',line = dict(color='red', width=2)),secondary_y=True)

fig.update_layout(title='Brand Media VS Purchase_Intent',
                        xaxis_title='Date',
                        yaxis_title='Purchase_Intent')

fig.update_yaxes(title_text="Purchase_Intent", secondary_y=True)                        

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


target_var = ['Date'] + ['Purchase_Intent']
channel_cols = ['Audio_Spend', 'Display_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps','Display_Spend','Video_Spend','Social_Spend']

df = model_df[target_var + channel_cols]


#############################
## Preprocessing Variables ##
#############################

# for ch in channel_cols:
#   if ch not in df:
#     df[ch] = (df[ch] - df[ch].min() )/(df[ch].max() - df[ch].min())


#############################
## Variable Transformation ##
#############################

# ## Creating Lags ##
# lag_list = [1]
# for ch_cols in df.columns:
#   if ch_cols not in target_var:
#     ct = 0
#     while ct<len(lag_list):
#       df[ch_cols+f'_Lag{lag_list[ct]}'] = df[ch_cols].shift(lag_list[ct])
#       ct+=1 


## Creating Saturation ##
# sat_list =  [0.8]
# for ch_cols in df.columns:
#   if ch_cols not in target_var:
#     ct=0
#     while ct<len(sat_list):

#       df[ch_cols + f'_Sat{int(sat_list[ct]*100)}'] = df[ch_cols]**sat_list[ct]
#       ct+=1

## Adding Adstock ##
# adstock_list = [0.3,0.4,0.5,0.6,0.7,0.8,0.9]
# for ch_cols in df.columns:
#   if ch_cols not in target_var:
#     ct=0
#     while ct<len(adstock_list):
#       df[ch_cols + f'_AdStock{int(adstock_list[ct]*100)}'] = adstock_func_week_cut(df[ch_cols], adstock_list[ct], 6)

#       ct+=1

# COMMAND ----------

## Creating Holiday Var ##
df['Holiday_July4'] = df['Date'].apply(lambda x: 1 if x == '2022-07-11' else 0)

## Adding Intercept ##
df['Intercept'] = 1

# COMMAND ----------

def adstock_func(var_data, decay_factor):
    x = 0
    adstock_var = [x:= x * decay_factor + v for v in var_data]
    return adstock_var

def power_func(var_data, pow_value):
  pow_var = [x**pow_value for x in var_data]
  return pow_var
  
def lag_func(var_data,lag):
    lag_var = var_data.shift(lag).fillna(method='bfill')
    return lag_var

# COMMAND ----------

all_trans = ['lag_1','lag_2','lag_3','lag_4','lag_5','lag_6','lag_7','lag_8','pow_0.25','pow_0.5','pow_0.75','adstock_0.3','adstock_0.5','adstock_0.7','adstock_0.8']

combs = []

for i in range(1, len(all_trans)+1): 
    els = [list(x) for x in itertools.combinations(all_trans, i)]
    if (len(els[0])<=2):
        for e in els:
            if str(e).count("lag")<=1 and str(e).count("adstock") <= 1 and str(e).count("pow") <= 1 :
                combs.append(e)

combs

# COMMAND ----------

all_vars =  ['Display_Imps','Search_Clicks','Social_Imps','TV_Imps','Video_Imps','Video_Spend','Display_Spend','Social_Spend']

for i in range(len(combs)):
  for var in all_vars:
      for c in combs[i]:
          temp = re.findall("\d+\.\d+",c)
          res = list(map(float, temp))
          
          if 'lag' in c:
            temp = re.findall(r'\d+', c)
            res = list(map(int, temp))
            df["{}_{}".format(var,c)] = lag_func(df[var],res[0])
            var = "{}_{}".format(var,c)
          
          elif 'pow' in c:
            df['{}_{}'.format(var,c)] = power_func(df[var].tolist(),res[0])
            var = "{}_{}".format(var,c)
 
          elif 'adstock' in c:
            df['{}_{}'.format(var,c)] = adstock_func(df[var],res[0])
            var = "{}_{}".format(var,c)


# COMMAND ----------

df.columns = df.columns.str.replace(' ', '_').str.replace('%', 'perc').str.replace('/','_').str.replace('.','_').str.replace('&','and')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3.0 Model Development

# COMMAND ----------

## Target Var ##
target_var = 'Purchase_Intent'

## Media Var #
video_cols = [col for col in df.columns if 'Video_Imps_' in col]
tv_cols = [col for col in df.columns if 'TV_Imps_' in col]
social_cols =[col for col in df.columns if 'Social_Spend_' in col]
display_cols = [col for col in df.columns if 'Display_Imps_' in col]
search_cols = [col for col in df.columns if 'Search_Clicks_' in col]

## Other Vars  ##
holiday_cols = ['Holiday_July4']

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Iteration 1

# COMMAND ----------

## Creating Combinations ##
combinations = list(itertools.product(display_cols,tv_cols))
combinations = [list(pair) for pair in combinations]
len(combinations)

# COMMAND ----------

# combinations = [['TV_Imps_adstock_0_7','Display_Imps_lag_3_adstock_0_3'],
#                 ['TV_Imps_adstock_0_8','Display_Imps_lag_3_adstock_0_3'],
#                 ['TV_Imps_lag_1_adstock_0_8','Display_Imps_lag_3_adstock_0_3'],
# ]

# COMMAND ----------

## Initilizng Dict to Log Results ##
exp_dict = {}

# COMMAND ----------


## Running Models ##
for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

  input_vars_string = '+'.join(channel_cols)
  model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols + ['Intercept']].dropna() ).fit()
  

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

# ## Creating Combinations ##
# combinations = list(itertools.product(video_cols, tv_cols, social_cols, display_cols))
# combinations = [list(pair) for pair in combinations]


# ## Running Models ##
# for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

#   input_vars_string = '+'.join(channel_cols+ holiday_cols)
#   model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols+ holiday_cols +['Intercept']].dropna() ).fit()
  

#   ## Getting Summary ##
#   summary_df = pd.DataFrame(model.summary().tables[1].data[1:], columns=model.summary().tables[1].data[0]).set_index('')
#   summary_df = summary_df.join(( (model.params)*model.model.exog.sum(axis=0) ).rename('Contri'))
#   summary_df['Target_Var'] = model.model.endog.sum()
  

#   ## Logging Results ##
#   exp_dict[idx] = summary_df

#   # if idx == 10:
#   #   break

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Iteration 3

# COMMAND ----------

# ## Creating Combinations ##
# combinations = list(itertools.product(video_cols, tv_cols, social_cols))
# combinations = [list(pair) for pair in combinations]
# print(len(combinations))


# ## Running Models ##
# for idx, channel_cols in tqdm.tqdm(enumerate(combinations)):

#   input_vars_string = '+'.join(channel_cols+ holiday_cols)
#   model = smf.ols( f'{target_var} ~ {input_vars_string}', data = df[ [target_var] + channel_cols+ holiday_cols +['Intercept']].dropna() ).fit()
  

#   ## Getting Summary ##
#   summary_df = pd.DataFrame(model.summary().tables[1].data[1:], columns=model.summary().tables[1].data[0]).set_index('')
#   summary_df = summary_df.join(( (model.params)*model.model.exog.sum(axis=0) ).rename('Contri'))
#   summary_df['Target_Var'] = model.model.endog.sum()
  

#   ## Logging Results ##
#   exp_dict[idx] = summary_df

#   # if idx == 10:
#   #   break

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.91 Saving Experiment Data

# COMMAND ----------

## Saving Results ##
# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/brand_model/mm_brandmedia_equity.json'

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

file_path = '/dbfs/blend360/sandbox/mmm/brand_model/mm_brandmedia_equity.json'

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
analysis_df.drop_duplicates(inplace=True)

## creating new column name ##
analysis_df['new_name'] = np.where( analysis_df.index.str.contains('Lag|Sat|AdStock|lag|adstock|pow'), analysis_df.index.to_series().apply(lambda x: "_".join(x.split("_")[:2])),analysis_df.index)
analysis_df['old_name'] = analysis_df.index

## Coeff Table #
pivot_coeff_df_2 = analysis_df.pivot(index = 'new_name', columns = 'ModelNumber', values = 'old_name').T.reset_index()
pivot_coeff_df_2['model_cols'] =  pivot_coeff_df_2.apply(lambda row: row.tolist()[1:], axis=1)

pivot_coeff_df = analysis_df.pivot(index = 'new_name', columns = 'ModelNumber', values = 'coef').T.reset_index()
pivot_coeff_df = pivot_coeff_df.astype('float')
pivot_coeff_df['ModelColumns'] = pivot_coeff_df_2['model_cols']
pivot_coeff_df['ModelNumber'] = pivot_coeff_df['ModelNumber'].astype('int')

# Specify the desired order of columns
desired_order = ['ModelNumber', 'ModelColumns'] + analysis_df[analysis_df['ModelNumber']==0]['new_name'].tolist()

# Reorder the pivot DataFrame columns
pivot_coeff_df = pivot_coeff_df[desired_order]

pivot_coeff_df.display()

# COMMAND ----------

# # Filtering for Models with Positive Coefficients ##

pivot_coeff_df['flag'] = pivot_coeff_df.apply( lambda x: 1 if ( (x['Display_Imps'] > 0) and   (x['Intercept'] > 0) and  (x['TV_Imps'] > 0)) else 0, axis=1)

# # pivot_coeff_df['flag'] = pivot_coeff_df.apply( lambda x: 1 if ( (x['Display_Spend'] > 0) and   (x['Intercept'] > 0) and (x['Search_Clicks'] > 0) and (x['Social_Spend'] > 0) and (x['TV_Imps'] > 0) and (x['Video_Imps'] > 0)) else 0, axis=1)
# # pivot_coeff_df['flag'] = pivot_coeff_df.apply( lambda x: 1 if ( (x['Display_Imps'] > 0) and   (x['Intercept'] > 0) and (x['Search_Clicks'] > 0) and (x['Social_Imps'] > 0) and (x['TV_Imps'] > 0) and (x['Video_Imps'] > 0)) else 0, axis=1)
# # pivot_coeff_df['flag'] = pivot_coeff_df.apply( lambda x: 1 if ( (x['Display_Imps'] > 0) and   (x['Intercept'] > 0) and (x['Social_Imps'] > 0) and (x['TV_Imps'] > 0) and (x['Video_Imps'] > 0)) else 0, axis=1)

pivot_coeff_df[pivot_coeff_df['flag'] == 1].display()

# COMMAND ----------

pivot_coeff_df.columns  = [i+"_coeff" if i not in ['ModelNumber','ModelColumns'] else i  for i in pivot_coeff_df.columns]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Pvals. Analysis

# COMMAND ----------

## PValue Table ##
pivot_pval_df = analysis_df.pivot(index = 'new_name', columns = 'ModelNumber', values = 'P>|t|').T.reset_index()
pivot_pval_df  = pivot_pval_df.astype('float')
pivot_pval_df['ModelNumber'] = pivot_pval_df['ModelNumber'].astype('int')

desired_order = ['ModelNumber'] + analysis_df[analysis_df['ModelNumber']==0]['new_name'].tolist()
# Reorder the pivot DataFrame columns
pivot_pval_df = pivot_pval_df[desired_order]

# COMMAND ----------

# # Filtering for Models for significant Coefficients ##

pivot_pval_df['flag'] = pivot_pval_df.apply( lambda x: 1 if ( (x['Display_Imps'] <= 0.30 ) and   (x['Intercept'] <= 0.30 ) and (x['TV_Imps'] <= 0.30 )) else 0, axis=1)

# # pivot_pval_df['flag'] = pivot_pval_df.apply( lambda x: 1 if ( (x['Display_Imps'] < 0.20 ) and   (x['Intercept'] < 0.20 ) and (x['Search_Clicks'] < 0.20 ) and (x['Social_Imps'] < 0.20 ) and (x['TV_Imps'] < 0.20 ) and (x['Video_Imps'] < 0.20 )) else 0, axis=1)

pivot_pval_df[pivot_pval_df['flag'] == 1].display()

# COMMAND ----------

pivot_pval_df.columns  = [i+"_pval" if i!='ModelNumber' else i  for i in pivot_pval_df.columns]

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

for col in [i for i in pivot_contri_df.columns if i not in ['ModelNumber','TotalContri','Target_Var']]:
  pivot_contri_df[col] = pivot_contri_df[col]/pivot_contri_df['TotalContri']

desired_order = ['ModelNumber'] + analysis_df[analysis_df['ModelNumber']==0]['new_name'].tolist()
# Reorder the pivot DataFrame columns
pivot_contri_df = pivot_contri_df[desired_order]

# COMMAND ----------

pivot_contri_df.columns  = [i+"_contri" if i!='ModelNumber' else i  for i in pivot_contri_df.columns]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.9 Joining all Analysis

# COMMAND ----------

## Joining Dataframe ##
pivot_coeff_pval_df = pivot_coeff_df.merge(pivot_pval_df, on='ModelNumber', how='inner').merge(pivot_contri_df, on='ModelNumber', how='inner')

# COMMAND ----------

pivot_coeff_pval_df.display()

# COMMAND ----------

pivot_coeff_pval_df[(pivot_coeff_pval_df['flag_coeff'] == 1) & (pivot_coeff_pval_df['flag_pval']==1)].display()

# COMMAND ----------

pivot_coeff_pval_df.saveAsTable("temp.brand_media_iterations")

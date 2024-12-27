# Databricks notebook source
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

## Reading Data ##
brand_equity = spark.table("temp.ja_blend_mmm2_BrandMetrics_YouGovVo2").toPandas() ## Loading Brand Equity 
brand_equity['Date'] = pd.to_datetime(brand_equity['Date'])

brand_media = spark.table("temp.mm_blend_mmm_MediaCom_BrandMedia_Pivoted").toPandas() ## Loading Brand Spend
brand_media['Date'] = pd.to_datetime(brand_media['Date'])

joins_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_initialmodel_results.csv').drop('Unnamed: 0',axis=1)
joins_df["base_joins"] = joins_df[['Seasonality_Feb','Seasonality_Mar', 'Seasonality_Apr', 'Seasonality_Dec','Fixed Dma Effect','July0422_lag']].sum(axis=1)
joins_df['Date'] = pd.to_datetime(joins_df['Date'])

model_df = pd.merge(brand_equity[['Date','Purchase_Intent']], joins_df[['Date','base_joins']], on='Date')
model_df.display()

# COMMAND ----------

# from plotly.subplots import make_subplots
# fig = go.Figure()
# fig = make_subplots(specs=[[{"secondary_y": True}]])

# fig.add_trace(go.Scatter(x=model_df['Date'], y=model_df['Purchase_Intent'],
#                             mode='lines',name='Purchase_Intent',line = dict(color='green', width=2)))
 

# fig.add_trace(go.Scatter(x=model_df['Date'], y=model_df['base_joins'],
#                             mode='lines',name='base_joins',line = dict(color='red', width=2)),secondary_y=True)

# fig.update_layout(title='Purchase_Intent VS base_joins',
#                         xaxis_title='Date',
#                         yaxis_title='base_joins')

# fig.update_yaxes(title_text="base_joins", secondary_y=True)                        

# COMMAND ----------



# COMMAND ----------

# Calculate Week-on-Week difference
model_df['WoW_difference_Purchase_Intent'] = model_df['Purchase_Intent'].diff(periods=1)

# COMMAND ----------

target_var = ['Date'] + ['base_joins']
channel_cols = ['Purchase_Intent','WoW_difference_Purchase_Intent']

df = model_df[target_var + channel_cols]

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

all_trans = ['lag_1','lag_2','lag_3','lag_4','lag_5','lag_6','lag_7','lag_8','adstock_0.3','adstock_0.5','adstock_0.7','adstock_0.8']

combs = []

for i in range(1, len(all_trans)+1): 
    els = [list(x) for x in itertools.combinations(all_trans, i)]
    if (len(els[0])<=2):
        for e in els:
            if str(e).count("lag")<=1 and str(e).count("adstock") <= 1 and str(e).count("pow") <= 1 :
                combs.append(e)

combs

# COMMAND ----------

all_vars =  ['Purchase_Intent']

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
 
          elif 'adstock' in c:
            df['{}_{}'.format(var,c)] = adstock_func(df[var],res[0])
            var = "{}_{}".format(var,c)


# COMMAND ----------

df.columns = df.columns.str.replace(' ', '_').str.replace('%', 'perc').str.replace('/','_').str.replace('.','_').str.replace('&','and')

# COMMAND ----------

target_var = 'base_joins'

## Creating Combinations ##
combinations = list(itertools.product([col for col in df.columns if 'Purchase_Intent' in col]))
combinations = [list(pair) for pair in combinations]
len(combinations)

# COMMAND ----------

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

## Saving Results ##
# Define the file path in DBFS
file_path = '/dbfs/blend360/sandbox/mmm/brand_model/mm_brandmedia_equity.json'

# Save the dictionary as a JSON file
with open(file_path, 'wb') as f:
    pickle.dump(exp_dict, f)


# Read the dictionary from the JSON file
with open(file_path, 'rb') as f:
    loaded_dict = pickle.load(f)

print(loaded_dict[1])

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
analysis_df['new_name'] = ['Intercept','Purchase_Intent']*(len(analysis_df.index)//2)
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

pivot_coeff_df.columns  = [i+"_coeff" if i not in ['ModelNumber','ModelColumns'] else i  for i in pivot_coeff_df.columns]

# COMMAND ----------

## PValue Table ##
pivot_pval_df = analysis_df.pivot(index = 'new_name', columns = 'ModelNumber', values = 'P>|t|').T.reset_index()
pivot_pval_df  = pivot_pval_df.astype('float')
pivot_pval_df['ModelNumber'] = pivot_pval_df['ModelNumber'].astype('int')

desired_order = ['ModelNumber'] + analysis_df[analysis_df['ModelNumber']==0]['new_name'].tolist()
# Reorder the pivot DataFrame columns
pivot_pval_df = pivot_pval_df[desired_order]

# COMMAND ----------


pivot_pval_df['flag'] = pivot_pval_df.apply( lambda x: 1 if ( (x['Purchase_Intent'] <= 0.20 ) and   (x['Intercept'] <= 0.20 )) else 0, axis=1)

# COMMAND ----------

pivot_pval_df.columns  = [i+"_pval" if i!='ModelNumber' else i  for i in pivot_pval_df.columns]

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

## Joining Dataframe ##
pivot_coeff_pval_df = pivot_coeff_df.merge(pivot_pval_df, on='ModelNumber', how='inner').merge(pivot_contri_df, on='ModelNumber', how='inner')

# COMMAND ----------

pivot_coeff_pval_df.display()

# COMMAND ----------

df.corr()

# COMMAND ----------

from plotly.subplots import make_subplots
fig = go.Figure()
fig = make_subplots(specs=[[{"secondary_y": True}]])

fig.add_trace(go.Scatter(x=df['Date'], y=df['Purchase_Intent_lag_2_adstock_0_5'],
                            mode='lines',name='Purchase_Intent_lag_2_adstock_0_5',line = dict(color='green', width=2)))
 

fig.add_trace(go.Scatter(x=df['Date'], y=df['base_joins'],
                            mode='lines',name='base_joins',line = dict(color='red', width=2)),secondary_y=True)

fig.update_layout(title='Purchase_Intent VS base_joins',
                        xaxis_title='Date',
                        yaxis_title='base_joins')

fig.update_yaxes(title_text="base_joins", secondary_y=True)                        

# COMMAND ----------



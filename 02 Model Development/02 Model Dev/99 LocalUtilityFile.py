# Databricks notebook source
import pyspark.sql.functions as f

# COMMAND ----------

import sys
import os
import json
import warnings
import tqdm

sys.path.append(os.path.abspath('/Workspace/Repos/arajput@aarp.org/AME/AME_src'))

# COMMAND ----------

import pandas as pd
from pandas.testing import assert_frame_equal
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns 
import datetime as dtta
import itertools

# COMMAND ----------

import statsmodels.formula.api as smf
import statsmodels.api as sm
from scipy.stats import skew, kurtosis, jarque_bera
from scipy.optimize import curve_fit
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error, r2_score
from statsmodels.stats.outliers_influence import variance_inflation_factor
from statsmodels.base._penalties import L2, Penalty

from scipy.optimize import minimize, LinearConstraint, NonlinearConstraint
from scipy.linalg import svd
from scipy.stats import gamma
from scipy.stats import weibull_min

# from AME import Model
# from AME import Transform

# COMMAND ----------

import os
import warnings

import arviz as az
import pymc as pm
import pytensor.tensor as pt
import xarray as xr

warnings.filterwarnings("ignore", module="scipy")

print(f"Running on PyMC v{pm.__version__}")

# COMMAND ----------

# import mlflow
# import mlflow.spark
from functools import reduce
import pickle

# COMMAND ----------

## Reading File : Excel ##
def read_excel(excel_file_path):
  # Create a SparkSession
  spark = SparkSession.builder.appName("ReadExcelWithHeader").config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5").getOrCreate()
  return spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load(excel_file_path)

## Reading File : CSV ##
def read_csv(csv_file_path, skipRows = None):
  return spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("skipRows",skipRows).load(csv_file_path)


## Read from Table ##
def read_table(table_name):
  return spark.sql(f"select * from {table_name}")

# COMMAND ----------

warnings.simplefilter('ignore')

# COMMAND ----------

#Initializing all Adhoc functions

#create a unit mean normalization funtion to can be used later
def unit_mean_normalization(df, var, crosssection = None):

    df[var].replace(0, np.nan, inplace=True) #change 0 to N/A not to consider them in the mean    
    
    if crosssection is None: 
        df[var + '_means'] = df[var].mean() #if there are no cross section, only creating a mean variable is needed
    else: 
        variable_mean = df[[crosssection, var]].groupby(crosssection).mean()[var] #make a table that is only the mean of the variable being normalized grouped by crosssection
        variable_mean = variable_mean.rename(var + '_means' ) #rename series to have _means at the end for left join to be performed without duplication of variable name
        df = df.merge(variable_mean, on=crosssection, how='left') #left join dep means for future calc
    
    df.insert(df.columns.get_loc(var)+1, var + "_umnorm", 0, True) # create a new column for normalized variable adn set to 0
    df[var + "_umnorm"] = df[var] / df[var + '_means'] # create the normalized variable by dividing by the left joined _means
    df = df.drop(columns = var + '_means') #drop the column of means
    df[var + "_umnorm"].fillna(value = 0, inplace = True) #fill na for new column in case there were divisions by 0 and other 0s prior to start
    df[var].fillna(value = 0, inplace = True) #fill na for original variable that 
    
    return df #return the df, otherwise look to make global df if possible





# COMMAND ----------

### Normalize Data and Scale Up Data ###


## Normalize ##

## Min Max Normalization ##
def min_max_normalize(val):
  return (val-(val.min()))/(val.max()-val.min())

## Unit Mean Normalization ##
def unit_mean_normalize(val):
    return val/val.mean()
  



## Scale up ##

## Scale up Min-Max ##
def upscale_MinMax(df, MinMaxScale_df):

  return_df = df.merge(MinMaxScale_df, on='DmaCode')

  cols = [col for col in return_df.columns if col not in ['DmaCode', 'Date', 'Min', 'Max']]
  for col in cols:
    return_df[col] = return_df[col]*(return_df['Max']-return_df['Min'])

  return_df['Random Dma Effect2'] = return_df['Min']
  return_df.drop(columns=['Min', 'Max'], inplace=True)

  return return_df

## Scale up Unit Mean ##
def upscale_UnitMean(df, UnitMeanScale_df):

  return_df = df.merge(UnitMeanScale_df, on='DmaCode')

  cols = [col for col in return_df.columns if col not in ['DmaCode', 'Date', 'Mean']]
  for col in cols:
    return_df[col] = return_df[col]*(return_df['Max']-return_df['Min']) + return_df['Min']

  return_df.drop(columns=['Mean'], inplace=True)

  return return_df

# COMMAND ----------

## Functions to Fill Agg. Analysis Df ##
### Calculate Contri ###
def calc_contri(df,year,var_name):

  val = df[pd.to_datetime(df['Date']).dt.year==year][var_name].sum()

  return val

### Calculate Spend ###
def calc_spend(df,year,var_name):

  try:
    val = df[pd.to_datetime(df['Date']).dt.year==year][var_name].sum()
  except:
    val=0
    

  return val

### Calculate Support ###
def calc_support(df,year,var_name):

  try:
    val = df[pd.to_datetime(df['Date']).dt.year==year][var_name].sum()
  except:
    val=0

  # val=0
  # if var_name.split("_")[0] == 'Search':
  #   val = df[pd.to_datetime(df['Date']).dt.year==year][var_name].sum()
  

  return val

# COMMAND ----------

# DMA Population Proportion
norm_ch_dma_code_prop = {
    '551': 0.002662927,
    '552': 0.000245082,
    '553': 0.000881159,
    '554': 0.001277038,
    '555': 0.003626019,
    '556': 0.004664178,
    '557': 0.004449887,
    '558': 0.000815235,
    '559': 0.000977296,
    '560': 0.012066671,
    '561': 0.007668207,
    '563': 0.008162467,
    '564': 0.003872267,
    '565': 0.000884812,
    '566': 0.006343069,
    '567': 0.008192084,
    '569': 0.000759085,
    '570': 0.002812351,
    '571': 0.005472185,
    '573': 0.003726824,
    '574': 0.002459281,
    '575': 0.003046102,
    '576': 0.001633937,
    '577': 0.004767692,
    '581': 0.001144373,
    '582': 0.00044206,
    '583': 0.000180585,
    '627': 0.00130698,
    '628': 0.001888758,
    '630': 0.007508268,
    '631': 0.000400398,
    '632': 0.003162121,
    '633': 0.001149894,
    '634': 0.001460919,
    '635': 0.005755824,
    '636': 0.002820532,
    '637': 0.00287493,
    '638': 0.00038243,
    '639': 0.000799722,
    '640': 0.005188457,
    '641': 0.007466931,
    '642': 0.002528442,
    '643': 0.001056828,
    '644': 0.000919494,
    '647': 0.000511005,
    '648': 0.002900912,
    '649': 0.002407194,
    '650': 0.006487021,
    '651': 0.00119696,
    '652': 0.003400662,
    '656': 0.001714765,
    '657': 0.001163773,
    '658': 0.004189391,
    '500': 0.003414283,
    '501': 0.063054526,
    '502': 0.001218076,
    '503': 0.002016967,
    '504': 0.02486704,
    '505': 0.019390548,
    '506': 0.022648242,
    '507': 0.002801624,
    '508': 0.009520892,
    '509': 0.002293585,
    '510': 0.015909154,
    '511': 0.01906718,
    '512': 0.008764742,
    '513': 0.004933315,
    '514': 0.006135504,
    '515': 0.009088006,
    '516': 0.001291829,
    '517': 0.012334914,
    '518': 0.007140254,
    '519': 0.002732156,
    '520': 0.002160112,
    '521': 0.00610367,
    '522': 0.001855421,
    '523': 0.002784166,
    '524': 0.019767104,
    '724': 0.001930275,
    '725': 0.002053261,
    '734': 0.000830851,
    '736': 0.000659436,
    '737': 0.000431473,
    '740': 0.000122652,
    '743': 0.001298741,
    '744': 0.002919418,
    '745': 0.000284036,
    '746': 0.00106485,
    '747': 0.000225863,
    '749': 0.000590834,
    '751': 0.015710984,
    '752': 0.003608796,
    '753': 0.013555644,
    '754': 0.000530328,
    '755': 0.000472557,
    '756': 0.000860581,
    '757': 0.00227345,
    '758': 0.001018931,
    '759': 0.000426178,
    '760': 0.000504264,
    '762': 0.00090506,
    '764': 0.000735956,
    '765': 0.002614001,
    '766': 0.000226279,
    '767': 0.000409459,
    '770': 0.009048714,
    '771': 0.000731022,
    '773': 0.000765794,
    '789': 0.003107935,
    '790': 0.004973628,
    '798': 3.40E-05,
    '800': 0.001629422,
    '801': 0.001914063,
    '802': 0.00041471,
    '803': 0.040400979,
    '804': 0.001080467,
    '807': 0.017565714,
    '810': 0.001889729,
    '811': 0.002661843,
    '813': 0.001453452,
    '819': 0.013978558,
    '820': 0.009545267,
    '821': 0.000604297,
    '825': 0.007513424,
    '828': 0.001646005,
    '839': 0.006847266,
    '855': 0.001746697,
    '862': 0.010748074,
    '866': 0.004231829,
    '868': 0.001368502,
    '881': 0.003418825,
    '659': 0.008519334,
    '661': 0.000435658,
    '662': 0.000932767,
    '669': 0.003426509,
    '670': 0.003087876,
    '671': 0.004938449,
    '673': 0.001480788,
    '675': 0.001966168,
    '676': 0.001475836,
    '678': 0.003450401,
    '679': 0.003616213,
    '682': 0.002504116,
    '686': 0.00605739,
    '687': 0.001206771,
    '691': 0.004260177,
    '692': 0.001354954,
    '693': 0.005752031,
    '698': 0.002492645,
    '702': 0.001931196,
    '705': 0.001712837,
    '709': 0.002271874,
    '710': 0.000893164,
    '711': 0.00057008,
    '716': 0.00348888,
    '717': 0.000884631,
    '718': 0.002720413,
    '722': 0.002274754,
    '525': 0.001234902,
    '526': 0.000952402,
    '527': 0.008725563,
    '528': 0.014747512,
    '529': 0.005525631,
    '530': 0.002708101,
    '531': 0.002669979,
    '532': 0.005126989,
    '533': 0.00954869,
    '534': 0.016532377,
    '535': 0.009724475,
    '536': 0.002682652,
    '537': 0.001172215,
    '538': 0.003806898,
    '539': 0.019324854,
    '540': 0.002789999,
    '541': 0.003965704,
    '542': 0.005011,
    '543': 0.002436337,
    '544': 0.005720365,
    '545': 0.003100567,
    '546': 0.003415213,
    '547': 0.004411385,
    '548': 0.008154408,
    '549': 0.0008551,
    '550': 0.00214687,
    '584': 0.000611905,
    '588': 0.002931025,
    '592': 0.001236613,
    '596': 0.000357301,
    '597': 0.000585805,
    '598': 0.000809266,
    '600': 0.00161475,
    '602': 0.026466779,
    '603': 0.001270582,
    '604': 0.001504441,
    '605': 0.001297106,
    '606': 0.00103806,
    '609': 0.01057958,
    '610': 0.001456142,
    '611': 0.001197696,
    '612': 0.003592055,
    '613': 0.014246309,
    '616': 0.007704664,
    '617': 0.00782723,
    '618': 0.017567588,
    '619': 0.003790469,
    '622': 0.006647511,
    '623': 0.02045022,
    '624': 0.001342538,
    '625': 0.002655076,
    '626': 0.000270035
}


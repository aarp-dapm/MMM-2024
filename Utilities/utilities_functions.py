# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DoubleType, FloatType, DateType, TimestampType
from pyspark.sql.types import *
from pyspark.sql.functions import date_format, col, desc, udf, from_unixtime, unix_timestamp, date_sub, date_add, last_day, format_number
from pyspark.sql.functions import round, sum, lit, add_months, coalesce, max, min, monotonically_increasing_id, approx_count_distinct
from pyspark.sql.window import Window
from functools import reduce
from pyspark.sql import SparkSession

from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import time

# COMMAND ----------

import statsmodels.formula.api as smf
from scipy.stats import skew, kurtosis
from scipy.linalg import svd

# COMMAND ----------

## Functions to create Dataframe

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

## Saving File ##
def save_df_func(save_df,table_name, display=True):

  ## Repalcing special character with '_' ##
  save_df = save_df.select([f.col(col).alias(col.replace(' ', '_')) for col in save_df.columns])

  spark.sql(f'DROP TABLE IF EXISTS {table_name}')
  save_df.write.mode("overwrite").saveAsTable(table_name)

  model_df = spark.sql(f"select * from {table_name}")
  if display:
    model_df.display()


## Expanding Dataframe ##
def expand(start_date, end_date):

  ## Creating Week DF ##
  df_week = spark.sparkContext.parallelize([Row(START_DATE = start_date, END_DATE = end_date)]).toDF()
  df_week = df_week.withColumn('start_date', f.col('START_DATE').cast('date')).withColumn('end_date', f.col('END_DATE').cast('date'))
  df_week  = df_week.withColumn("Date", f.explode(f.expr("sequence(start_date, end_date, interval 7 day)"))).select('Date')

  ## Creating DMA DF ##
  df_dma = spark.createDataFrame(dma_code, StringType()).toDF('DMA_Code')

  ## Week-DMA DF ##
  df = df_week.crossJoin(df_dma)


  return df


## QC DataFrame ##
def unit_test(before, after, met_col_name, date_col_name):

  before_temp = before.groupBy(date_col_name[0]).agg(f.sum(f.col(met_col_name[0])).alias('before_spend'))
  after_temp = after.groupBy(date_col_name[1]).agg(f.sum(f.col(met_col_name[1])).alias('after_spend'))
  
  temp = before_temp.join(after_temp, before_temp[date_col_name[0]] == after_temp[date_col_name[1]], 'left')
  temp = temp.withColumn('Diff', f.col('before_spend')-f.col('after_spend') )
  return temp

# COMMAND ----------

## Preprocessing Dataframe ##

def rename_cols_func(df, dict_):
  for key, val in dict_.items():
    df = df.withColumnRenamed(key, val)
  return df

# COMMAND ----------

## Functions to Stitch Dataframe
def load_saved_table(table_name):
  return spark.sql(f"select * from {table_name}")


def stitch_df(df_name_list):

  df = df_name_list[0]
  for new_df in df_name_list[1:]:
    df = df.union(new_df)

  return df


## Function to create DF from Table name list ##
def df_from_name_list(table_name):

  ## Saving Dataframes to Stitch ##
  df_list = []
  for name in table_name:
    df_list.append(load_saved_table(name))


  ## Stitching Dataframe ##
  return stitch_df(df_list)


# COMMAND ----------

## Function for Creating Promo Columns ##
def WeekOf(val):
  
  dt = f.to_date(val)
  week = f.date_sub(dt,f.dayofweek(dt)-2)
  return week

# COMMAND ----------

## Making Data Reporting ready ##
def report_df(df):
  return df.groupBy(*['Date', 'SubChannel']).agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks'))

# COMMAND ----------


dma_code = ['500',
 '501',
 '502',
 '503',
 '504',
 '505',
 '506',
 '507',
 '508',
 '509',
 '510',
 '511',
 '512',
 '513',
 '514',
 '515',
 '516',
 '517',
 '518',
 '519',
 '520',
 '521',
 '522',
 '523',
 '524',
 '525',
 '526',
 '527',
 '528',
 '529',
 '530',
 '531',
 '532',
 '533',
 '534',
 '535',
 '536',
 '537',
 '538',
 '539',
 '540',
 '541',
 '542',
 '543',
 '544',
 '545',
 '546',
 '547',
 '548',
 '549',
 '550',
 '551',
 '552',
 '553',
 '554',
 '555',
 '556',
 '557',
 '558',
 '559',
 '560',
 '561',
 '563',
 '564',
 '565',
 '566',
 '567',
 '569',
 '570',
 '571',
 '573',
 '574',
 '575',
 '576',
 '577',
 '581',
 '582',
 '583',
 '584',
 '588',
 '592',
 '596',
 '597',
 '598',
 '600',
 '602',
 '603',
 '604',
 '605',
 '606',
 '609',
 '610',
 '611',
 '612',
 '613',
 '616',
 '617',
 '618',
 '619',
 '622',
 '623',
 '624',
 '625',
 '626',
 '627',
 '628',
 '630',
 '631',
 '632',
 '633',
 '634',
 '635',
 '636',
 '637',
 '638',
 '639',
 '640',
 '641',
 '642',
 '643',
 '644',
 '647',
 '648',
 '649',
 '650',
 '651',
 '652',
 '656',
 '657',
 '658',
 '659',
 '661',
 '662',
 '669',
 '670',
 '671',
 '673',
 '675',
 '676',
 '678',
 '679',
 '682',
 '686',
 '687',
 '691',
 '692',
 '693',
 '698',
 '702',
 '705',
 '709',
 '710',
 '711',
 '716',
 '717',
 '718',
 '722',
 '724',
 '725',
 '734',
 '736',
 '737',
 '740',
 '743',
 '744',
 '745',
 '746',
 '747',
 '749',
 '751',
 '752',
 '753',
 '754',
 '755',
 '756',
 '757',
 '758',
 '759',
 '760',
 '762',
 '764',
 '765',
 '766',
 '767',
 '770',
 '771',
 '773',
 '789',
 '790',
 '798',
 '800',
 '801',
 '802',
 '803',
 '804',
 '807',
 '810',
 '811',
 '813',
 '819',
 '820',
 '821',
 '825',
 '828',
 '839',
 '855',
 '862',
 '866',
 '868',
 '881']

# COMMAND ----------

us_states = [
    "California",
    "Texas",
    "Florida",
    "New York",
    "Pennsylvania",
    "Ohio",
    "Illinois",
    "Georgia",
    "North Carolina",
    "Michigan",
    "New Jersey",
    "Virginia",
    "Arizona",
    "Indiana",
    "Maryland",
    "Tennessee",
    "Washington",
    "Massachusetts",
    "Missouri",
    "Wisconsin",
    "South Carolina",
    "Minnesota",
    "Alabama",
    "Colorado",
    "Kentucky",
    "Louisiana",
    "Connecticut",
    "Oregon",
    "Oklahoma",
    "Nevada",
    "Iowa",
    "Arkansas",
    "Mississippi",
    "Kansas",
    "Utah",
    "West Virginia",
    "New Hampshire",
    "Nebraska",
    "Maine",
    "New Mexico",
    "Idaho",
    "Hawaii",
    "Delaware",
    "Rhode Island",
    "Montana",
    "Alaska",
    "South Dakota",
    "District of Columbia",
    "Vermont",
    "North Dakota",
    "Wyoming"
]


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


# COMMAND ----------

## Brand TV each quarter proportion by DMA ##
Q1_23 = {
    '500': 0.01422,
    '501': 0.05581,
    '502': 0.00142,
    '503': 0.00218,
    '504': 0.02442,
    '505': 0.01523,
    '506': 0.02129,
    '507': 0.00329,
    '508': 0.01149,
    '509': 0.00227,
    '510': 0.01354,
    '511': 0.01897,
    '512': 0.00895,
    '513': 0.00471,
    '514': 0.0066,
    '515': 0.00754,
    '516': 0.00161,
    '517': 0.01103,
    '518': 0.0071,
    '519': 0.00313,
    '520': 0.00252,
    '521': 0.006,
    '522': 0.00206,
    '523': 0.00371,
    '524': 0.02004,
    '525': 0.00138,
    '526': 0.00117,
    '527': 0.00952,
    '528': 0.00956,
    '529': 0.00618,
    '530': 0.00256,
    '531': 0.00315,
    '532': 0.00631,
    '533': 0.00869,
    '534': 0.01348,
    '535': 0.00861,
    '536': 0.00269,
    '537': 0.00176,
    '538': 0.00418,
    '539': 0.01614,
    '540': 0.00264,
    '541': 0.00504,
    '542': 0.00501,
    '543': 0.0027,
    '544': 0.00683,
    '545': 0.00325,
    '546': 0.00388,
    '547': 0.00429,
    '548': 0.00755,
    '549': 0.00105,
    '550': 0.00223,
    '551': 0.00242,
    '552': 0.00035,
    '553': 0.00102,
    '554': 0.00152,
    '555': 0.00407,
    '556': 0.00585,
    '557': 0.00519,
    '558': 0.00074,
    '559': 0.00142,
    '560': 0.01083,
    '561': 0.00583,
    '563': 0.0067,
    '564': 0.00491,
    '565': 0.00118,
    '566': 0.00734,
    '567': 0.0084,
    '569': 0.00102,
    '570': 0.00366,
    '571': 0.00539,
    '573': 0.00453,
    '574': 0.00316,
    '575': 0.00387,
    '576': 0.00171,
    '577': 0.00645,
    '581': 0.00131,
    '582': 0.00065,
    '583': 0.00018,
    '584': 0.00089,
    '588': 0.00285,
    '592': 0.00118,
    '596': 0.00039,
    '597': 0.00074,
    '598': 0.00113,
    '600': 0.00188,
    '602': 0.02653,
    '603': 0.00141,
    '604': 0.00166,
    '605': 0.00174,
    '606': 0.00093,
    '609': 0.0118,
    '610': 0.00175,
    '611': 0.00147,
    '612': 0.0037,
    '613': 0.01428,
    '616': 0.00782,
    '617': 0.00818,
    '618': 0.01868,
    '619': 0.00395,
    '622': 0.00625,
    '623': 0.01946,
    '624': 0.00144,
    '625': 0.0034,
    '626': 0.00032,
    '627': 0.00143,
    '628': 0.00174,
    '630': 0.0065,
    '631': 0.00043,
    '632': 0.0042,
    '633': 0.00138,
    '634': 0.0016,
    '635': 0.00597,
    '636': 0.00242,
    '637': 0.00325,
    '638': 0.00047,
    '639': 0.00108,
    '640': 0.00558,
    '641': 0.00748,
    '642': 0.00244,
    '643': 0.00108,
    '644': 0.0009,
    '647': 0.00057,
    '648': 0.00377,
    '649': 0.00286,
    '650': 0.00648,
    '651': 0.00141,
    '652': 0.00417,
    '656': 0.00141,
    '657': 0.00125,
    '658': 0.00434,
    '659': 0.0098,
    '661': 0.00053,
    '662': 0.00113,
    '669': 0.00371,
    '670': 0.00283,
    '671': 0.00509,
    '673': 0.00162,
    '675': 0.00247,
    '676': 0.00169,
    '678': 0.00405,
    '679': 0.00421,
    '682': 0.00293,
    '686': 0.00582,
    '687': 0.00155,
    '691': 0.00386,
    '692': 0.00168,
    '693': 0.00525,
    '698': 0.00243,
    '702': 0.00219,
    '705': 0.00214,
    '709': 0.00258,
    '710': 0.00101,
    '711': 0.00066,
    '716': 0.00339,
    '717': 0.00108,
    '718': 0.00342,
    '722': 0.00278,
    '724': 0.0023,
    '725': 0.00241,
    '734': 0.00087,
    '736': 0.00089,
    '737': 0.00047,
    '740': 0.00014,
    '743': 0.00095,
    '744': 0.00411,
    '745': 0.00023,
    '746': 0.00128,
    '747': 0.00023,
    '749': 0.00045,
    '751': 0.01204,
    '752': 0.00328,
    '753': 0.01457,
    '754': 0.0007,
    '755': 0.00064,
    '756': 0.00105,
    '757': 0.00232,
    '758': 0.00103,
    '759': 0.00055,
    '760': 0.00053,
    '762': 0.00123,
    '764': 0.00099,
    '765': 0.00206,
    '766': 0.00031,
    '767': 0.00056,
    '770': 0.00718,
    '771': 0.00083,
    '773': 0.0007,
    '789': 0.00379,
    '790': 0.00575,
    '798': 0.00004,
    '800': 0.00165,
    '801': 0.00256,
    '802': 0.0006,
    '803': 0.0346,
    '804': 0.00135,
    '807': 0.01507,
    '810': 0.0021,
    '811': 0.00284,
    '813': 0.00184,
    '819': 0.01606,
    '820': 0.00322,
    '821': 0.00064,
    '825': 0.00712,
    '828': 0.00142,
    '839': 0.00552,
    '855': 0.00182,
    '862': 0.01023,
    '866': 0.00423,
    '868': 0.00178,
    '881': 0.00426
}


# COMMAND ----------

##############################################################################
##############################################################################
##########################   DMA Mapping Starts  #############################
##############################################################################
##############################################################################

# COMMAND ----------

ref_dict = {"PORTLAND-AUBURN": 500, "NEW YORK": 501, "BINGHAMTON": 502, "MACON": 503, "PHILADELPHIA": 504, "DETROIT": 505, "BOSTON-MANCHESTER": 506, "SAVANNAH": 507, "PITTSBURGH": 508, "FT. WAYNE": 509, "CLEVELAND-AKRON-CANTON": 510, "WASHINGTON, DC-HAGARSTOWN": 511, "BALTIMORE": 512, "FLINT-SAGINAW-BAY CITY": 513, "BUFFALO": 514, "CINCINNATI": 515, "ERIE": 516, "CHARLOTTE": 517, "GREENSBORO-HIGH.POINT-WINSTON.SALEM": 518, "CHARLESTON, SC": 519, "AUGUSTA": 520, "PROVIDENCE-NEW BEDFORD": 521, "COLUMBUS, GA-OPELIKA, AL": 522, "BURLINGTON-PLATTSBURGH": 523, "ATLANTA": 524, "ALBANY, GA": 525, "UTICA": 526, "INDIANAPOLIS": 527, "MIAMI-FT. LAUDERDALE": 528, "LOUISVILLE": 529, "TALLAHASSEE-THOMASVILLE": 530, "TRI-CITIES, TN-VA": 531, "ALBANY-SCHENECTADY-TROY": 532, "HARTFORD-NEW HAVEN": 533, "ORLANDO-DAYTONA BEACH-MELBOURNE": 534, "COLUMBUS, OH": 535, "YOUNGSTOWN": 536, "BANGOR": 537, "ROCHESTER, NY": 538, "TAMPA-ST. PETERSBURG-SARASOTA": 539, "TRAVERSE CITY-CADILLAC": 540, "LEXINGTON": 541, "DAYTON": 542, "SPRINGFIELD-HOLYOKE": 543, "NORFOLK-PORTSMITH-NEWPORT NEWS": 544, "GREENVILLE-NEW BERN-WASHINGTON,NC": 545, "COLUMBIA, SC": 546, "TOLEDO": 547, "WEST PALM BEACH-FT. PIERCE": 548, "WATERTOWN": 549, "WILMINGTON": 550, "LANSING": 551, "PRESQUE ISLE": 552, "MARQUETTE": 553, "WHEELING-STEUBENVILLE": 554, "SYRACUSE": 555, "RICHMOND-PETERSBURG": 556, "KNOXVILLE": 557, "LIMA": 558, "BLUEFIELD-BECKLEY-OAK HILL": 559, "RALEIGH-DURHAM-FAYETTEVLLE": 560, "JACKSONVILLE": 561, "GRAND RAPIDS-KALMZOO-BATTLECREEK": 563, "CHARLESTON-HUNTINGTON": 564, "ELMIRA-CORNING": 565, "HARRISBURG-LANCASTER-LEBANON-YORK,PA": 566, "GREENVILLE-SPARTA-ASHEVILLE": 567, "HARRISONBURG": 569, "MYRTLE BEACH-FLORENCE": 570, "FT. MYERS-NAPLES": 571, "ROANOKE-LYNCHBURG": 573, "JOHNSTOWN-ALTOONA-ST COLGE": 574, "CHATTANOOGA": 575, "SALISBURY": 576, "WILKES BARRE-SCRANTON": 577, "TERRE HAUTE": 581, "LAFAYETTE, IN": 582, "ALPENA": 583, "CHARLOTTESVILLE": 584, "SOUTH BEND-ELKHART": 588, "GAINESVILLE": 592, "ZANESVILLE": 596, "PARKERSBURG": 597, "CLARKSBURG-WESTON": 598, "CORPUS CHRISTI": 600, "CHICAGO": 602, "JOPLIN-PITTSBURG": 603, "COLUMBIA-JEFFERSON CITY": 604, "TOPEKA": 605, "DOTHAN": 606, "ST. LOUIS": 609, "ROCKFORD": 610, "ROCHESTR-MASON CITY-AUSTIN": 611, "SHREVEPORT": 612, "MINNEAPOLIS-ST. PAUL": 613, "KANSAS CITY": 616, "MILWAUKEE": 617, "HOUSTON": 618, "SPRINGFIELD, MO": 619, "NEW ORLEANS": 622, "DALLAS-FT. WORTH": 623, "SIOUX CITY": 624, "WACO-TEMPLE-BRYAN": 625, "VICTORIA": 626, "WICHITA FALLS & LAWTON": 627, "MONROE-EL DORADO": 628, "BIRMINGHAM-ANNISTON-TUSCALOOSA": 630, "OTTUMWA-KIRKSVILLE": 631, "PADUCAH-CAPE GIRARDO-HARRISBURG": 632, "ODESSA-MIDLAND": 633, "AMARILLO": 634, "AUSTIN": 635, "HARLINGEN-WESLACO-BROWNSVILLE-MCARTHUR": 636, "CEDAR RAPIDS-WATERLOO-IOWA CITY & DUBUQUE, IA": 637, "ST. JOSEPH": 638, "JACKSON, TN": 639, "MEMPHIS": 640, "SAN ANTONIO": 641, "LAFAYETTE, LA": 642, "LAKE CHARLES": 643, "ALEXANDRIA, LA": 644, "GREENWOOD-GREENVILLE": 647, "CHAMPAIGN & SPRINGFIELD-DECATUR": 648, "EVANSVILLE": 649, "OKLAHOMA CITY": 650, "LUBBOCK": 651, "OMAHA": 652, "PANAMA CITY": 656, "SHERMAN-ADA": 657, "GREEN BAY-APPLETON": 658, "NASHVILLE": 659, "SAN ANGELO": 661, "ABILENE-SWEETWATER": 662, "MADISON": 669, "FT. SMITH-FAYETTEVILLE-SPRNGDALE-ROGERS": 670, "TULSA": 671, "COLUMBUS-TUPELO-WEST POINT": 673, "PEORIA-BLOOMINGTON": 675, "DULUTH-SUPERIOR": 676, "WICHITA-HUTCHINSON PLUS": 678, "DES MOINES-AMES": 679, "DAVENPORT-R.ISLAND-MOLINE": 682, "MOBILE-PENSACOLA": 686, "MINOT-BISMARCK-DICKINSON": 687, "HUNTSVILLE-DECATUR-FLORENCE": 691, "BEAUMONT-PORT ARTHUR": 692, "LITTLE ROCK-PINE BLUFF": 693, "MONTGOMERY-SELMA": 698, "LA CROSSE-EAU CLAIRE": 702, "WAUSAU-RHINELANDER": 705, "TYLER-LONGVIEW (LUFKIN&NACOGDOCHES)": 709, "HATTIESBURG-LAUREL": 710, "MERIDIAN": 711, "BATON ROUGE": 716, "QUINCY-HANNIBAL-KEOKUK": 717, "JACKSON, MS": 718, "LINCOLN & HASTINGS-KEARNY": 722, "FARGO-VALLEY CITY": 724, "SIOUX FALLS-MITCHELL": 725, "JONESBORO": 734, "BOWLING GREEN": 736, "MANKATO": 737, "NORTH PLATTE": 740, "ANCHORAGE": 743, "HONOLULU": 744, "FAIRBANKS": 745, "BILOXI-GULFPORT": 746, "JUNEAU": 747, "LAREDO": 749, "DENVER": 751, "COLORADO SPRINGS-PUEBLO": 752, "PHOENIX-PRESCOTT": 753, "BUTTE-BOZEMAN": 754, "GREAT FALLS": 755, "BILLINGS": 756, "BOISE": 757, "IDAHO FALLS-POCATELLO-JACKSON": 758, "CHEYENNE-SCOTTSBLUFF": 759, "TWIN FALLS": 760, "MISSOULA": 762, "RAPID CITY": 764, "EL PASO-LAS CRUCES": 765, "HELENA": 766, "CASPER-RIVERTON": 767, "SALT LAKE CITY": 770, "YUMA-EL CENTRO": 771, "GRAND JUNCTION-MONTROSE": 773, "TUCSON-SIERRA VISTA": 789, "ALBUQUERQUE-SANTA FE": 790, "GLENDIVE": 798, "BAKERSFIELD": 800, "EUGENE": 801, "EUREKA": 802, "LOS ANGELES": 803, "PALM SPRINGS": 804, "SAN FRANCISCO-OAKLAND-SAN JOSE": 807, "YAKIMA-PASCO-RICHLAND-KENNWICK": 810, "RENO": 811, "MEDFORD-KLAMATH FALLS": 813, "SEATTLE-TACOMA": 819, "PORTLAND, OR": 820, "BEND, OR": 821, "SAN DIEGO": 825, "MONTEREY-SALINAS": 828, "LAS VEGAS": 839, "SANTA BARBARA-SANTA MARIA-SAN LUIS OBISPO": 855, "SACRAMENTO-STOCKTON-MODESTO": 862, "FRESNO-VISALIA": 866, "CHICO-REDDING": 868, "SPOKANE": 881} 

# COMMAND ----------

import openai
import os
import pandas as pd
import numpy as np
import tiktoken
import json


## Define Class ##

class dma_map:
    
    def __init__(self, api_key, target_dict, model_engine = 'gpt-3.5-turbo', max_tokens = 4000):
        
        self.client = openai.OpenAI(api_key = api_key)
        self.model_engine = model_engine
        self.max_tokens = max_tokens
        self.encoding = tiktoken.encoding_for_model(self.model_engine)
        
        self.ref_dict =  ref_dict
        self.target_dict = target_dict
        self.prompt_text = f"Given refrence dma name and dma code mapping {self.ref_dict}. Generate dma mapping for \
            {self.target_dict} and return assocaited DMA code as python dictionary.**Make sure you have all DMAs generated**   \
            ** Be Precise with all columbus and assocated states**"
        self.gen_dict = None
        
        
    def append_prompt_text(self, text = None):
        if text:
            self.prompt_text = self.prompt_text+text
            
        token_len = len(self.encoding.encode(self.prompt_text))
        print(f"Allowed tokens are {self.max_tokens}, your prompt tokens are {token_len}")
        
        
    def gen_map(self):
        
        msg = [{"role": "user", "content":self.prompt_text}]

        token_len = len(self.encoding.encode(self.prompt_text))
        print(f"Allowed tokens are {self.max_tokens}, your prompt tokens are {token_len}")

        response = self.client.chat.completions.create(
                                            model= self.model_engine,
                                            max_tokens = self.max_tokens,
                                            messages = msg,
                                            temperature = 0
                                            )
        gen_dict = eval(response.choices[0].message.content)

        return gen_dict
    

    def QC1(self, user_ret_gen_dict):
        
        not_gen_list = list(set(list(self.ref_dict.values()))-set(list(user_ret_gen_dict.values())))
        false_gen_list = list(set(list(user_ret_gen_dict.values()))-set(list(self.ref_dict.values())))
        
        not_dma_name_list = list(set(self.target_dict)-set(list(user_ret_gen_dict.keys())))
        false_dma_name_list = list(set(list(user_ret_gen_dict.keys())) - set(self.target_dict))

        print(f"Following DMA Codes are not generated: {not_gen_list}")
        print(f"Following DMA Codes are falsely generated: {false_gen_list}")
        print("*****************************************************************")
        print(f"Following DMA Names are not generated: {not_dma_name_list}")
        print(f"Following DMA Names are falsely generated: {false_dma_name_list}")
        
        
        
    def QC2(self, user_ret_gen_dict):
        
        df_list = []

        if len(self.ref_dict.values())>=len(user_ret_gen_dict):
            for s_dma_name, s_dma_code in self.ref_dict.items():
                try:
                    m_dma_name = list(user_ret_gen_dict.keys())[list(user_ret_gen_dict.values()).index(s_dma_code)]
                    m_dma_code = user_ret_gen_dict[list(user_ret_gen_dict.keys())[list(user_ret_gen_dict.values()).index(s_dma_code)]]
                    df_list.append({'Target(Gen) DMA Code':m_dma_code, 'Target(Gen) DMA Name': m_dma_name, 'Ref. DMA Code': s_dma_code, 'Ref. DMA Name': s_dma_name})
                except:
                    df_list.append({'Target(Gen) DMA Code':m_dma_code, 'Target(Gen) DMA Name': "No Map for Code", 'Ref. DMA Code': s_dma_code, 'Ref. DMA Name': s_dma_name})

        else:
            for m_dma_name, m_dma_code in user_ret_gen_dict.items():
                try:
                    s_dma_name = list(self.ref_dict.keys())[list(self.ref_dict.values()).index(m_dma_code)]
                    s_dma_code = self.ref_dict[list(self.ref_dict.keys())[list(self.ref_dict.values()).index(m_dma_code)]]
                    df_list.append({'Target(Gen) DMA Code':m_dma_code, 'Target(Gen) DMA Name': m_dma_name, 'Ref. DMA Code': s_dma_code, 'Ref. DMA Name': s_dma_name})
                except:
                    df_list.append({'Target(Gen) DMA Code':m_dma_code, 'Target(Gen) DMA Name': m_dma_name, 'Ref. DMA Code': 'No Code', 'Ref. DMA Name': 'No Source DMA'})
        df_ = pd.DataFrame(df_list)
        df_['flag'] = 0  # Initialize the column with 0
        df_.loc[df_['Target(Gen) DMA Code'] == df_['Ref. DMA Code'], 'flag'] = 1

        return df_


## Execution ##

api_key = 
# map_obj = dma_map(api_key, target_dict)

# append_text = "**Be precise**"
# map_obj.append_prompt_text(append_text)

# gen_map = map_obj.gen_map()

# map_obj.QC1(gen_map)


# COMMAND ----------

## get_target_list ##
def get_traget_list(df,col):
  return [i[col] for i in df.select(col).dropDuplicates().collect()]

# COMMAND ----------

##############################################################################
##############################################################################
##########################   DMA Mapping Ends  ###############################
##############################################################################
##############################################################################

# COMMAND ----------

### TV CHAnnel Groupings ###
other_brand_ch_group = [
    "TV LAND",
    "Bounce TV",
    "Animal Planet",
    "TLC",
    "Fox Sports 1",
    "UP TV",
    "LMN",
    "MeTV",
    "National Geographic",
    "MLB Network",
    "CW",
    "ESPN2",
    "Bally Sports West",
    "NBC Sports Chicago",
    "Bally Sports South",
    "ROOT Sports Northwest",
    "Bally Sports San Diego",
    "Yes Network",
    "Bally Sports Midwest",
    "Sportsnet NY",
    "New England Sports Network",
    "Bally Sports Wisconsin",
    "Bally Sports Detroit",
    "Bally Sports Southwest",
    "Bally Sports Sun",
    "Bally Sports Florida",
    "Bally Sports Southeast",
    "Bally Sports Ohio",
    "Bally Sports Arizona",
    "RFD TV",
    "ESPNEWS",
    "FOX Sports 2",
    "CBS Sports",
    "Cleo TV",
    "MyNetworkTV"
]

other_drtv_ch_group = [
    'TBS',
    'Independent Film (IFC)',
    'Animal Planet',
    'Smithsonian',
    'FOX',
    'Bounce TV',
    'HGTV',
    'AccuWeather',
    'USA Network',
    'Cooking Channel',
    'Fox News',
    'A&E',
    'SYFY',
    'History Channel',
    'BET',
    'MyNetworkTV',
    'Oprah Winfrey Network',
    'Paramount Network',
    'Antenna TV',
    'OXYGEN',
    'GAC Family',
    'MSNBC',
    'Start TV',
    'WE TV',
    'TLC',
    'MeTV',
    'Movies!',
    'Scripps News',
    'ESPN',
    'Magnolia Network',
    'Discovery Channel',
    'Weather Channel',
    'ESPN2',
    'True Crime Network',
    'TV ONE',
    'AMC',
    'NHL',
    'Get TV',
    'Family Entertainment Television',
    'True Real TV',
    'Justice Central',
    'Comedy.TV'
]

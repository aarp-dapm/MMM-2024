# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.0 Reading Data

# COMMAND ----------

## Reaqding Data ##
rawdata_df=spark.sql('select * from temp.ja_blend_mmm2_MediaDf_Subch').filter(f.year(f.col('Date')).isin([2022, 2023])).toPandas()
rawdata_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.0 Tranformation Section

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Defining Functions

# COMMAND ----------

## Curves ##
curve_df = read_csv('dbfs:/blend360/sandbox/mmm/model/var_trans_curves/transformation_curves.csv')
pd_curve_df = curve_df.toPandas()
pd_curve_df

## Create Design Matrix ##
def create_custom_matrix(elements, size):
    # Initialize the matrix with zeros
    matrix = np.zeros((size, size))
    
    # Determine the number of elements to use for filling the matrix
    num_elements = len(elements)
    
    # Fill the matrix according to the specified pattern
    for i in range(size):
        for j in range(num_elements):
            if i + j < size:
                matrix[i, i + j] = elements[j]
    
    return matrix.T


## Creating Design matrix ##
design_dict = {}
for col in curve_df.columns:
  elements = list(pd_curve_df[col].values)
  size = 104
  design_dict[col] = create_custom_matrix(elements, size)

# COMMAND ----------

######################
###### AdStock #######
######################

## Vanilla Adstock ##
def adstock_func(var_data, factor):
    x = 0
    adstock_var = [x := x * factor + v for v in var_data]

    suffix = f'_AdStock{round(factor*100)}'
    return pd.Series(adstock_var, index=var_data.index), suffix


## Define the adstock function with week cutoff ##
def adstock_func_week_cut(var_data, decay_rate, cutoff_weeks):
    adstocked_data = np.zeros_like(var_data)
    for t in range(len(var_data)):
        for i in range(cutoff_weeks):
            if t - i >= 0:
                adstocked_data[t] += var_data[t - i] * (decay_rate ** i)

    suffix = f'_AdStock{round(decay_rate*100)}Week{cutoff_weeks}'
    return pd.Series(adstocked_data, index=var_data.index), suffix

## Implement custom Adstock curves ##
def custom_adstock(var_data, cutoff_weeks, peak_week, decay_rate):

  suffix = f'{cutoff_weeks}L{peak_week}Wk{round(decay_rate*100)}Ad'

  var_matrix = np.tile(list(var_data.values), (104,1))
  adstocked_data = np.sum(var_matrix*design_dict[suffix], axis=1)

  return pd.Series(adstocked_data, index=var_data.index), '_AdStock'+suffix




######################
###### Power #######
######################
def power_func(var_data, pow_value):
    pow_var = [x**pow_value for x in var_data]

    suffix = f'_Power{round(pow_value*100)}'
    return pd.Series(pow_var, index=var_data.index), suffix
  

## Range Function ##
def frange(x,y,jump):
  while x<y:
    yield round(float(x),2)

    x+=jump


###########################
###### Transformation #####
###########################

def single_transform(grouped_df, var_name, adstock_params, sat_params):


  single_transform_dict = {}

  dma_dict = {} ## To Store DMA level Information ##
  for cross_section, group in grouped_df:

    ## Tranformation ##
    original_series = group.set_index('Date')[var_name]
    
    # adstock_series, adstock_suffix = adstock_func_week_cut(original_series, *adstock_params)
    adstock_series, adstock_suffix = custom_adstock(original_series, *adstock_params)
    t_var_name = var_name + adstock_suffix

    power_series, pow_suffix = power_func(adstock_series, *sat_params)
    t_var_name = t_var_name + pow_suffix

    scaled_series= (power_series - power_series.min())/(power_series.max() - power_series.min()) ## Min Max Norm
    # scaled_series= power_series/power_series.mean() ## Unit Mean Norm
    # t_var_name = t_var_name + 'UnitMean'
    

    ## Concatination ##
    dma_dict[cross_section] = scaled_series
    # print(len(power_series))
    # dma_dict[cross_section] = power_series

  single_transform_dict[t_var_name] = dma_dict
  return single_transform_dict


#########################
#### Stack Operation ####
#########################

def stack_var_transfomartion(transform_list):
  concat_list = []
  name_list = []
  for idx, val in enumerate(transform_list):
    name_list.append(list(val.keys())[0])
    concat_list.append( pd.concat(val[list(val.keys())[0]],axis=0)   )

  temp = pd.concat(concat_list, axis=1)
  temp.columns = name_list
  return temp

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Transformation Dictionary

# COMMAND ----------

'''
Finding Columns for Subchannel

'''

cols = []
for col in rawdata_df.columns:
  if (col.split('_')[0] == 'TV') & (col.split('_')[-1] == 'Imps'):
    cols.append(col)
cols

# COMMAND ----------

'''

Any other Preprocessing

'''


# COMMAND ----------

##############################
### Best Frequentist Model ###
##############################

input_vars = [ 
'PromoEvnt_22MembDrive_Eng',
'PromoEvnt_22BlackFriday_Eng',

'July0422_lag',

'Seasonality_Feb', 
'Seasonality_Mar', 
'Seasonality_Apr',
'Seasonality_Dec',

'PromoEvnt_23MembDrive_Eng', 
'PromoEvnt_23BlackFriday_Eng',

'TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk90Ad_Power90',
'Social_Imps_AdStock6L1Wk90Ad_Power90',


'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'Email_Spend_AdStock6L1Wk20Ad_Power90',

'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'

         
       ]

# COMMAND ----------

#######################################
######### User Input Dict #############
#######################################
cols = [ 'TV_MediaCom_BrandTV_Imps', 'TV_MediaCom_NonBrandTV_Imps']

input_dict = {}
for col in cols:
  input_dict[col] = {'Adstock':{'week_cutoff':[6], 'peak_week':[2], 'decay_rate': [0.7]}, 'Saturation':{'power':[0.7]}}


#######################################
#### Model Input Transform Dict #######
#######################################


def ret_ListOfTuples(data_dict):
 
  combinations = list(itertools.product(*data_dict.values()))
  combinations = [tuple(combination) for combination in combinations]

  return combinations
  


transform_dict ={ }

for key, value in input_dict.items():
  transform_dict[key] = {}  ## initializing key
  for key2, value2 in value.items():
    transform_dict[key][key2] = ret_ListOfTuples(value2)

transform_dict

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Transformation Process

# COMMAND ----------

# MAGIC %time
# MAGIC
# MAGIC grouped_df = rawdata_df.groupby('DmaCode')[['Date', 'TV_MediaCom_BrandTV_Imps', 'TV_MediaCom_NonBrandTV_Imps']]
# MAGIC
# MAGIC print("Starting Transformations")
# MAGIC ### Tranformation ###
# MAGIC
# MAGIC var_transform_dict ={}
# MAGIC for var_name, params in transform_dict.items():
# MAGIC
# MAGIC   print(f"Transforming {var_name}")
# MAGIC   var_transform_dict[var_name] = []
# MAGIC   for Adstock in params['Adstock']:
# MAGIC     for sat in params['Saturation']:
# MAGIC       var_transform_dict[var_name].append(single_transform(grouped_df, var_name, Adstock, sat))
# MAGIC
# MAGIC print("Finished Transformations, Starting to Stack")
# MAGIC ### Stack-Operation ###
# MAGIC stack_list = []
# MAGIC for var_name in transform_dict:
# MAGIC   stack_list.append(stack_var_transfomartion(var_transform_dict[var_name]))
# MAGIC
# MAGIC print("Final Stack")
# MAGIC ## Final Stack ##
# MAGIC transformed_df = pd.concat(stack_list, axis=1).reset_index()
# MAGIC transformed_df.rename(columns={'level_0':'DmaCode'}, inplace=True)
# MAGIC transformed_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Creating one final Model Dataframe 

# COMMAND ----------

## saving model dataframe ##
## Saving Model ##
transformed_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_Campaign_TV_MediaCom.csv')

## Reading Model ##
transformed_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_Campaign_TV_MediaCom.csv').drop('Unnamed: 0',axis=1)
transformed_df['Date'] = pd.to_datetime(transformed_df['Date'])
transformed_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.0 Model Development

# COMMAND ----------

## Reading SubChannel Level Data ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_SubChannel_TV.csv').drop('Unnamed: 0',axis=1)
model_df.head()

# COMMAND ----------


##############################################################
###### Appending Spend, Imps, Clicks from RawData ############
##############################################################
submodel_cols = ['DmaCode', 'Date', 'TV_MediaCom_BrandTV_Spend', 'TV_MediaCom_NonBrandTV_Spend',  'TV_MediaCom_BrandTV_Imps', 'TV_MediaCom_NonBrandTV_Imps',  'TV_MediaCom_BrandTV_Clicks', 'TV_MediaCom_NonBrandTV_Clicks']

## Creating New Columns ##
submodel_rawdata_df = rawdata_df[submodel_cols]

submodel_rawdata_df['DmaCode'] = submodel_rawdata_df['DmaCode'].astype('int')
submodel_rawdata_df['Date'] = pd.to_datetime(submodel_rawdata_df['Date'])


model_df['DmaCode'] = model_df['DmaCode'].astype('int')
model_df['Date'] = pd.to_datetime(model_df['Date'])

## Merging Extended Transformations ##

model_df = model_df.merge(submodel_rawdata_df, on=['DmaCode', 'Date'])



##############################################
###### Appending Tarnsformed Data ############
##############################################

## Creating New Columns ##
transformed_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_Campaign_TV_MediaCom.csv').drop('Unnamed: 0',axis=1)
transformed_df['Date'] = pd.to_datetime(transformed_df['Date'])

model_df['DmaCode'] = model_df['DmaCode'].astype('int')
model_df['Date'] = pd.to_datetime(model_df['Date'])

## Merging Unit Mean Norm ##
model_df = model_df.merge(transformed_df, on=['DmaCode', 'Date'])



## saving model dataframe ##
model_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_Campaign_TV_MediaCom.csv')
## Reading Model ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_Campaign_TV_MediaCom.csv').drop('Unnamed: 0',axis=1)
model_df.head()

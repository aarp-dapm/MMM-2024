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
rawdata_df=spark.sql('select * from temp.ja_blend_mmm2_DateDmaAge_SubChModelDf').toPandas()



## Archive: Creating CrossSection at DMA and Age Bucket level ##
# rawdata_df.insert(1, 'DmaAge',rawdata_df['DmaCode']+ "-" + rawdata_df['AgeBucket'])
# rawdata_df.drop(['DmaCode', 'AgeBucket'], axis=1, inplace=True)

## Creating CrossSection at DMA level ##
rawdata_df.drop(['AgeBucket'], axis=1, inplace=True)
rawdata_df = rawdata_df.groupby(['Date', 'DmaCode']).sum().reset_index()




## Dropping Few Columns ###
cols2drop = [ 
             'ASISocial_Spend', 'IcmDigitalSocial_Spend', 'IcmSocialSocial_Spend', 'MediaComSocial_Spend', 'MembershipDigitalSocial_Spend',
 'ASISocial_Imps', 'IcmDigitalSocial_Imps', 'IcmSocialSocial_Imps', 'MediaComSocial_Imps', 'MembershipDigitalSocial_Imps',
 'ASISocial_Clicks', 'IcmDigitalSocial_Clicks', 'IcmSocialSocial_Clicks', 'MediaComSocial_Clicks', 'MembershipDigitalSocial_Clicks'
 ]

rawdata_df = rawdata_df.drop(columns=cols2drop).sort_values(by=['DmaCode', 'Date'])
rawdata_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Column Classification

# COMMAND ----------

'''
Not Needed for this problem

'''

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Model Data

# COMMAND ----------

'''
Not Needed for this problem

'''

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
  if (col.split('_')[0] == 'LeadGen') & (col.split('_')[-1] == 'Imps'):
    cols.append(col)
cols

# COMMAND ----------

'''

Any other Preprocessing

'''


# COMMAND ----------

#######################################
######### User Input Dict #############
#######################################
cols = ['LeadGen_LeadGen_CARDLINKING_Imps', 'LeadGen_LeadGen_CoRegLeads_Imps', 'LeadGen_LeadGen_Impact_Imps', 'LeadGen_LeadGen_Linkouts_Imps', 'LeadGen_LeadGen_MajorRocket_Imps', 'LeadGen_LeadGen_OfferWalls_Imps']
input_dict ={
  
  
  'LeadGen_LeadGen_CARDLINKING_Imps' : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
  'LeadGen_LeadGen_CoRegLeads_Imps' : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
  'LeadGen_LeadGen_Impact_Imps' : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
  'LeadGen_LeadGen_Linkouts_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
  'LeadGen_LeadGen_MajorRocket_Imps' : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
  'LeadGen_LeadGen_OfferWalls_Imps' : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
}

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
# MAGIC grouped_df = rawdata_df.groupby('DmaCode')[['Date', 'LeadGen_LeadGen_CARDLINKING_Imps', 'LeadGen_LeadGen_CoRegLeads_Imps', 'LeadGen_LeadGen_Impact_Imps', 'LeadGen_LeadGen_Linkouts_Imps', 'LeadGen_LeadGen_MajorRocket_Imps', 'LeadGen_LeadGen_OfferWalls_Imps']]
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

transformed_df.head()

# COMMAND ----------

## saving model dataframe ##
# transformed_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel.csv') ## MinMax Norm
# transformed_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_extendedVo1_statsmodel.csv') ## MinMax Norm 2, Extended Transformation
# transformed_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_unitmean_statsmodel.csv') ## UnitMean Norm
# transformed_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_EmailSpend_statsmodel.csv') ## Email SPend Transformation 
# transformed_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_LeadgenImps_statsmodel.csv')
# transformed_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_IndividualSocialImps_statsmodel.csv')
# transformed_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_RadioSpendVo2.csv')

## Saving Model ##
# transformed_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_SubChannel_Leadgen.csv')

## Reading Model ##
transformed_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_SubChannel_Leadgen.csv').drop('Unnamed: 0',axis=1)
transformed_df['Date'] = pd.to_datetime(transformed_df['Date'])
transformed_df.head()

# COMMAND ----------

## Creating Model Dataframe ##


############################
###### Append 1 ############
############################

## Creating New Columns ##
# rawdata_df['DmaCode'] = rawdata_df['DmaCode'].astype('int')
# rawdata_df['Date'] = pd.to_datetime(rawdata_df['Date'])

## Merging Min Max Norm ##
# transformed_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodelVo2.csv').drop('Unnamed: 0',axis=1)
# transformed_df['Date'] = pd.to_datetime(transformed_df['Date'])

## Remove Radio Spend ##
# r_list = []
# for col in transformed_df.columns:
#   if col.startswith('Radio_Spend_Ad'):
#     r_list.append(col)

# transformed_df = transformed_df.drop(columns=r_list)

# model_df = rawdata_df.merge(transformed_df, on=['DmaCode', 'Date'])


############################
###### Append 2 ############
############################

## Creating New Columns ##
# transformed_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_RadioSpendVo2.csv').drop('Unnamed: 0',axis=1)
# transformed_df['Date'] = pd.to_datetime(transformed_df['Date'])

# model_df['DmaCode'] = model_df['DmaCode'].astype('int')
# model_df['Date'] = pd.to_datetime(model_df['Date'])

## Merging Extended Transformations ##
# model_df = model_df.merge(transformed_df, on=['DmaCode', 'Date'])


############################
###### Append 3 ############
############################

## Creating New Columns ##
# transformed_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_EmailSpend_statsmodel.csv').drop('Unnamed: 0',axis=1)
# transformed_df['Date'] = pd.to_datetime(transformed_df['Date'])

# model_df['DmaCode'] = model_df['DmaCode'].astype('int')
# model_df['Date'] = pd.to_datetime(model_df['Date'])

## Merging Unit Mean Norm ##
# model_df = model_df.merge(transformed_df, on=['DmaCode', 'Date'])





############################
###### Append 4 ############
############################

# Creating New Columns ##
# transformed_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_LeadgenImps_statsmodel.csv').drop('Unnamed: 0',axis=1)
# transformed_df['Date'] = pd.to_datetime(transformed_df['Date'])

# model_df['DmaCode'] = model_df['DmaCode'].astype('int')
# model_df['Date'] = pd.to_datetime(model_df['Date'])

## Merging Unit Mean Norm ##
# model_df = model_df.merge(transformed_df, on=['DmaCode', 'Date'])


############################
###### Append 5 ############
############################

## Creating New Columns ##
# transformed_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_IndividualSocialImps_statsmodel.csv').drop('Unnamed: 0',axis=1)
# transformed_df['Date'] = pd.to_datetime(transformed_df['Date'])

# model_df['DmaCode'] = model_df['DmaCode'].astype('int')
# model_df['Date'] = pd.to_datetime(model_df['Date'])

## Merging Unit Mean Norm ##
# model_df = model_df.merge(transformed_df, on=['DmaCode', 'Date'])
# model_df.shape, transformed_df.shape

############################
###### Saving Df  ##########
############################

## saving model dataframe ##
# model_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_df_statsmodel.csv')


# Reading model dataframe #
# model_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_df_statsmodel.csv').drop('Unnamed: 0',axis=1)
# model_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Adding More Variables

# COMMAND ----------


###################################################################################################
################################## Dummy Variables ################################################
###################################################################################################

###############
#### 2022 #####
###############

## Adding Peak flag for 2022-02-28 ##
model_df['Peak_20220228'] = model_df['Date'].apply(lambda x: 1 if x == '2022-02-28' else 0)


## Adding 4th Of July Flag ##
model_df['July0422'] = model_df['Date'].apply(lambda x: 1 if x == '2022-07-04' else 0)
model_df['July0422_lag'] = model_df['Date'].apply(lambda x: 1 if x == '2022-07-11' else 0)

## Adding flag for 10/10/2022 ##
model_df['dummy_20221010'] = model_df['Date'].apply(lambda x: 1 if x == '2022-10-10' else 0)

## Adding Peak flag for 2022-11-28 ##
model_df['Peak_20221128'] = model_df['Date'].apply(lambda x: 1 if x == '2022-11-28' else 0)

###############
#### 2023 #####
###############

## Adding Peak flag for 2023-02-27 ##
model_df['Peak_20230227'] = model_df['Date'].apply(lambda x: 1 if x == '2023-02-27' else 0)


## Adding 4th Of July Flag ##
model_df['July0423'] = model_df['Date'].apply(lambda x: 1 if x == '2023-07-03' else 0)

## Adding Peak flag for 2023-11-27 ##
model_df['Peak_20231127'] = model_df['Date'].apply(lambda x: 1 if x == '2023-11-27' else 0)


###################################################################################################
################################## Special Variable Transformation ################################
###################################################################################################


### Splitting Leadgen ###
model_df['Date'] = pd.to_datetime(model_df['Date']) 
mask_2022 = model_df['Date'].dt.year==2022

### Splitting Leadgen ###
var = 'LeadGen_Imps_AdStock6L1Wk60Ad_Power80'
model_df[f'{var}_2022'] = model_df[var]*mask_2022
model_df[f'{var}_2023'] = model_df[var]*(1-mask_2022)

## Min max Scaling Target Variable ##
# model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2022'] = model_df.groupby('DmaCode')['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2022'].transform(min_max_normalize)
# model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2023'] = model_df.groupby('DmaCode')['LeadGen_Imps_AdStock6L1Wk60Ad_Power80_2023'].transform(min_max_normalize)


### Splitting Email ###
var = 'Email_Imps_AdStock6L1Wk60Ad_Power30'
model_df[f'{var}_2022'] = model_df[var]*mask_2022
model_df[f'{var}_2023'] = model_df[var]*(1-mask_2022)

### SPlitting Display ###
var = 'Display_Imps_AdStock6L4Wk30Ad_Power30'
model_df[f'{var}_2022'] = model_df[var]*mask_2022
model_df[f'{var}_2023'] = model_df[var]*(1-mask_2022)



## Fill NaN with Zero ##
model_df = model_df.fillna(0)

# COMMAND ----------




## Adding a Trend Component ##
model_df = model_df.sort_values(by=['DmaCode', 'Date'])
model_df['Trend'] = model_df.groupby('DmaCode').cumcount() + 1


## Min max Scaling Target Variable ##
model_df['Joins_norm'] = model_df.groupby('DmaCode')['Joins'].transform(min_max_normalize)

## Unit Mean Scaling Target Variable ##
model_df['Joins_unitNorm']  = model_df.groupby('DmaCode')['Joins'].transform(unit_mean_normalize)

# COMMAND ----------

## Adding more PromoVariables ##
'''

Below variables were provided on 6/28/2024, in middle of model devleopment

'''

promo_vars_23 = ['DmaCode', 'Date', 'PromoEvnt_23MembDrive', 'PromoEvnt_23MemorialDay', 'PromoEvnt_23LaborDay', 'PromoEvnt_23BlackFriday']
promo_df_23 = rawdata_df[promo_vars_23]

############################
###### Append 4 ############
############################

## Creating New Columns ##
model_df['DmaCode'] = model_df['DmaCode'].astype('int')
model_df['Date'] = pd.to_datetime(model_df['Date'])


promo_df_23['DmaCode'] = promo_df_23['DmaCode'].astype('int')
promo_df_23['Date'] = pd.to_datetime(promo_df_23['Date'])



## Merging Min Max Norm ##

model_df = model_df.merge(promo_df_23, on=['DmaCode', 'Date'])


## Define Functions ##
def transform_PromoFlags(val):
  val = val/val.max()
  return val



## Engineering Promo Events Features ##

promo_cols = ['PromoEvnt_22MembDrive',
       'PromoEvnt_22BlackFriday',
       'PromoEvnt_22LaborDay',
       'PromoEvnt_22MemorialDay','PromoEvnt_23MembDrive', 
       'PromoEvnt_23BlackFriday',
       'PromoEvnt_23LaborDay',
       'PromoEvnt_23MemorialDay',]

for col in promo_cols:
  model_df[f'{col}_Eng'] = model_df[col]*model_df['Joins_norm']
  model_df[f'{col}_Eng'] = model_df[f'{col}_Eng'].transform(transform_PromoFlags)







## Creating One Single Variable for Promos ##

# model_df['PromoEvnt'] = model_df.apply(lambda x: 1 if x['PromoEvnt_22MembDrive'] or x['PromoEvnt_22MemorialDay'] or x['PromoEvnt_22LaborDay'] or x['PromoEvnt_22BlackFriday'] or x['PromoEvnt_22WsjTodayShow'] or x['PromoEvnt_23RollingStone'] else 0 , axis=1 )
# model_df.head()

# COMMAND ----------

## Adding more Seasonal Variables ##
'''

More Seasonal Variables added

'''

more_seasonal_vars = ['DmaCode', 'Date','Seasonality_Sin7', 'Seasonality_Cos7','Seasonality_Sin8', 'Seasonality_Cos8','Seasonality_Sin9', 'Seasonality_Cos9','Seasonality_Sin10', 'Seasonality_Cos10']
seasonal_df_23 = rawdata_df[more_seasonal_vars]

############################
###### Append 5 ############
############################

## Creating New Columns ##
model_df['DmaCode'] = model_df['DmaCode'].astype('int')
model_df['Date'] = pd.to_datetime(model_df['Date'])


seasonal_df_23['DmaCode'] = seasonal_df_23['DmaCode'].astype('int')
seasonal_df_23['Date'] = pd.to_datetime(seasonal_df_23['Date'])



## Merging  ##

model_df = model_df.merge(seasonal_df_23, on=['DmaCode', 'Date'])


################################################
###### Creating More Seasoanl Flags ############
################################################

## Creating Quarter Flags ##
model_df['Seasonality_Q1'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month in [1,2,3] else 0)
model_df['Seasonality_Q2'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month in [4,5,6] else 0)
model_df['Seasonality_Q3'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month in [7,8,9] else 0)
model_df['Seasonality_Q4'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month in [10,11,12] else 0)

## Creating Monthly Flags ##
model_df['Seasonality_Jan'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 1 else 0)
model_df['Seasonality_Feb'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 2 else 0)
model_df['Seasonality_Mar'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 3 else 0)

model_df['Seasonality_Apr'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 4 else 0)
model_df['Seasonality_May'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 5 else 0)
model_df['Seasonality_Jun'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 6 else 0)

model_df['Seasonality_Jul'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 7 else 0)
model_df['Seasonality_Aug'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 8 else 0)
model_df['Seasonality_Sep'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 9 else 0)

model_df['Seasonality_Oct'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 10 else 0)
model_df['Seasonality_Nov'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 11 else 0)
model_df['Seasonality_Dec'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).month == 12 else 0)


## Creating Year Flags ##
model_df['Seasonality_Year23'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).year==2023 else 0)

model_df.head()


# COMMAND ----------

'''

More Variables will be added here

'''

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.0 Model Development

# COMMAND ----------

'''
Saving and Reading Model Data
'''



## Reading Model ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_statsmodel.csv').drop('Unnamed: 0',axis=1)

main_model_cols_support  = ['DmaCode', 'Date', 'Joins', 'Joins_norm'] + ['PromoEvnt_22MembDrive_Eng',
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
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90']


main_model_cols_spend = ['TV_Spend', 'Search_Spend', 'Social_Spend', 'LeadGen_Spend', 'Email_Spend', 'DirectMail_Spend', 'AltMedia_Spend', 'Video_Spend', 'Display_Spend', 'Print_Spend']


## Updating Dataframe ##

'''
Updating Dataframe with relvant columns only from main model
'''
model_df = model_df[main_model_cols_support + main_model_cols_spend]


# COMMAND ----------


##############################################################
###### Appending Spend, Imps, Clicks from RawData ############
##############################################################
submodel_cols = ['DmaCode', 'Date', 'LeadGen_LeadGen_CARDLINKING_Spend', 'LeadGen_LeadGen_CoRegLeads_Spend', 'LeadGen_LeadGen_Impact_Spend', 'LeadGen_LeadGen_Linkouts_Spend', 'LeadGen_LeadGen_MajorRocket_Spend', 'LeadGen_LeadGen_OfferWalls_Spend','LeadGen_LeadGen_CARDLINKING_Imps', 'LeadGen_LeadGen_CoRegLeads_Imps', 'LeadGen_LeadGen_Impact_Imps', 'LeadGen_LeadGen_Linkouts_Imps', 'LeadGen_LeadGen_MajorRocket_Imps', 'LeadGen_LeadGen_OfferWalls_Imps','LeadGen_LeadGen_CARDLINKING_Clicks', 'LeadGen_LeadGen_CoRegLeads_Clicks', 'LeadGen_LeadGen_Impact_Clicks', 'LeadGen_LeadGen_Linkouts_Clicks', 'LeadGen_LeadGen_MajorRocket_Clicks', 'LeadGen_LeadGen_OfferWalls_Clicks']

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
transformed_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_SubChannel_Leadgen.csv').drop('Unnamed: 0',axis=1)
transformed_df['Date'] = pd.to_datetime(transformed_df['Date'])

model_df['DmaCode'] = model_df['DmaCode'].astype('int')
model_df['Date'] = pd.to_datetime(model_df['Date'])

## Merging Unit Mean Norm ##
model_df = model_df.merge(transformed_df, on=['DmaCode', 'Date'])



## saving model dataframe ##
model_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_SubChannel_Leadgen.csv')
## Reading Model ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_SubChannel_Leadgen.csv').drop('Unnamed: 0',axis=1)
model_df.head()

# COMMAND ----------



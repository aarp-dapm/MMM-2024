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
rawdata_df=spark.sql('select * from temp.ja_blend_mmm2_DateDmaAge_ChModelDf').toPandas()


## Archive: Creating CrossSection at DMA and Age Bucket level ##
# rawdata_df.insert(1, 'DmaAge',rawdata_df['DmaCode']+ "-" + rawdata_df['AgeBucket'])
# rawdata_df.drop(['DmaCode', 'AgeBucket'], axis=1, inplace=True)

## Creating CrossSection at DMA level ##
rawdata_df.drop(['AgeBucket'], axis=1, inplace=True)
rawdata_df = rawdata_df.groupby(['Date', 'DmaCode']).sum().reset_index()
rawdata_df.display()

# COMMAND ----------

for col in rawdata_df.columns:
  print(col)

# COMMAND ----------

spend_col = []
for col in rawdata_df.columns:
  if col.endswith('Spend'):
    spend_col.append(col)


rawdata_df.groupby(pd.to_datetime(rawdata_df['Date']).dt.year)[spend_col].sum().reset_index().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Column Classification

# COMMAND ----------

## General Classification ##
primary_cols = ['DmaCode', 'Date']
target_col = ['Joins']
other_col = ['Reg', 'BenRefAll']

spend_col = ['Affiliate_Spend', 'AltMedia_Spend', 'Audio_Spend', 'DMP_Spend', 'DirectMail_Spend', 'Display_Spend', 'Email_Spend', 'LeadGen_Spend', 'Radio_Spend', 'OOH_Spend', 'Print_Spend', 'Search_Spend','Social_Spend', 'TV_Spend', 'Video_Spend']

imps_col = ['Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps', 'LeadGen_Imps',
       'Radio_Imps', 'OOH_Imps', 'Print_Imps', 'Search_Imps', 'Social_Imps', 'TV_Imps', 'Video_Imps']

clicks_col = ['Affiliate_Clicks', 'AltMedia_Clicks', 'Audio_Clicks', 'DMP_Clicks', 'DirectMail_Clicks', 'Display_Clicks', 'Email_Clicks', 'LeadGen_Clicks', 'Radio_Clicks', 'OOH_Clicks', 'Print_Clicks', 'Search_Clicks', 'Social_Clicks', 'TV_Clicks', 'Video_Clicks']

HnP_col = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow',
       'PromoEvnt_22TikTok', 'Holiday_22Christams', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23EvdaySavCamp', 'PromoEvnt_23RollingStone',
       'PromoEvnt_23MembDrive', 'PromoEvnt_23MemorialDay',
       'PromoEvnt_23LaborDay', 'PromoEvnt_23BlackFriday', 'Holiday_23Christams']

seasonality_col = ['Seasonality_Sin1', 'Seasonality_Cos1','Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin3', 'Seasonality_Cos3', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin5', 'Seasonality_Cos5', 'Seasonality_Sin6', 'Seasonality_Cos6', 'Seasonality_Sin7', 'Seasonality_Cos7', 'Seasonality_Sin8', 'Seasonality_Cos8', 'Seasonality_Sin9', 'Seasonality_Cos9', 'Seasonality_Sin10', 'Seasonality_Cos10']

Brand_col = ['Index', 'Buzz', 'Impression', 'Quality', 'Value', 'Reputation', 'Satisfaction', 'Recommend',
       'Awareness', 'Attention', 'Ad_Awareness', 'WOM_Exposure', 'Consideration', 'Purchase_Intent', 'Current_Customer', 'Former_Customer']

## Model Building Columns ##
primary_cols = ['DmaCode', 'Date']
target_col = ['Joins']

MediaModel_col = ['Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps', 'LeadGen_Imps',
       'Radio_Imps', 'OOH_Imps', 'Print_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps']  ## Adding Search Clicks and removing Search Impressions 

HnP_col = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow',
       'PromoEvnt_22TikTok', 'Holiday_22Christams', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23EvdaySavCamp', 'PromoEvnt_23RollingStone',
       'PromoEvnt_23MembDrive', 'PromoEvnt_23MemorialDay',
       'PromoEvnt_23LaborDay', 'PromoEvnt_23BlackFriday', 'Holiday_23Christams']

seasonality_col = ['Seasonality_Sin1', 'Seasonality_Cos1','Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin3', 'Seasonality_Cos3', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin5', 'Seasonality_Cos5', 'Seasonality_Sin6', 'Seasonality_Cos6',  'Seasonality_Sin7', 'Seasonality_Cos7', 'Seasonality_Sin8', 'Seasonality_Cos8', 'Seasonality_Sin9', 'Seasonality_Cos9', 'Seasonality_Sin10', 'Seasonality_Cos10']

Brand_col = ['Index', 'Buzz', 'Impression', 'Quality', 'Value', 'Reputation', 'Satisfaction', 'Recommend',
       'Awareness', 'Attention', 'Ad_Awareness', 'WOM_Exposure', 'Consideration', 'Purchase_Intent', 'Current_Customer', 'Former_Customer']

other_col = ['Reg', 'BenRefAll']


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Model Data

# COMMAND ----------

more_cols = [ 
'ASISocial_Spend', 'IcmDigitalSocial_Spend', 'IcmSocialSocial_Spend', 'MediaComSocial_Spend', 'MembershipDigitalSocial_Spend',
'ASISocial_Imps', 'IcmDigitalSocial_Imps', 'IcmSocialSocial_Imps', 'MediaComSocial_Imps', 'MembershipDigitalSocial_Imps',
'ASISocial_Clicks', 'IcmDigitalSocial_Clicks', 'IcmSocialSocial_Clicks', 'MediaComSocial_Clicks','MembershipDigitalSocial_Clicks'
]

rawdata_df = rawdata_df[primary_cols+target_col+MediaModel_col+spend_col+HnP_col+seasonality_col+Brand_col+other_col + more_cols].sort_values(by=['DmaCode', 'Date'])

## Adding Radio Spend only for 2023 ##
rawdata_df['Radio_Spend_2023'] = 
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

#######################################
######### User Input Dict #############
#######################################

# input_dict ={
  
  
#   'Affiliate_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'AltMedia_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'Audio_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'DMP_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'DirectMail_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'Display_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},  
#   'Email_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'LeadGen_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'Radio_Spend': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'OOH_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'Print_Imps':  {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'Search_Clicks': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'Social_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'TV_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
#   'Video_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}}
   
# }


# input_dict ={
  
  
#   'Affiliate_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'AltMedia_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'Audio_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'DMP_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'DirectMail_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'Display_Imps':   {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'Email_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'LeadGen_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'Radio_Spend': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'OOH_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'Print_Imps':  {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'Search_Clicks': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'Social_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'TV_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'Video_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},


#   'ASISocial_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'IcmDigitalSocial_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'IcmSocialSocial_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'MediaComSocial_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
#   'MembershipDigitalSocial_Imps': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},


#   'Email_Spend': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
   
# }

input_dict ={
  
  
  'Radio_Spend': {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.1,0.9,0.1))}, 'Saturation':{'power':list(frange(0.1,0.9,0.1))}},
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
# MAGIC grouped_df = rawdata_df.groupby('DmaCode')[['Date', 'Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps', 'LeadGen_Imps', 'Radio_Spend', 'OOH_Imps', 'Print_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps', 'Email_Spend', 'ASISocial_Imps', 'IcmDigitalSocial_Imps', 'IcmSocialSocial_Imps', 'MediaComSocial_Imps', 'MembershipDigitalSocial_Imps']]
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

## Vo2 ##
# transformed_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodelVo2.csv')
transformed_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_RadioSpendVo2.csv')

## Reading Model ##
transformed_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_RadioSpendVo2.csv').drop('Unnamed: 0',axis=1)
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
model_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_df_statsmodel.csv').drop('Unnamed: 0',axis=1)
model_df.head()

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

## saving model dataframe ##
# model_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_statsmodel.csv')

## Reading Model ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_statsmodel.csv').drop('Unnamed: 0',axis=1)
model_df.head()

# COMMAND ----------

for col in model_df.columns:
  if col.startswith("Holiday"):
    print(col)

# COMMAND ----------

## Model Building Columns Inventory ##
primary_cols = ['DmaCode', 'Date']
target_col = ['Joins']

MediaModel_col = ['Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps', 'LeadGen_Imps',
       'Radio_Spend', 'OOH_Imps', 'Print_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps'] 

HnP_col = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow',
       'PromoEvnt_22TikTok', 'Holiday_22Christams', 'PromoEvnt_23KaylaCoupon', 
       'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'PromoEvnt_23RollingStone', 'Holiday_23Christams'] ## Removed  'PromoEvnt_23EvdaySavCamp' (con)

seasonality_col = ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6']

Brand_col = ['Index', 'Buzz', 'Impression', 'Quality', 'Value', 'Reputation', 'Satisfaction', 'Recommend',
       'Awareness', 'Attention', 'Ad_Awareness', 'WOM_Exposure', 'Consideration', 'Purchase_Intent', 'Current_Customer', 'Former_Customer']

dummy_col = ['dummy_20220711']

other_col = ['Reg', 'BenRefAll']

# COMMAND ----------

'''

Getting each channel transformation with best correlation to start with

'''

## Getting best variable for each channel ##
non_transformed_cols_list = list(transform_dict.keys())
transformed_cols_key_value_pair = {col:[col2 for col2 in list(model_df.columns) if col2.startswith(col+"_") ]  for col in non_transformed_cols_list} 

## Getting Correlation of Each Tranformation with Target Variable ##
corr_dict = {}
for key in transformed_cols_key_value_pair:

  idx = transformed_cols_key_value_pair[key]
  corr_ch = model_df[idx+['Joins']].corr().loc[idx,'Joins'].sort_values(ascending=False)
  corr_dict[key] = corr_ch.to_dict() 

## Getting Best Correlation One ##
corr_dict_best = {}
for key in corr_dict:
  corr_dict_best[key] = list(corr_dict[key].keys())[0]

corr_dict_best.values()

# COMMAND ----------

# ## Vizualization BlocK ##
# def viz_transform(var_name = 'Affiliate_Imps', transformed_var_name = 'Affiliate_Imps_AdStock90_Power30' ):

#   ## Create Data ##
#   df1 = model_df.groupby('Date').agg({var_name:'sum'})
#   df2 = model_df.groupby('Date').agg({transformed_var_name:'sum'})

#   df = df1.join(df2)

#   # Create subplots
#   fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

#   # Plot all columns on the left-hand side
#   df.plot(ax=ax1, marker='o')
#   ax1.set_title(f'{var_name} and {transformed_var_name}')
#   ax1.set_xlabel('Date')
#   ax1.set_ylabel('Values')
#   ax1.legend(title='Columns')
#   ax1.grid(True)

#   # Plot a selected column on the right-hand side (e.g., column 'A')
#   df2.plot(ax=ax2, color='tab:blue', marker='o')
#   ax2.set_title(f'{var_name} and {transformed_var_name}')
#   ax2.set_xlabel('Date')
#   ax2.set_ylabel(transformed_var_name)
#   ax2.grid(True)

#   # Show the plots
#   plt.tight_layout()
#   plt.show()


# ## Function Call ##
# viz_transform(var_name = 'Affiliate_Imps', transformed_var_name = 'Affiliate_Imps_AdStock90Week6_Power90'  )

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1 Model Creation with Best Correlation

# COMMAND ----------

## Model Creation ##
seasoanlity_vars =  ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6'] # Double Check on Fourier Frequencies 

Holiday_vars = ['Holiday_22Christams', 'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'Holiday_23Christams']

Promo_vars = ['PromoEvnt_22MembDrive','PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow', 'PromoEvnt_22TikTok', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23RollingStone']

dummy_vars = ['dummy_20220711']


media_vars = ['Affiliate_Imps_AdStock6L5Wk90Ad_Power90', 'AltMedia_Imps_AdStock13L5Wk30Ad_Power90', 'Audio_Imps_AdStock6L5Wk90Ad_Power30', 'DMP_Imps_AdStock13L1Wk30Ad_Power90', 'DirectMail_Imps_AdStock13L4Wk60Ad_Power70', 'Display_Imps_AdStock13L5Wk50Ad_Power30', 'Email_Imps_AdStock6L2Wk60Ad_Power30', 'LeadGen_Imps_AdStock6L1Wk70Ad_Power90', 'Radio_Spend_AdStock13L1Wk30Ad_Power30', 'OOH_Imps_AdStock13L1Wk30Ad_Power30', 'Print_Imps_AdStock13L1Wk30Ad_Power30', 'Search_Clicks_AdStock6L1Wk70Ad_Power30', 'Social_Imps_AdStock13L5Wk30Ad_Power30', 'TV_Imps_AdStock6L1Wk50Ad_Power30', 'Video_Imps_AdStock6L1Wk50Ad_Power30']


Regularization = False

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2 Model Creation with priors from previous model

# COMMAND ----------

## Model Creation ##
seasoanlity_vars =  ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6'] # Double Check on Fourier Frequencies 

Holiday_vars = ['Holiday_22Christams', 'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'Holiday_23Christams']

Promo_vars = ['PromoEvnt_22MembDrive','PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow', 'PromoEvnt_22TikTok', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23RollingStone']

dummy_vars = ['dummy_20220711']

##### Media Vars #####
best_corr_media_vars = ['Affiliate_Imps_AdStock6L5Wk90Ad_Power90', 'DMP_Imps_AdStock13L1Wk30Ad_Power90', 'Radio_Spend_AdStock13L1Wk30Ad_Power30', 'OOH_Imps_AdStock13L1Wk30Ad_Power30', 'Print_Imps_AdStock13L1Wk30Ad_Power30']

prev_model_media_vars = ['AltMedia_Imps_AdStock6L1Wk90Ad_Power90', 'Audio_Imps_AdStock6L1Wk90Ad_Power30', 'DirectMail_Imps_AdStock6L1Wk90Ad_Power70', 'Display_Imps_AdStock6L1Wk30Ad_Power30', 'Email_Imps_AdStock6L1Wk80Ad_Power30', 'LeadGen_Imps_AdStock6L1Wk90Ad_Power90', 'Search_Clicks_AdStock6L1Wk80Ad_Power30', 'Social_Imps_AdStock6L1Wk30Ad_Power30', 'TV_Imps_AdStock6L1Wk90Ad_Power30', 'Video_Imps_AdStock6L1Wk30Ad_Power30']

media_vars = best_corr_media_vars+prev_model_media_vars 


Regularization = False

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.3 Model Creation with priors from previous model Vo3: Dropping Radio, Audio, Affiliate, OOH, Print, DMP

# COMMAND ----------

## Model Creation ##
seasoanlity_vars =  ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6'] # Double Check on Fourier Frequencies 

Holiday_vars = ['Holiday_22Christams', 'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'Holiday_23Christams']

Promo_vars = ['PromoEvnt_22MembDrive','PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow', 'PromoEvnt_22TikTok', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23RollingStone']

dummy_vars = ['dummy_20220711']

##### Media Vars #####
best_corr_media_vars = []

prev_model_media_vars = ['AltMedia_Imps_AdStock6L1Wk90Ad_Power90', 'DirectMail_Imps_AdStock6L1Wk90Ad_Power70', 'Display_Imps_AdStock6L1Wk30Ad_Power30', 'Email_Imps_AdStock6L1Wk80Ad_Power30', 'LeadGen_Imps_AdStock6L1Wk90Ad_Power90', 'Search_Clicks_AdStock6L1Wk80Ad_Power30', 'Social_Imps_AdStock6L1Wk30Ad_Power30', 'TV_Imps_AdStock6L1Wk90Ad_Power30', 'Video_Imps_AdStock6L1Wk30Ad_Power30']

media_vars = best_corr_media_vars+prev_model_media_vars 


Regularization = False

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.4 Model Creation with priors from previous model Vo4: Taking Best Configurations of other vars (Dropping Radio, Audio, Affiliate, OOH, Print, DMP)

# COMMAND ----------

## Model Creation ##
seasoanlity_vars =  ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6'] # Double Check on Fourier Frequencies 

Holiday_vars = ['Holiday_22Christams', 'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'Holiday_23Christams']

Promo_vars = ['PromoEvnt_22MembDrive','PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow', 'PromoEvnt_22TikTok', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23RollingStone']

dummy_vars = ['dummy_20220711']

##### Media Vars #####
best_corr_media_vars = []

prev_model_media_vars = ['Search_Clicks_AdStock6L1Wk80Ad_Power30','Email_Imps_AdStock6L1Wk80Ad_Power30', 'LeadGen_Imps_AdStock6L1Wk90Ad_Power90','AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
'DirectMail_Imps_AdStock6L4Wk90Ad_Power70', 'Display_Imps_AdStock6L4Wk30Ad_Power30', 'Social_Imps_AdStock6L4Wk30Ad_Power30', 'TV_Imps_AdStock6L1Wk90Ad_Power30', 'Video_Imps_AdStock6L2Wk30Ad_Power30']

media_vars = best_corr_media_vars+prev_model_media_vars 

Regularization = False

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.5 Model Creation with priors from previous model Vo5: Dropping Display from Vo4

# COMMAND ----------

## Model Creation ##
seasoanlity_vars =  ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6'] # Double Check on Fourier Frequencies 

Holiday_vars = ['Holiday_22Christams', 'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'Holiday_23Christams']

Promo_vars = ['PromoEvnt_22MembDrive','PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow', 'PromoEvnt_23RollingStone'] #, 'PromoEvnt_22TikTok', 'PromoEvnt_23KaylaCoupon'

dummy_vars = ['dummy_20220711', 'Peak_20220228', 'dummy_20221010', 'Peak_20221128', 'Peak_20230227', 'Peak_20231127']

##### Media Vars #####
best_corr_media_vars = []

prev_model_media_vars = ['Search_Clicks_AdStock6L1Wk80Ad_Power30','Email_Imps_AdStock6L1Wk80Ad_Power30', 'LeadGen_Imps_AdStock6L1Wk90Ad_Power90','AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
'DirectMail_Imps_AdStock6L4Wk90Ad_Power70', 'Social_Imps_AdStock6L4Wk30Ad_Power30', 'TV_Imps_AdStock6L1Wk90Ad_Power30', 'Video_Imps_AdStock6L2Wk30Ad_Power30']

media_vars = best_corr_media_vars+prev_model_media_vars 


Regularization = False

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.6 Model Vo6: L1 Regualrizing Vo4

# COMMAND ----------

# ## Model Creation ##
# seasoanlity_vars =  ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6'] # Double Check on Fourier Frequencies 

# Holiday_vars = ['Holiday_22Christams', 'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'Holiday_23Christams']

# Promo_vars = ['PromoEvnt_22MembDrive','PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow', 'PromoEvnt_22TikTok', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23RollingStone']

# dummy_vars = ['dummy_20220711']

# ##### Media Vars #####
# best_corr_media_vars = []

# prev_model_media_vars = ['Search_Clicks_AdStock6L1Wk80Ad_Power30', 'Email_Imps_AdStock6L1Wk80Ad_Power30', 'LeadGen_Imps_AdStock6L1Wk90Ad_Power90', 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
# 'DirectMail_Imps_AdStock6L4Wk90Ad_Power70', 'Display_Imps_AdStock6L4Wk30Ad_Power30', 'Social_Imps_AdStock6L4Wk30Ad_Power30','TV_Imps_AdStock6L1Wk90Ad_Power30','Video_Imps_AdStock6L2Wk30Ad_Power30']

# media_vars = best_corr_media_vars+prev_model_media_vars 


# ## Regualrization Coeff. ##
# reg_coeff = list({
# 'Intercept':0, 'Seasonality_Sin2' : 0, 'Seasonality_Cos2' : 0, 'Seasonality_Sin4' : 0, 'Seasonality_Cos4' : 0, 'Seasonality_Sin6' : 0, 'Seasonality_Cos6' : 0,
# 'Holiday_22Christams' : 0, 'Holiday_23MemorialDay' : 0, 'Holiday_23LaborDay' : 0, 'Holiday_23Christams' : 0,
# 'PromoEvnt_22MembDrive' : 0, 'PromoEvnt_22MemorialDay' : 0, 'PromoEvnt_22LaborDay' : 0, 'PromoEvnt_22BlackFriday' : 0, 'PromoEvnt_22WsjTodayShow' : 0,
# 'PromoEvnt_22TikTok' : 0, 'PromoEvnt_23KaylaCoupon' : 0, 'PromoEvnt_23RollingStone' : 0, 'dummy_20220711' : 0,

# 'Search_Clicks_AdStock6L1Wk80Ad_Power30' : 4, 
# 'Email_Imps_AdStock6L1Wk80Ad_Power30' : 10,
# 'LeadGen_Imps_AdStock6L1Wk90Ad_Power90' : 6, 
# 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90' : 0,
# 'DirectMail_Imps_AdStock6L4Wk90Ad_Power70' : 4, 
# 'Display_Imps_AdStock6L4Wk30Ad_Power30' : 2, 
# 'Social_Imps_AdStock6L4Wk30Ad_Power30' : 0, 
# 'TV_Imps_AdStock6L1Wk90Ad_Power30' : 0,
# 'Video_Imps_AdStock6L2Wk30Ad_Power30' : 4,



# 'DmaCode Var':0
#   }.values())


# Regularization = True

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.7 Model Vo7: L2 regualrization on Vo4

# COMMAND ----------

# ## Model Creation ##
# seasoanlity_vars =  ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6'] # Double Check on Fourier Frequencies 

# Holiday_vars = ['Holiday_22Christams', 'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'Holiday_23Christams']

# Promo_vars = ['PromoEvnt_22MembDrive','PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow', 'PromoEvnt_22TikTok', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23RollingStone']

# dummy_vars = ['dummy_20220711']

# ##### Media Vars #####
# best_corr_media_vars = []

# prev_model_media_vars = ['Search_Clicks_AdStock6L1Wk80Ad_Power30', 'Email_Imps_AdStock6L1Wk80Ad_Power30', 'LeadGen_Imps_AdStock6L1Wk90Ad_Power90', 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
# 'DirectMail_Imps_AdStock6L4Wk90Ad_Power70', 'Display_Imps_AdStock6L4Wk30Ad_Power30', 'Social_Imps_AdStock6L4Wk30Ad_Power30','TV_Imps_AdStock6L1Wk90Ad_Power30','Video_Imps_AdStock6L2Wk30Ad_Power30']

# split_media_vars = []

# media_vars = best_corr_media_vars+prev_model_media_vars+split_media_vars 

# ## Regualrization Coeff. ##
# reg_coeff = list({
# 'Intercept':0, 

# 'Seasonality_Sin2' : 0, 'Seasonality_Cos2' : 0, 'Seasonality_Sin4' : 0, 'Seasonality_Cos4' : 0, 'Seasonality_Sin6' : 0, 'Seasonality_Cos6' : 0,
# 'Holiday_22Christams' : 0, 'Holiday_23MemorialDay' : 0, 'Holiday_23LaborDay' : 0, 'Holiday_23Christams' : 0,
# 'PromoEvnt_22MembDrive' : 0, 'PromoEvnt_22MemorialDay' : 0, 'PromoEvnt_22LaborDay' : 0, 'PromoEvnt_22BlackFriday' : 0, 'PromoEvnt_22WsjTodayShow' : 0,
# 'PromoEvnt_22TikTok' : 0, 'PromoEvnt_23KaylaCoupon' : 0, 'PromoEvnt_23RollingStone' : 0, 'dummy_20220711' : 0,

# 'Search_Clicks_AdStock6L1Wk80Ad_Power30' : 0, 
# 'Email_Imps_AdStock6L1Wk80Ad_Power30' : 0,
# 'LeadGen_Imps_AdStock6L1Wk90Ad_Power90' : 0, 
# 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90' : 0,
# 'DirectMail_Imps_AdStock6L4Wk90Ad_Power70' : 0, 
# 'Display_Imps_AdStock6L4Wk30Ad_Power30':6,
# 'Social_Imps_AdStock6L4Wk30Ad_Power30' : 0, 
# 'TV_Imps_AdStock6L1Wk90Ad_Power30' : 0,
# 'Video_Imps_AdStock6L2Wk30Ad_Power30' : 0,



# # 'DmaCode Var':0
#   }.values())

# ## converting Reg Coeff into np array of type float64 ##
# reg_coeff = np.asarray(reg_coeff, dtype='float64')



# Regularization = False

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.8 Model Vo8: Dropped Display and Video from Vo4. Grid search for Social

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay',
       'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday',
       'PromoEvnt_22WsjTodayShow', 'PromoEvnt_23RollingStone',

       'Email_Imps_AdStock6L1Wk80Ad_Power30',
       'LeadGen_Imps_AdStock6L1Wk90Ad_Power90',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
       'DirectMail_Imps_AdStock6L4Wk90Ad_Power70',
       'TV_Imps_AdStock6L1Wk90Ad_Power30',
       'Search_Clicks_AdStock13L1Wk90Ad_Power90',
       'Social_Imps_AdStock6L3Wk90Ad_Power80']





input_vars_str = " + ".join(input_vars)

Regularization = False

## Model Fit ##

if Regularization:
  assert len(reg_coeff) - len(input_vars) == 1
  print("########### Running Regularized Model ###########")
  model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit_regularized(method = L2(), alpha=100)
else:
  print("@@@@@@@@@@@ Running Un-Regularized Model @@@@@@@@@@@")
  model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.9 Model Vo9: Added Dummies except for  'Peak_20221128' 

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay',
       'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday',
       'PromoEvnt_22WsjTodayShow', 'PromoEvnt_23RollingStone',


       'dummy_20220711','Peak_20220228', 'Peak_20230227',
      'dummy_20221010','Peak_20231127',

       'Email_Imps_AdStock6L1Wk80Ad_Power30',
       'LeadGen_Imps_AdStock6L1Wk90Ad_Power90',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
       'DirectMail_Imps_AdStock6L4Wk90Ad_Power70',
       'TV_Imps_AdStock6L1Wk90Ad_Power30',
       'Search_Clicks_AdStock13L1Wk90Ad_Power90',
       'Social_Imps_AdStock6L3Wk90Ad_Power80']





input_vars_str = " + ".join(input_vars)

Regularization = False

## Model Fit ##

if Regularization:
  assert len(reg_coeff) - len(input_vars) == 1
  print("########### Running Regularized Model ###########")
  model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit_regularized(method = L2(), alpha=100)
else:
  print("@@@@@@@@@@@ Running Un-Regularized Model @@@@@@@@@@@")
  model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.10 Model Vo10: Added all Dummies and removed Email. Exhaustive Grid Search for Social

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay',
       'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday',
       'PromoEvnt_22WsjTodayShow', 'PromoEvnt_23RollingStone',

       'dummy_20220711','Peak_20220228', 'Peak_20230227',
        'dummy_20221010','Peak_20231127',
         'Peak_20221128', 


       'LeadGen_Imps_AdStock6L1Wk90Ad_Power90',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
       'DirectMail_Imps_AdStock6L4Wk90Ad_Power70',
       'TV_Imps_AdStock6L1Wk90Ad_Power30',
       'Search_Clicks_AdStock13L1Wk90Ad_Power90',
       'Social_Imps_AdStock13L3Wk70Ad_Power90'
       ]





input_vars_str = " + ".join(input_vars)

Regularization = False

## Model Fit ##

if Regularization:
  assert len(reg_coeff) - len(input_vars) == 1
  print("########### Running Regularized Model ###########")
  model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit_regularized(method = L2(), alpha=100)
else:
  print("@@@@@@@@@@@ Running Un-Regularized Model @@@@@@@@@@@")
  model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.11 Model Vo11: On top of Vo10: Trying Transformations suggested by Joe

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay',
       'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday',
       'PromoEvnt_22WsjTodayShow', 'PromoEvnt_23RollingStone',

       'dummy_20220711',
       'Peak_20220228','Peak_20230227',
       'dummy_20221010','Peak_20231127',
       'Peak_20221128', 


       'LeadGen_Imps_AdStock6L1Wk90Ad_Power30',
       'AltMedia_Imps_AdStock6L2Wk90Ad_Power50', #'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk60Ad_Power60',
       'Search_Clicks_AdStock6L1Wk90Ad_Power30', #'Search_Clicks_AdStock13L1Wk90Ad_Power90',
       'Social_Imps_AdStock6L3Wk90Ad_Power90'
       ]





input_vars_str = " + ".join(input_vars)

Regularization = False

## Model Fit ##

if Regularization:
  assert len(reg_coeff) - len(input_vars) == 1
  print("########### Running Regularized Model ###########")
  model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit_regularized(method = L2(), alpha=100)
else:
  print("@@@@@@@@@@@ Running Un-Regularized Model @@@@@@@@@@@")
  model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.12 Model Vo12: Best from 6/26/2024

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 
              # 'PromoEvnt_22LaborDay',
              
       'PromoEvnt_22BlackFriday', 
       'PromoEvnt_23RollingStone',
       'dummy_20220711', 
       'LeadGen_Imps_AdStock6L1Wk90Ad_Power30',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power50',
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
       'Search_Clicks_AdStock6L1Wk80Ad_Power70',
       'Social_Imps_AdStock6L1Wk60Ad_Power90'

       ]





input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.13 Model Vo12: Best from 6/2_/2024

# COMMAND ----------

input_vars = ['PromoEvnt_22MembDrive', 'PromoEvnt_22BlackFriday',
       'PromoEvnt_23RollingStone',
       'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
       'TV_Imps_AdStock6L2Wk70Ad_Power70',
       'Search_Clicks_AdStock6L1Wk80Ad_Power70',
       'Social_Imps_AdStock6L1Wk60Ad_Power90',
       'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
       'AltMedia_Imps_AdStock6L3Wk90Ad_Power90' ]

input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization       

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.14 Model Vo13: Best of 7/1/2024

# COMMAND ----------

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

'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'

         
       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.14 Model Vo14: Introducing Email and removing Social

# COMMAND ----------

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
# 'Social_Imps_AdStock6L1Wk90Ad_Power90',

'LeadGen_Imps_AdStock6L1Wk90Ad_Power90', #'LeadGen_Imps_AdStock6L1Wk30Ad_Power30',
'Email_Imps_AdStock6L1Wk90Ad_Power90', # 'Email_Imps_AdStock6L1Wk30Ad_Power30'

'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90'

         
       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.15 Model Vo15: Best of Email

# COMMAND ----------

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
'LeadGen_Imps_AdStock6L1Wk90Ad_Power90',
'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
'Email_Imps_AdStock6L1Wk30Ad_Power90'

         
       ]



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.16 Model Vo16: Bringing in Vo14 and adding Email into it

# COMMAND ----------

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



input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Joins_norm ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode').fit() ## Min-Max Normalization

# COMMAND ----------

#Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

## Saving Model ##
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_prior.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo3.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo4.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo5.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo6.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo7.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo8.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo9.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo10.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo12.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo13.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo14.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo15.pkl'
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo16.pkl'
model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo16_updatedData.pkl'
with open(model_path,"wb") as f:
  pickle.dump(model, f)


# ## Reading Model ##
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo16.pkl'
# with open(model_path, "rb") as f:
#   model = pickle.load(f)

# COMMAND ----------

## Creating Model Predict ##
joins = model_df[['DmaCode', 'Date','Joins_norm']]
pred = model.fittedvalues.rename('Pred')

if 'Joins' not in joins.columns:
  joins.rename(columns = {joins.columns[-1]:'Joins'},inplace=True)

pred_df = pd.concat([joins, pred],axis=1)

pred_df = pd.concat([joins, pred],axis=1)

# Aggregate the data by 'Date'
pred_df_date = pred_df.groupby('Date').agg({'Joins':'sum', 'Pred':'sum'}).reset_index()

## Plots ##
plt.figure(figsize=(25,6))
plt.plot( pred_df_date.Joins, label='Actual')
plt.plot( pred_df_date.Pred, label='Predicted')

plt.axvline(x=0, color='r')
plt.axvline(x=51, color='r')
plt.axvline(x=103, color='r')

# Calculate and display residuals
residuals = pred_df_date.Joins - pred_df_date.Pred
# Create a figure with two subplots (1 row, 2 columns)
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(25, 8))

# Plot residuals over time
ax1.scatter(pred_df_date.Date, residuals, label='Residuals')
ax1.axhline(y=0, color='r', linestyle='--')
ax1.set_xlabel('Date')
ax1.set_ylabel('Residuals')
ax1.set_title('Residuals Over Time')

ax1.axvline(x=0, color='r')
ax1.axvline(x=51, color='r')
ax1.axvline(x=103, color='r')



ax1.legend()

# Plot histogram of residuals
ax2.hist(residuals, bins=30, edgecolor='black')
ax2.set_xlabel('Residuals')
ax2.set_ylabel('Frequency')
ax2.set_title('Histogram of Residuals')


# Calculate and display mean squared error (MSE) and R-squared
mape = mean_absolute_percentage_error(pred_df_date.Joins, pred_df_date.Pred)
r_squared = r2_score(pred_df_date.Joins, pred_df_date.Pred)
skewness = skew(residuals)
kurt = kurtosis(residuals)
dw_stat = sm.stats.stattools.durbin_watson(residuals)
jb_test = jarque_bera(residuals)


print(f"Mean Absolute Percentage Error (MAPE): {mape}")
print(f"R-squared: {r_squared}")
print(f"Skewness: {skewness}")
print(f"Kurtosis: {kurt}")
print(f"Jarque Bera: {jb_test}")
print(f"DW Stat: {dw_stat}")
print(f"Condition Number: {np.linalg.cond(model.model.exog)}")


plt.tight_layout()
plt.show()

# COMMAND ----------

#Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

random_effect_dict = {key:value.values[0]+model.fe_params['Intercept'] for key,value in model.random_effects.items()}
len([k for k,v in random_effect_dict.items() if v<0])

# COMMAND ----------

## VIF ##
X = model_df[input_vars]
# X = sm.add_constant(X) ## Check if there is no intercept

## Creating 
vif_data = pd.DataFrame()
vif_data["feature"] = X.columns
vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]

vif_data.round(2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 4.0 Analyzing Residuals 

# COMMAND ----------

## Getting Residuals ##
pred_df_date['Residuals'] = pred_df_date['Joins'] - pred_df_date['Pred']
pred_df_date.display()

# COMMAND ----------

plt.figure(figsize=(20,8))
plt.plot(pred_df_date['Residuals'])

plt.axhline(y=0, color='r', linestyle='--')
plt.axvline(x=51, color='r')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 ACF and PACF plots for Residuals

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

# Assuming your DataFrame is named model_df and the time series column is named 'time_series_column'
time_series = pred_df_date['Residuals']

fig, ax = plt.subplots(2, 1, figsize=(20, 8))

# Plot ACF
plot_acf(time_series, ax=ax[0], lags=60, alpha=0.05)
ax[0].set_title('Autocorrelation Function (ACF)')

# Plot PACF
plot_pacf(time_series, ax=ax[1], lags=40, method='ywm')
ax[1].set_title('Partial Autocorrelation Function (PACF)')

plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Test for Stationarity of Residuals

# COMMAND ----------

import pandas as pd
from statsmodels.tsa.stattools import adfuller, kpss

# Assuming you have a DataFrame 'df' with a column 'residuals'
residuals = pred_df_date['Residuals']

# Augmented Dickey-Fuller test     
'''
The null hypothesis of the ADF test is that the series has a unit root (i.e., it is non-stationary).

'''
adf_result = adfuller(residuals)
print('ADF Statistic:', adf_result[0])
print('p-value:', adf_result[1])
for key, value in adf_result[4].items():
    print('Critical Value (%s): %.3f' % (key, value))

# Kwiatkowski-Phillips-Schmidt-Shin test

'''

The null hypothesis of the KPSS test is that the series is stationary.

'''



kpss_result = kpss(residuals, regression='c')
print('\nKPSS Statistic:', kpss_result[0])
print('p-value:', kpss_result[1])
for key, value in kpss_result[3].items():
    print('Critical Value (%s): %.3f' % (key, value))


# COMMAND ----------

# MAGIC %md
# MAGIC # AdHoc Section

# COMMAND ----------

## Data ##
# analysis_df = model_df.drop(columns = ['DmaCode']).groupby('Date').sum().reset_index()

# COMMAND ----------

# ## Plots ##
# fig, ax1 = plt.subplots(1,1, figsize = (25,6))

# ax1.plot( pred_df_date.Joins.values, label='Actual')
# ax1.plot( pred_df_date.Pred.values, label='Predicted')


# ax2 = ax1.twinx()
# # ax2.plot(analysis_df['Seasonality_Sin5'][:].values, linestyle= '--', color ='g', label = 'Sin Wave') ## Phased Out totally
# # ax2.plot(analysis_df['Seasonality_Cos5'][:].values, linestyle= '--', color ='r', label = 'Cos Wave')

# # ax2.plot(analysis_df['Seasonality_Sin6'][:].values, linestyle= '--', color =(0.8 , 0.2,0.5), label = 'Sin Wave') ## Not capturing early year seasonlaity
# ax2.plot(analysis_df['Seasonality_Cos6'][:].values, linestyle= '--', color =(1 , 0.2,1), label = 'Cos Wave')

# ax2.set_ylim(3500,-3500)
# ax2.legend()


# ax1.axvline(x=0, color='r')
# ax1.axvline(x=51, color='r')
# ax1.axvline(x=103, color='r')

# COMMAND ----------

# vars = ['TV_Imps_AdStock6L2Wk60Ad_Power60', 'Search_Clicks_AdStock6L1Wk90Ad_Power30', 'Social_Imps_AdStock6L2Wk90Ad_Power90']
# analysis_df = model_df.groupby(['Date', 'DmaCode']+vars).sum().reset_index()

# # Increase overall figure size
# p
# sns.pairplot(analysis_df[vars])

# COMMAND ----------

# model_df[vars].corr()

# COMMAND ----------

# MAGIC %md
# MAGIC # 5.0 Saving Contribution File 

# COMMAND ----------

# while True:
#   var=1

# COMMAND ----------



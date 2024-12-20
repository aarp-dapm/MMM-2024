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

# MAGIC %md
# MAGIC ### 3.0 Loading Frequentist Model

# COMMAND ----------

## Reading Model ##
model_path =  '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_prev_model_priorVo16_updatedData.pkl'

with open(model_path, "rb") as f:
  model = pickle.load(f)

#Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1 Best Model: Frequentist Approach

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

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2 Bayesian Model

# COMMAND ----------

## Definig Some Utilities 
RANDOM_SEED = 12345


DmaCodes, mn_DmaCodes = model_df.DmaCode.factorize()
coords = {'DmaCodes': mn_DmaCodes}


# COMMAND ----------

with pm.Model(coords=coords) as varying_intercept:
       
       ################
       ## Input Data ##
       ################

       dma_idx = pm.MutableData('dma_idx', DmaCodes, dims='obs_id')

       ## Media Variables ##
       tv_idx = pm.MutableData('tv_idx', model_df['TV_Imps_AdStock6L2Wk70Ad_Power70'].values, dims='obs_id')
       search_idx = pm.MutableData('search_idx', model_df['Search_Clicks_AdStock6L1Wk90Ad_Power90'].values, dims='obs_id') 
       social_idx = pm.MutableData('social_idx', model_df['Social_Imps_AdStock6L1Wk90Ad_Power90'].values, dims='obs_id')

       leadgen_idx = pm.MutableData('leadgen_idx', model_df['LeadGen_Imps_AdStock6L1Wk60Ad_Power80'].values, dims='obs_id')
       email_idx = pm.MutableData('email_idx', model_df['Email_Spend_AdStock6L1Wk20Ad_Power90'].values, dims='obs_id')

       dm_idx = pm.MutableData('dm_idx', model_df['DirectMail_Imps_AdStock6L3Wk70Ad_Power90'].values, dims='obs_id')
       AltMedia_idx = pm.MutableData('AltMedia_idx', model_df['AltMedia_Imps_AdStock6L3Wk90Ad_Power90'].values, dims='obs_id')



       ## Promotions and Seasonality  ##

       PromoEvnt_22MembDrive_idx = pm.MutableData('PromoEvnt_22MembDrive_idx', model_df['PromoEvnt_22MembDrive_Eng'].values, dims='obs_id')
       PromoEvnt_22BlackFriday_idx = pm.MutableData('PromoEvnt_22BlackFriday_idx', model_df['PromoEvnt_22BlackFriday_Eng'].values, dims='obs_id')

       PromoEvnt_23MembDrive_idx = pm.MutableData('PromoEvnt_23embDrive_idx', model_df['PromoEvnt_23MembDrive_Eng'].values, dims='obs_id')
       PromoEvnt_23BlackFriday_idx = pm.MutableData('PromoEvnt_23BlackFriday_idx', model_df['PromoEvnt_23BlackFriday_Eng'].values, dims='obs_id')
       

       Seasonality_Feb_idx = pm.MutableData('Seasonality_Feb_idx', model_df['Seasonality_Feb'].values, dims='obs_id')
       Seasonality_Mar_idx = pm.MutableData('Seasonality_Mar_idx', model_df['Seasonality_Mar'].values, dims='obs_id')
       Seasonality_Apr_idx = pm.MutableData('Seasonality_Apr_idx', model_df['Seasonality_Apr'].values, dims='obs_id')
       Seasonality_Dec_idx = pm.MutableData('Seasonality_Dec_idx', model_df['Seasonality_Feb'].values, dims='obs_id')

       ## Other Flags ##
       July0422_lag_idx = pm.MutableData('July0422_lag_idx', model_df['July0422_lag'].values, dims='obs_id')



       #############################
       ## Setup Data Distribution ##
       #############################

       ## Random Effect ##

       ### Priors ###
       mu_re = pm.Normal('mu_re', mu=0, sigma=10)
       sigma_re = pm.Exponential('sigma_re',1)

       intercept_re = pm.Normal('intercept_re', mu=mu_re, sigma=sigma_re, dims='DmaCodes')

       ## Fixed Effect ##
       
       ### Media Variables ###
       beta_tv =    pm.Normal('beta_tv', mu= 0.037,sigma=0.005)           #  pm.Beta('beta_tv', alpha=1.5, beta=10) 
       beta_search =  pm.Normal('beta_search', mu= 0.085,sigma=0.005)      #  pm.Beta('beta_search', alpha=1.5, beta=10)
       beta_social =  pm.Normal('beta_social', mu= 0.029,sigma=0.007)      #  pm.Beta('beta_social', alpha=1.5, beta=10)
       
       beta_leadgen =  pm.Normal('beta_leadgen', mu= 0.21,sigma=0.007)    #  pm.Beta('beta_leadgen', alpha=3, beta=10)  
       beta_email = pm.Normal('beta_email', mu= 0.059,sigma=0.006)         #  pm.Beta('beta_email', alpha=1.5, beta=10)

       beta_dm =   pm.Normal('beta_dm', mu= 0.082,sigma=0.005)             #  pm.Beta('beta_dm', alpha=1.5, beta=10)
       beta_AltMedia = pm.Normal('beta_AltMedia', mu= 0.084,sigma=0.004)  # pm.Beta('beta_AltMedia', alpha=1.5, beta=10)

       ## Promotions and Seasonality  ##
       beta_PromoEvnt_22MembDrive = pm.Normal('beta_PromoEvnt_22MembDrive', mu= 0.257,sigma=0.011) #  pm.Beta('beta_PromoEvnt_22MembDrive', alpha=3, beta=10)
       beta_PromoEvnt_22BlackFriday = pm.Normal('beta_PromoEvnt_22BlackFriday', mu= 0.416,sigma=0.011)  # pm.Beta('beta_PromoEvnt_22BlackFriday', alpha=7, beta=10)

       beta_PromoEvnt_23MembDrive = pm.Normal('beta_PromoEvnt_23MembDrive', mu= 0.306,sigma=0.007)  #  pm.Beta('beta_PromoEvnt_23MembDrive', alpha=4, beta=10)
       beta_PromoEvnt_23BlackFriday = pm.Normal('beta_PromoEvnt_23BlackFriday', mu= 0.370,sigma=0.01)  # pm.Beta('beta_PromoEvnt_23BlackFriday', alpha=6, beta=10)


       beta_Seasonality_Feb = pm.Normal('beta_Seasonality_Feb', mu= 0.117,sigma=0.004)       
       beta_Seasonality_Mar = pm.Normal('beta_Seasonality_Mar', mu= 0.086,sigma=0.004)       
       beta_Seasonality_Apr = pm.Normal('beta_Seasonality_Apr', mu= -0.038,sigma=0.004)       
       beta_Seasonality_Dec = pm.Normal('beta_Seasonality_Dec', mu= -0.128,sigma=0.004)       
       
       
       ### Other Flags ###
       beta_July0422_lag = pm.Beta('beta_July0422_lag', alpha=3, beta=10) #pm.Normal('beta_July0422_lag', mu= 0.316,sigma=1)




       #################
       ## Setup Model ##
       #################


       ## Setup Error Model ##
       sd_y = pm.Normal("sd_y", mu=0, sigma = 0.2)

       ## Setup Expected Value ##
       y_hat = intercept_re[dma_idx] + beta_tv*tv_idx + beta_search*search_idx + beta_social*social_idx + beta_leadgen*leadgen_idx + beta_email*email_idx + beta_dm*dm_idx + beta_AltMedia*AltMedia_idx + beta_PromoEvnt_22MembDrive*PromoEvnt_22MembDrive_idx + beta_PromoEvnt_22BlackFriday*PromoEvnt_22BlackFriday_idx + beta_PromoEvnt_23MembDrive*PromoEvnt_23MembDrive_idx + beta_PromoEvnt_23BlackFriday*PromoEvnt_23BlackFriday_idx + beta_Seasonality_Mar*Seasonality_Mar_idx + beta_Seasonality_Apr*Seasonality_Apr_idx + beta_Seasonality_Feb*Seasonality_Feb_idx + beta_Seasonality_Dec*Seasonality_Dec_idx  + beta_July0422_lag*July0422_lag_idx

       ## Setup Liklehood function ##
       y_like = pm.Normal('y_like', mu= y_hat, sigma=sd_y, observed=model_df['Joins_norm'], dims='obs_id')




# COMMAND ----------

## Running Bayesian Machinery ##
with varying_intercept:
  varying_intercept_trace = pm.sample(draws=3000,tune=100, chains=4, random_seed = RANDOM_SEED, progressbar=True)

# COMMAND ----------

# Model Summary #
summary_df = az.summary(varying_intercept_trace, round_to=2).reset_index()
summary_df.display()

# COMMAND ----------

# Plot Posterior Distribution #
pm.plot_trace(varying_intercept_trace)

# COMMAND ----------

### All Model Variables ###
map_dict = {

'beta_July0422_lag':'July0422_lag',

'beta_PromoEvnt_22BlackFriday':'PromoEvnt_22BlackFriday_Eng',
'beta_PromoEvnt_22MembDrive':'PromoEvnt_22MembDrive_Eng',
'beta_PromoEvnt_23BlackFriday':'PromoEvnt_23BlackFriday_Eng',
'beta_PromoEvnt_23MembDrive':'PromoEvnt_23MembDrive_Eng',

'beta_Seasonality_Feb':'Seasonality_Feb',
'beta_Seasonality_Apr':'Seasonality_Apr',
'beta_Seasonality_Mar':'Seasonality_Mar',
'beta_Seasonality_Dec':'Seasonality_Dec',

'beta_dm': 'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'beta_AltMedia':'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
'beta_email':'Email_Spend_AdStock6L1Wk20Ad_Power90',
'beta_leadgen':'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'beta_search':'Search_Clicks_AdStock6L1Wk90Ad_Power90',
'beta_social':'Social_Imps_AdStock6L1Wk90Ad_Power90',
'beta_tv': 'TV_Imps_AdStock6L2Wk70Ad_Power70',



}


### Geting Fixed Effects Variables ##
fe_coeff_df = summary_df[summary_df['index'].isin(map_dict.keys())]
re_coeff_df = summary_df[summary_df['index'].str.contains('intercept_re')]

### Geting Fixed Effects into Df ##
fe_coeff_df['input_vars'] = fe_coeff_df['index'].map(map_dict)

### Creating map for Random effect ###
re_coeff_df['DmaCode'] = re_coeff_df['index'].str.split('[').str[1].str.split(']').str[0]
re_coeff_df['DmaCode'] = re_coeff_df['DmaCode'].astype('int64')
re_dict =  re_coeff_df.set_index('DmaCode')['mean'].to_dict()



### Creating Contribution data ###
input_vars = list(fe_coeff_df['input_vars'].unique()) 


## Adding Variable Contribution ##
coeff_value = fe_coeff_df['mean'].values
coeff_names = fe_coeff_df['input_vars'].values

input_model = model_df[input_vars].values
## Variable contribution##
contribution = pd.DataFrame(coeff_value*input_model, columns=coeff_names)
contribution = pd.concat([model_df[['DmaCode', 'Date', 'Joins', 'Joins_norm']], contribution], axis=1)

contribution['Random Intercept'] = contribution['DmaCode'].map(re_dict)
contribution.insert(4, 'Pred', contribution[input_vars + ['Random Intercept'] ].sum(axis=1))

contribution['Fixed Dma Effect'] = contribution['Random Intercept'].mean()
contribution['Random Dma Effect'] = contribution['Random Intercept'] - contribution['Fixed Dma Effect']

contribution['Residuals'] = contribution['Joins_norm'] - contribution['Pred']
contribution['ratio_resid_join'] =  contribution['Residuals']/contribution['Joins_norm']
contribution['Year'] = pd.to_datetime(contribution['Date']).dt.year

## Update name ##
contribution.rename(columns={'Joins':'Joins_unnorm', 'Joins_norm':'Joins'}, inplace=True)

contribution.head()

# COMMAND ----------

analysis_df = contribution.groupby('Date')[['Joins', 'Pred' ]].sum()


## Plots ##
plt.figure(figsize=(25,6))
plt.plot( analysis_df.Joins, label='Actual')
plt.plot( analysis_df.Pred, label='Predicted')

plt.axvline(x=0, color='r')
plt.axvline(x=51, color='r')
plt.axvline(x=103, color='r')

# Calculate and display residuals
residuals = analysis_df.Joins - analysis_df.Pred
# Create a figure with two subplots (1 row, 2 columns)
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(25, 6))

# Plot residuals over time
ax1.scatter(analysis_df.index, residuals, label='Residuals')
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
mape = mean_absolute_percentage_error(analysis_df.Joins, analysis_df.Pred)
r_squared = r2_score(analysis_df.Joins, analysis_df.Pred)
skewness = skew(residuals)
kurt = kurtosis(residuals)
dw_stat = sm.stats.stattools.durbin_watson(residuals)


print(f"Mean Absolute Percentage Error (MAPE): {mape}")
print(f"R-squared: {r_squared}")
print(f"Skewness: {skewness}")
print(f"Kurtosis: {kurt}")
print(f"DW Stat: {dw_stat}")
print(f"Condition Number: {np.linalg.cond(model.model.exog)}")

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregated Analysis Dataframe 

# COMMAND ----------

######################################################################################
##################### Enter List of All Model Variables ##############################
######################################################################################

## Model Creation ##
seasoanlity_vars =  ['Seasonality_Feb', 'Seasonality_Mar', 'Seasonality_Apr', 'Seasonality_Dec']

Holiday_vars = [ 'July0422_lag']

Promo_vars = ['PromoEvnt_22MembDrive_Eng', 'PromoEvnt_22BlackFriday_Eng', 'PromoEvnt_23MembDrive_Eng', 'PromoEvnt_23BlackFriday_Eng']

dummy_vars = []

##### Media Vars #####


media_vars = ['TV_Imps_AdStock6L2Wk70Ad_Power70',
'Search_Clicks_AdStock6L1Wk90Ad_Power90',
'Social_Imps_AdStock6L1Wk90Ad_Power90',


'LeadGen_Imps_AdStock6L1Wk60Ad_Power80',
'Email_Spend_AdStock6L1Wk20Ad_Power90',

'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'AltMedia_Imps_AdStock6L3Wk90Ad_Power90']


######################################################################################
##################### Creating Agg. Analysis Dataframe ###############################
######################################################################################

agg_analysis_list = []

#############################
### Adding Media Variable ###
#############################

for media in media_vars:
  dict_ = {'ChannelLever': 'Membership', 'Channel': media.split("_")[0], 'Support': "_".join(media.split("_")[:2]), 'Transformation': media, 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
  agg_analysis_list.append(dict_)

###############################
### Adding Holiday Variable ###
###############################

for Holiday in Holiday_vars:
  dict_ = {'ChannelLever': 'Holiday', 'Channel': Holiday.split("_")[1], 'Support': Holiday, 'Transformation': Holiday, 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
  agg_analysis_list.append(dict_)


##################################
### Adding Promotions Variable ###
##################################

for Promo in Promo_vars:
  dict_ = {'ChannelLever': 'Promotions', 'Channel': Promo.split("_")[1], 'Support': Promo, 'Transformation': Promo, 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
  agg_analysis_list.append(dict_)

##################################
### Adding Seasoanlity Variable ##
##################################

for season in seasoanlity_vars:
  dict_ = {'ChannelLever': 'Base', 'Channel': 'Seasonal', 'Support': season, 'Transformation': season, 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
  agg_analysis_list.append(dict_)

############################
### Adding Base Variable ###
############################

for dummy in dummy_vars:
  dict_ = {'ChannelLever': 'Base', 'Channel': 'Dummy', 'Support': dummy, 'Transformation': dummy, 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
  agg_analysis_list.append(dict_)

########################
### Adding Intercept ###
########################

## Fixed Effect ##
dict_ = {'ChannelLever': 'Base', 'Channel': 'Fixed Dma Effect', 'Support': 'Fixed Dma Effect', 'Transformation': 'Fixed Dma Effect', 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
agg_analysis_list.append(dict_)

## Random Effect ##
dict_ = {'ChannelLever': 'Base', 'Channel': 'Random Dma Effect', 'Support': 'Random Dma Effect', 'Transformation': 'Random Dma Effect', 'Contrib_2022': 0,  'Contrib_2023': 0, 'Support_2022': 0,  'Support_2023': 0,  'Spend_2022': 0,  'Spend_2023': 0 }
agg_analysis_list.append(dict_)

##########################
### Creating Dataframe ###
##########################

agg_analysis_df = pd.DataFrame(agg_analysis_list)


#########################
### Filling Dataframe ###
#########################

## Adding Contribution ##
agg_analysis_df['Contrib_2022'] = agg_analysis_df.apply(lambda x: calc_contri(contribution, 2022, x['Transformation']), axis=1)
agg_analysis_df['Contrib_2023'] = agg_analysis_df.apply(lambda x: calc_contri(contribution, 2023, x['Transformation']), axis=1)

## Adding Contribution Proportion ##
agg_analysis_df['Contrib_2022_Prop'] = agg_analysis_df['Contrib_2022']/agg_analysis_df['Contrib_2022'].sum()
agg_analysis_df['Contrib_2023_Prop'] = agg_analysis_df['Contrib_2023']/agg_analysis_df['Contrib_2023'].sum() 

## Adding Support ##
agg_analysis_df['Support_2022'] = agg_analysis_df.apply(lambda x: calc_support(model_df, 2022, x['Support']), axis=1)
agg_analysis_df['Support_2023'] = agg_analysis_df.apply(lambda x: calc_support(model_df, 2023, x['Support']), axis=1)

## Adding Spend ##
agg_analysis_df['Spend_2022'] = agg_analysis_df.apply(lambda x: calc_spend(model_df, 2022, x['Channel']+'_Spend'), axis=1)
agg_analysis_df['Spend_2023'] = agg_analysis_df.apply(lambda x: calc_spend(model_df, 2023, x['Channel']+'_Spend'), axis=1)

agg_analysis_df = agg_analysis_df.round(2)
agg_analysis_df

# COMMAND ----------

agg_analysis_df.display()

# COMMAND ----------

re_coeff_df.display()

# COMMAND ----------

while True:
  val=1

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



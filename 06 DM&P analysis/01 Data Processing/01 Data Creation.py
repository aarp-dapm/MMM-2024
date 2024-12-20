# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Loading Data

# COMMAND ----------

## Reaqding Data ##
rawdata_df=spark.sql('select * from temp.ja_blend_mmm2_DateDma_BuyingGroupDf').toPandas()  ## Loading Buying Group Level Data
rawdata_df = rawdata_df.groupby(['Date', 'DmaCode']).sum().reset_index()
rawdata_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0  Data Tranformation 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Defining Functions

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
# MAGIC ## 2.2 Transformation Dictionary

# COMMAND ----------

input_dict ={
"Email_AcqMail_Spend" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Email_LeadGen_Spend" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Email_MediaComOtherEM_Spend" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},

"Radio_MediaComOtherEM_Spend" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},

"Affiliate_IcmDigital_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},

"AltMedia_AltMedia_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},

"Audio_MediaComBrand_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Audio_MediaComOtherEM_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},

"DMP_IcmDigital_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},

"DirectMail_AcqMail_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},

"Display_ASI_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Display_IcmDigital_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Display_MediaComBrand_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Display_MediaComOtherEM_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Display_MembershipDigital_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},


"LeadGen_LeadGen_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},

"OOH_MediaComOtherEM_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},

"Print_MediaComOtherEM_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},


"Social_ASI_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Social_IcmDigital_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Social_IcmSocial_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Social_MediaComBrand_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Social_MediaComOtherEM_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Social_MembershipDigital_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},

"TV_DRTV_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"TV_MediaComBrand_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"TV_MediaComOtherEM_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},

"Video_IcmDigital_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Video_MediaComBrand_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Video_MediaComOtherEM_Imps" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},



"Search_ASI_Clicks" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Search_IcmDigital_Clicks" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Search_MediaComBrand_Clicks" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Search_MediaComOtherEM_Clicks" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
"Search_MembershipDigital_Clicks" : {'Adstock':{'week_cutoff':[6,13], 'peak_week':[1,2,3,4,5], 'decay_rate': list(frange(0.3,0.9,0.1))}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}}
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
# MAGIC ## 2.3 Transformation Process

# COMMAND ----------

# MAGIC %time
# MAGIC
# MAGIC grouped_df = rawdata_df.groupby('DmaCode')[['Date', 'Email_AcqMail_Spend', 'Email_LeadGen_Spend', 'Email_MediaComOtherEM_Spend', 'Radio_MediaComOtherEM_Spend', 'Affiliate_IcmDigital_Imps', 'AltMedia_AltMedia_Imps',
# MAGIC  'Audio_MediaComBrand_Imps', 'Audio_MediaComOtherEM_Imps', 'DMP_IcmDigital_Imps', 'DirectMail_AcqMail_Imps', 'Display_ASI_Imps', 'Display_IcmDigital_Imps', 'Display_MediaComBrand_Imps', 'Display_MediaComOtherEM_Imps',
# MAGIC  'Display_MembershipDigital_Imps', 'LeadGen_LeadGen_Imps', 'OOH_MediaComOtherEM_Imps', 'Print_MediaComOtherEM_Imps', 'Social_ASI_Imps', 'Social_IcmDigital_Imps', 'Social_IcmSocial_Imps', 'Social_MediaComBrand_Imps',
# MAGIC  'Social_MediaComOtherEM_Imps', 'Social_MembershipDigital_Imps', 'TV_DRTV_Imps', 'TV_MediaComBrand_Imps', 'TV_MediaComOtherEM_Imps', 'Video_IcmDigital_Imps', 'Video_MediaComBrand_Imps', 'Video_MediaComOtherEM_Imps',
# MAGIC  'Search_ASI_Clicks', 'Search_IcmDigital_Clicks', 'Search_MediaComBrand_Clicks', 'Search_MediaComOtherEM_Clicks', 'Search_MembershipDigital_Clicks']]
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
# MAGIC ## 2.4 Creating one final Model Dataframe 

# COMMAND ----------

## saving model dataframe ##
# transformed_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_BuyinGroup_Channel.csv')

## Reading Model ##
# transformed_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_BuyinGroup_Channel.csv').drop('Unnamed: 0',axis=1)
# transformed_df['Date'] = pd.to_datetime(transformed_df['Date'])
# transformed_df.head()

# COMMAND ----------

######################################
##### Creating Model Dataframe #######
######################################


################
### Append 1 ###
################

## Reading Previous Dataframe ##
# model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_statsmodel.csv').drop('Unnamed: 0',axis=1)
# model_df['DmaCode'] = model_df['DmaCode'].astype('int')
# model_df['Date'] = pd.to_datetime(model_df['Date'])

## Reading Transformed Data ##
# transformed_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_only_transformed_df_statsmodel_BuyinGroup_Channel.csv').drop('Unnamed: 0',axis=1)
# transformed_df['Date'] = pd.to_datetime(transformed_df['Date'])

## Merging Extended Transformations ##

# main_model_cols = ['PromoEvnt_22MembDrive_Eng', 'PromoEvnt_22BlackFriday_Eng', 'July0422_lag', 'Seasonality_Feb',  'Seasonality_Mar',  'Seasonality_Apr', 'Seasonality_Dec', 'PromoEvnt_23MembDrive_Eng', 
#  'PromoEvnt_23BlackFriday_Eng', 'TV_Imps_AdStock6L2Wk70Ad_Power70', 'TV_Spend', 'Search_Clicks_AdStock6L1Wk90Ad_Power90', 'Search_Spend', 'Social_Imps_AdStock6L1Wk90Ad_Power90', 'Social_Spend', 'LeadGen_Imps_AdStock6L1Wk60Ad_Power80', 'LeadGen_Spend', 'Email_Spend_AdStock6L1Wk20Ad_Power90', 'Email_Spend', 'DirectMail_Imps_AdStock6L3Wk70Ad_Power90', 'DirectMail_Spend', 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',  'AltMedia_Spend']

# primary_cols = ['DmaCode', 'Date']
# target_col = ['Joins']

# MediaModel_col = ['Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps', 'LeadGen_Imps',
#        'Radio_Spend', 'OOH_Imps', 'Print_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps'] 

# HnP_col = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow',
#        'PromoEvnt_22TikTok', 'Holiday_22Christams', 'PromoEvnt_23KaylaCoupon', 
#         'PromoEvnt_23RollingStone', 'Holiday_23Christams'] ## Removed  'PromoEvnt_23EvdaySavCamp' (con) 'Holiday_23MemorialDay', 'Holiday_23LaborDay',

# seasonality_col = ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6']

# Brand_col = ['Index', 'Buzz', 'Impression', 'Quality', 'Value', 'Reputation', 'Satisfaction', 'Recommend',
#        'Awareness', 'Attention', 'Ad_Awareness', 'WOM_Exposure', 'Consideration', 'Purchase_Intent', 'Current_Customer', 'Former_Customer']

# dummy_col = ['dummy_20220711']

# other_col = ['Reg', 'BenRefAll']



# model_df = model_df[primary_cols + target_col + other_col + MediaModel_col + main_model_cols + HnP_col + seasonality_col + Brand_col].merge(transformed_df, on=['DmaCode', 'Date'])



################
### Append 2 ###
################

## Reading Previous Dataframe ##
# model_df = pd.read_csv('').drop('Unnamed: 0',axis=1)
# model_df['DmaCode'] = model_df['DmaCode'].astype('int')
# model_df['Date'] = pd.to_datetime(model_df['Date'])


## Reading Support Data ##
# support_df = rawdata_df[["DmaCode", "Date", "Affiliate_IcmDigital_Spend", "AltMedia_AltMedia_Spend", "Audio_MediaComBrand_Spend", "Audio_MediaComOtherEM_Spend", "DMP_IcmDigital_Spend", "DirectMail_AcqMail_Spend", "Display_ASI_Spend", "Display_IcmDigital_Spend", "Display_MediaComBrand_Spend", "Display_MediaComOtherEM_Spend", "Display_MembershipDigital_Spend", "Email_AcqMail_Spend", "Email_LeadGen_Spend", "Email_MediaComOtherEM_Spend", "LeadGen_LeadGen_Spend", "OOH_MediaComOtherEM_Spend", "Print_MediaComOtherEM_Spend", "Radio_MediaComOtherEM_Spend", "Search_ASI_Spend", "Search_IcmDigital_Spend", "Search_MediaComBrand_Spend", "Search_MediaComOtherEM_Spend", "Search_MembershipDigital_Spend", "Social_ASI_Spend", "Social_IcmDigital_Spend", "Social_IcmSocial_Spend", "Social_MediaComBrand_Spend", "Social_MediaComOtherEM_Spend", "Social_MembershipDigital_Spend", "TV_DRTV_Spend", "TV_MediaComBrand_Spend", "TV_MediaComOtherEM_Spend", "Video_IcmDigital_Spend", "Video_MediaComBrand_Spend", "Video_MediaComOtherEM_Spend", "Affiliate_IcmDigital_Imps", "AltMedia_AltMedia_Imps", "Audio_MediaComBrand_Imps", "Audio_MediaComOtherEM_Imps", "DMP_IcmDigital_Imps", "DirectMail_AcqMail_Imps", "Display_ASI_Imps", "Display_IcmDigital_Imps", "Display_MediaComBrand_Imps", "Display_MediaComOtherEM_Imps", "Display_MembershipDigital_Imps", "Email_AcqMail_Imps", "Email_LeadGen_Imps", "Email_MediaComOtherEM_Imps", "LeadGen_LeadGen_Imps", "OOH_MediaComOtherEM_Imps", "Print_MediaComOtherEM_Imps", "Radio_MediaComOtherEM_Imps", "Search_ASI_Imps", "Search_IcmDigital_Imps", "Search_MediaComBrand_Imps", "Search_MediaComOtherEM_Imps", "Search_MembershipDigital_Imps", "Social_ASI_Imps", "Social_IcmDigital_Imps", "Social_IcmSocial_Imps", "Social_MediaComBrand_Imps", "Social_MediaComOtherEM_Imps", "Social_MembershipDigital_Imps", "TV_DRTV_Imps", "TV_MediaComBrand_Imps", "TV_MediaComOtherEM_Imps", "Video_IcmDigital_Imps", "Video_MediaComBrand_Imps", "Video_MediaComOtherEM_Imps", "Affiliate_IcmDigital_Clicks", "AltMedia_AltMedia_Clicks", "Audio_MediaComBrand_Clicks", "Audio_MediaComOtherEM_Clicks", "DMP_IcmDigital_Clicks", "DirectMail_AcqMail_Clicks", "Display_ASI_Clicks", "Display_IcmDigital_Clicks", "Display_MediaComBrand_Clicks", "Display_MediaComOtherEM_Clicks", "Display_MembershipDigital_Clicks", "Email_AcqMail_Clicks", "Email_LeadGen_Clicks", "Email_MediaComOtherEM_Clicks", "LeadGen_LeadGen_Clicks", "OOH_MediaComOtherEM_Clicks", "Print_MediaComOtherEM_Clicks", "Radio_MediaComOtherEM_Clicks", "Search_ASI_Clicks", "Search_IcmDigital_Clicks", "Search_MediaComBrand_Clicks", "Search_MediaComOtherEM_Clicks", "Search_MembershipDigital_Clicks", "Social_ASI_Clicks", "Social_IcmDigital_Clicks", "Social_IcmSocial_Clicks", "Social_MediaComBrand_Clicks", "Social_MediaComOtherEM_Clicks", "Social_MembershipDigital_Clicks", "TV_DRTV_Clicks", "TV_MediaComBrand_Clicks", "TV_MediaComOtherEM_Clicks", "Video_IcmDigital_Clicks", "Video_MediaComBrand_Clicks", "Video_MediaComOtherEM_Clicks"]]

# support_df['DmaCode'] = support_df['DmaCode'].astype('int')
# support_df['Date'] = pd.to_datetime(support_df['Date'])



## Merging Extended Transformations ##
# model_df = model_df.merge(support_df, on=['DmaCode', 'Date'])




#############################################################
##################### ARCHIVE  ##############################
#############################################################

################
### Append 3 ###
################

## Reading Previous Dataframe ##
# model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_reg_df_MembershipFlag.csv').drop('Unnamed: 0',axis=1)
# model_df['DmaCode'] = model_df['DmaCode'].astype('int')
# model_df['Date'] = pd.to_datetime(model_df['Date'])


## Reading Support Data ##
# support_df = rawdata_df[['DmaCode', 'Date', 'Affiliate_NonMembership_Spend', 'AltMedia_Membership_Spend', 'Audio_NonMembership_Spend', 'DMP_NonMembership_Spend', 'DirectMail_Membership_Spend', 'Display_Membership_Spend', 'Display_NonMembership_Spend', 'LeadGen_Membership_Spend', 'OOH_NonMembership_Spend', 'Print_NonMembership_Spend', 'Radio_NonMembership_Spend', 'Search_Membership_Spend', 'Search_NonMembership_Spend', 'Social_Membership_Spend', 'Social_NonMembership_Spend', 'TV_Membership_Spend', 'TV_NonMembership_Spend', 'Video_NonMembership_Spend']]

# support_df['DmaCode'] = support_df['DmaCode'].astype('int')
# support_df['Date'] = pd.to_datetime(support_df['Date'])



## Merging Extended Transformations ##
# model_df = model_df.merge(support_df, on=['DmaCode', 'Date'])



################
### Edit 1 ###
################

# model_df['Email_Membership_Spend'] = model_df['Email_Membership_Spend_y']
# model_df['Email_NonMembership_Spend'] = model_df['Email_NonMembership_Spend_y']

# model_df = model_df.drop(['Email_Membership_Spend_x', 'Email_NonMembership_Spend_x', 'Email_Membership_Spend_y', 'Email_NonMembership_Spend_y'], axis=1)

# COMMAND ----------

## Saving Dataframe ##
# model_df.fillna(0, inplace=True)
# model_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_reg_df_BuyingGroup.csv')

## Reading Model ##
# model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_reg_df_BuyingGroup.csv').drop('Unnamed: 0',axis=1)
# model_df.head()

# COMMAND ----------



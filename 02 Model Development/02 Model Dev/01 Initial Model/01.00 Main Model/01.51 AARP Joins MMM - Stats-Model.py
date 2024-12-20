# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Reading Data

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
# MAGIC ## 1.2 Column Classification

# COMMAND ----------

## General Classification ##
primary_cols = ['DmaCode', 'Date']
target_col = ['Joins']
other_col = ['Reg', 'BenRefAll']

spend_col = ['Affiliate_Spend', 'AltMedia_Spend', 'Audio_Spend', 'DMP_Spend', 'DirectMail_Spend', 'Display_Spend', 'Email_Spend', 'Facebook_Spend', 'LeadGen_Spend', 'Radio_Spend',
       'Newsletter_Spend', 'OOH_Spend', 'Print_Spend', 'Search_Spend','Social_Spend', 'TV_Spend', 'Video_Spend']

imps_col = ['Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps', 'Facebook_Imps', 'LeadGen_Imps',
       'Radio_Imps', 'Newsletter_Imps', 'OOH_Imps', 'Print_Imps', 'Search_Imps', 'Social_Imps', 'TV_Imps', 'Video_Imps']

clicks_col = ['Affiliate_Clicks', 'AltMedia_Clicks', 'Audio_Clicks', 'DMP_Clicks', 'DirectMail_Clicks', 'Display_Clicks', 'Email_Clicks',
       'Facebook_Clicks', 'LeadGen_Clicks', 'Radio_Clicks', 'Newsletter_Clicks', 'OOH_Clicks', 'Print_Clicks', 'Search_Clicks', 'Social_Clicks', 'TV_Clicks', 'Video_Clicks']

HnP_col = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow',
       'PromoEvnt_22TikTok', 'Holiday_22Christams', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23EvdaySavCamp', 'PromoEvnt_23RollingStone',
       'PromoEvnt_23MemorialDay', 'PromoEvnt_23LaborDay', 'PromoEvnt_23BlackFriday', 'Holiday_23Christams']

seasonality_col = ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6']

Brand_col = ['Index', 'Buzz', 'Impression', 'Quality', 'Value', 'Reputation', 'Satisfaction', 'Recommend',
       'Awareness', 'Attention', 'Ad_Awareness', 'WOM_Exposure', 'Consideration', 'Purchase_Intent', 'Current_Customer', 'Former_Customer']

## Model Building Columns ##
primary_cols = ['DmaCode', 'Date']
target_col = ['Joins']

MediaModel_col = ['Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps', 'Facebook_Imps', 'LeadGen_Imps',
       'Radio_Imps', 'Newsletter_Imps', 'OOH_Imps', 'Print_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps']  ## Adding Search Clicks and removing Search Impressions 

HnP_col = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow',
       'PromoEvnt_22TikTok', 'Holiday_22Christams', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23EvdaySavCamp', 'PromoEvnt_23RollingStone',
       'PromoEvnt_23MemorialDay', 'PromoEvnt_23LaborDay', 'PromoEvnt_23BlackFriday', 'Holiday_23Christams']

seasonality_col = ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6']

Brand_col = ['Index', 'Buzz', 'Impression', 'Quality', 'Value', 'Reputation', 'Satisfaction', 'Recommend',
       'Awareness', 'Attention', 'Ad_Awareness', 'WOM_Exposure', 'Consideration', 'Purchase_Intent', 'Current_Customer', 'Former_Customer']

other_col = ['Reg', 'BenRefAll']


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Model Data

# COMMAND ----------

rawdata_df = rawdata_df[primary_cols+target_col+MediaModel_col+spend_col+HnP_col+seasonality_col+Brand_col+other_col].sort_values(by=['DmaCode', 'Date'])
rawdata_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.0 Tranformation Section

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Defining Functions

# COMMAND ----------

######################
###### AdStock #######
######################


def adstock_func(var_data, factor):
    x = 0
    adstock_var = [x := x * factor + v for v in var_data]

    suffix = f'_AdStock{round(factor*100)}'
    return pd.Series(adstock_var, index=var_data.index), suffix


# Define the adstock function with week cutoff
def adstock_func_week_cut(var_data, decay_rate, cutoff_weeks):
    adstocked_data = np.zeros_like(var_data)
    for t in range(len(var_data)):
        for i in range(cutoff_weeks):
            if t - i >= 0:
                adstocked_data[t] += var_data[t - i] * (decay_rate ** i)

    suffix = f'_AdStock{round(decay_rate*100)}Week{cutoff_weeks}'
    return pd.Series(adstocked_data, index=var_data.index), suffix

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
    
    adstock_series, adstock_suffix = adstock_func_week_cut(original_series, *adstock_params)    
    t_var_name = var_name + adstock_suffix

    power_series, pow_suffix = power_func(adstock_series, *sat_params)
    t_var_name = t_var_name + pow_suffix

    scaled_series= (power_series - power_series.min())/(power_series.max() - power_series.min())
    

    ## Concatination ##
    dma_dict[cross_section] = scaled_series

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

input_dict ={
  
  'Affiliate_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},
  'AltMedia_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}}, 
  'Audio_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}}, 
  'DMP_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}}, 
  'DirectMail_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},  
  'Display_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},  
  'Email_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},  
  'Facebook_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},  
  'LeadGen_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},  
  'Radio_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},  
  'Newsletter_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},  
  'OOH_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},  
  'Print_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},  
  'Search_Clicks': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}}, 
  'Social_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},  
  'TV_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}},  
  'Video_Imps': {'Adstock':{'decay_rate': list(frange(0.3,0.9,0.1)), 'week_cutoff':[6]}, 'Saturation':{'power':list(frange(0.3,0.9,0.1))}}, 
   
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
# MAGIC grouped_df = rawdata_df.groupby('DmaCode')[['Date', 'Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps', 'Facebook_Imps', 'LeadGen_Imps', 'Radio_Imps', 'Newsletter_Imps', 'OOH_Imps', 'Print_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps']]
# MAGIC
# MAGIC print("Starting Transformations")
# MAGIC ### Tranformation ###
# MAGIC
# MAGIC var_transform_dict ={}
# MAGIC for var_name, params in transform_dict.items():
# MAGIC
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

## Creating Model Dataframe ##
# model_df = rawdata_df.merge(transformed_df, on=['DmaCode', 'Date'])

## saving model dataframe ##
# model_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_df_statsmodel.csv')
# model_df.head()

# COMMAND ----------

## Reading Model ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_df_statsmodel.csv').drop('Unnamed: 0',axis=1)
model_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Adding More Variables

# COMMAND ----------

## Adding Year Column ##
model_df['YearFlag_22'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).year==2022 else 0)

## Adding flag for 7/11/2022 ##
model_df['dummy_2022_07_11'] = model_df['Date'].apply(lambda x: 1 if x == '2022-07-11' else 0)

## Fill NaN with Zero ##
model_df = model_df.fillna(0)

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
model_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_statsmodel.csv')

## Reading Model ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_updated_df_statsmodel.csv').drop('Unnamed: 0',axis=1)
model_df.head()

# COMMAND ----------

## Model Building Columns Inventory ##
primary_cols = ['DmaCode', 'Date']
target_col = ['Joins']

MediaModel_col = ['Affiliate_Imps', 'AltMedia_Imps', 'Audio_Imps', 'DMP_Imps', 'DirectMail_Imps', 'Display_Imps', 'Email_Imps', 'Facebook_Imps', 'LeadGen_Imps',
       'LocalRadio_Imps', 'Newsletter_Imps', 'OOH_Imps', 'Print_Imps', 'Search_Clicks', 'Social_Imps', 'TV_Imps', 'Video_Imps']  ## Adding Search Clicks and removing Search Impressions 

HnP_col = ['PromoEvnt_22MembDrive', 'PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow',
       'PromoEvnt_22TikTok', 'Holiday_22Christams', 'PromoEvnt_23KaylaCoupon',
       'PromoEvnt_23MemorialDay', 'PromoEvnt_23LaborDay', 'PromoEvnt_23BlackFriday', 'Holiday_23Christams'] ## Removed  'PromoEvnt_23EvdaySavCamp' (con) and 'PromoEvnt_23RollingStone'

seasonality_col = ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6']

Brand_col = ['Index', 'Buzz', 'Impression', 'Quality', 'Value', 'Reputation', 'Satisfaction', 'Recommend',
       'Awareness', 'Attention', 'Ad_Awareness', 'WOM_Exposure', 'Consideration', 'Purchase_Intent', 'Current_Customer', 'Former_Customer']

dummy_col = ['YearFlag_22', 'dummy_2022_07_11']

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

corr_dict_best

# COMMAND ----------

corr_dict_best.values()

# COMMAND ----------

## Vizualization BlocK ##
def viz_transform(var_name = 'Affiliate_Imps', transformed_var_name = 'Affiliate_Imps_AdStock90_Power30' ):

  ## Create Data ##
  df1 = model_df.groupby('Date').agg({var_name:'sum'})
  df2 = model_df.groupby('Date').agg({transformed_var_name:'sum'})

  df = df1.join(df2)

  # Create subplots
  fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

  # Plot all columns on the left-hand side
  df.plot(ax=ax1, marker='o')
  ax1.set_title(f'{var_name} and {transformed_var_name}')
  ax1.set_xlabel('Date')
  ax1.set_ylabel('Values')
  ax1.legend(title='Columns')
  ax1.grid(True)

  # Plot a selected column on the right-hand side (e.g., column 'A')
  df2.plot(ax=ax2, color='tab:blue', marker='o')
  ax2.set_title(f'{var_name} and {transformed_var_name}')
  ax2.set_xlabel('Date')
  ax2.set_ylabel(transformed_var_name)
  ax2.grid(True)

  # Show the plots
  plt.tight_layout()
  plt.show()


## Function Call ##
viz_transform(var_name = 'Affiliate_Imps', transformed_var_name = 'Affiliate_Imps_AdStock90Week6_Power90'  )

# COMMAND ----------

#############################
### Gut Check for AdStock ###
#############################

def visualize_decay(initial_value=10000, num_weeks=10, gamma_values=np.arange(0.1, 1.0, 0.1)):
    plt.figure(figsize=(12, 8))
    
    decay_data = []

    for gamma in gamma_values:
        values = [initial_value]
        for week in range(1, num_weeks):
            values.append(values[-1] ** gamma)
        plt.plot(range(1, num_weeks + 1), values, label=f'gamma = {gamma:.1f}')
        
    
    plt.xlabel('Week')
    plt.ylabel('Value')
    plt.title('Decay over 10 Weeks for Different Gamma Values')
    plt.legend()
    plt.yscale('log')
    plt.grid(True)
    plt.show()
    
    

# Call the function and store the returned DataFrame
visualize_decay()

# COMMAND ----------

## Model Creation ##
seasoanlity_vars =  ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6'] # Double Check on Fourier Frequencies 

Holiday_vars = ['PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'Holiday_22Christams', 'PromoEvnt_23MemorialDay', 'PromoEvnt_23LaborDay', 'PromoEvnt_23BlackFriday', 'Holiday_23Christams']

Promo_vars = ['PromoEvnt_22MembDrive', 'PromoEvnt_22WsjTodayShow', 'PromoEvnt_22TikTok', 'PromoEvnt_23KaylaCoupon']

dummy_vars = ['YearFlag_22', 'dummy_2022_07_11']

media_vars = ['Affiliate_Imps_AdStock90Week6_Power90', 'AltMedia_Imps_AdStock90Week6_Power30', 'Audio_Imps_AdStock90Week6_Power90', 'DMP_Imps_AdStock80Week6_Power90', 'DirectMail_Imps_AdStock90Week6_Power40', 'Display_Imps_AdStock90Week6_Power90', 'Email_Imps_AdStock30Week6_Power90', 'Facebook_Imps_AdStock30Week6_Power90', 'LeadGen_Imps_AdStock30Week6_Power90', 'Search_Clicks_AdStock30Week6_Power30', 'Social_Imps_AdStock90Week6_Power30', 'TV_Imps_AdStock50Week6_Power30', 'Video_Imps_AdStock30Week6_Power30'] ## Removed   'Radio_Imps_AdStock30Week6_Power30', 'Newsletter_Imps_AdStock30Week6_Power30', 'OOH_Imps_AdStock90Week6_Power30', 'Print_Imps_AdStock80Week6_Power30', ##


input_vars = seasoanlity_vars + Holiday_vars + Promo_vars + dummy_vars + media_vars 

'''

Write Assertion for NULL VAlues

'''

input_vars_str = " + ".join(input_vars)

## Model Fit ##
model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit()

# COMMAND ----------

## Saving Model ##
model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel.pkl'
with open(model_path,"wb") as f:
  pickle.dump(model, f)


# ## Reading Model ##
# model_path = '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel.pkl'
# with open(model_path, "rb") as f:
#   model = pickle.load(f)

# COMMAND ----------

## Creating Model Predict ##
joins = model_df[['DmaCode', 'Date','Joins']]
pred = model.fittedvalues.rename('Pred')

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
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(25, 6))

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


print(f"Mean Absolute Percentage Error (MAPE): {mape}")
print(f"R-squared: {r_squared}")
print(f"Skewness: {skewness}")
print(f"Kurtosis: {kurt}")
print(f"DW Stat: {dw_stat}")


plt.tight_layout()
plt.show()

# COMMAND ----------

#Model Summary
model.summary().tables[1].reset_index().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Contribution File 

# COMMAND ----------

## Place Holder ##

# COMMAND ----------



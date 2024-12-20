# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Reading Data

# COMMAND ----------

## Reading parameter file ##
param_mem_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_membership_curves_params.csv').drop('Unnamed: 0',axis=1)
param_icm_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_icm_curves_params.csv').drop('Unnamed: 0',axis=1)
param_brand_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_brand_curves_params.csv').drop('Unnamed: 0',axis=1)
param_asi_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_asi_curves_params.csv').drop('Unnamed: 0',axis=1)
param_RemainingEM_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_RemainingEM_curves_params.csv').drop('Unnamed: 0',axis=1)

param_df = pd.concat([param_mem_df, param_icm_df, param_brand_df, param_asi_df, param_RemainingEM_df])
param_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Budget Optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Defining Functions 

# COMMAND ----------

### For Logging Start ##
# log_df = pd.DataFrame(columns = ['iteration','Channel', 'Spend', 'Scaled_Spend', 'Hill_Equation_Output', 'Scaled_Output']) 
# ct = 0
### For Logging End ##




## Func 1 ##
def hill_equation(x, kd, n):
  return x**n/(kd**n + x**n)


## Func 2 ##
def join_constraint(x):
  result=0 
  
  # global ct, log_df
  # temp = []
  # ct+=1


  for idx, spend in enumerate(x):


    ch_list = list(response_curve_params.keys())
    params = response_curve_params[ch_list[idx]]

    # ### Limiting Spend value to upper bound and lower bound ###
    # if spend > opt_param_df[opt_param_df['Channel'] == ch_list[idx]]['Upper Bound Spend'].values[0]:
    #   spend = opt_param_df[opt_param_df['Channel'] == ch_list[idx]]['Upper Bound Spend'].values[0]-1
    # elif spend < opt_param_df[opt_param_df['Channel'] == ch_list[idx]]['Lower Bound Spend'].values[0]:
    #   spend = opt_param_df[opt_param_df['Channel'] == ch_list[idx]]['Lower Bound Spend'].values[0]+1
    # else:
    #   spend = spend


        
    scaled_spend = ((spend/params['num_rows']) - params['x_min'])/((params['x_max']-params['x_min']))

    hill_equation_output = hill_equation(scaled_spend, params['k'],params['n'])

    scaled_output = (hill_equation_output)*(params['y_max']-params['y_min']) + params['y_min']
    scaled_output = scaled_output*params['num_rows']*params['factor']

    # log_df = log_df._append({'iteration':ct,'Channel':ch_list[idx], 'Spend':spend, 'Scaled_Spend':scaled_spend, 'Hill_Equation_Output':hill_equation_output, 'Scaled_Output':scaled_output}, ignore_index=True)
    # temp+=[round(scaled_output,2)]

    result += scaled_output
  # print((temp))
  return result
  


## Func 3 ##
def func5(total_spend_2023,k, n,x_min, x_max, y_min, y_max, num_rows, factor):

  x_scaled = ((total_spend_2023/num_rows)-x_min)/(x_max-x_min) 
  y_scaled = hill_equation(x_scaled, k, n)
  y = (y_scaled*(y_max-y_min)+y_min)*num_rows*factor
  return y


## Func 4 ##
def objective_function(x):
  # print(x)
  return sum(x)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Defining other params

# COMMAND ----------

response_curve_params = param_df.set_index('Channel').to_dict(orient='index')
response_curve_params

# COMMAND ----------

pd.DataFrame.from_dict(response_curve_params).T['Total_Contri_2023'].sum()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Defining Constraints and Objective function

# COMMAND ----------

### Defining params ###

lb = -40
ub = 40

opt_param = {
              'DirectMail_Membership':   {'Total Spend': 52112911.35541834,   'Upper Bound':ub,   'Lower Bound': lb}, 
              'AltMedia_Membership':     {'Total Spend': 50093326.45130657,   'Upper Bound':ub,   'Lower Bound': lb}, 
              'LeadGen_Membership':      {'Total Spend': 23149207.0,          'Upper Bound':ub,   'Lower Bound': lb}, 
              'Email_Membership':        {'Total Spend': 17852147.326496422,  'Upper Bound':ub,   'Lower Bound': lb}, 
              'Display_Membership':      {'Total Spend': 6179189.19883013,    'Upper Bound':ub,   'Lower Bound': lb}, 
              'Search_Membership':       {'Total Spend': 22028026.85855786,   'Upper Bound':ub,   'Lower Bound': lb}, 
              'Social_Membership':       {'Total Spend': 21836602.231147,     'Upper Bound':ub,   'Lower Bound': lb}, 
              'TV_Membership':           {'Total Spend': 15127078.156955216,  'Upper Bound':ub,   'Lower Bound': lb},


              'Display_ICM':   {'Total Spend': 1785556.2832789824,   'Upper Bound':0,   'Lower Bound': 0}, 
              'Search_ICM':    {'Total Spend': 6896452.472262759,    'Upper Bound':0,   'Lower Bound': 0}, 
              'Social_ICM':    {'Total Spend': 14619418.366258431,   'Upper Bound':0,   'Lower Bound': 0}, 
              'Video_ICM':     {'Total Spend': 1503818.1578639755,   'Upper Bound':0,   'Lower Bound': 0},


              'Display_Brand':    {'Total Spend': 4179305.159738069,     'Upper Bound':0,   'Lower Bound': 0}, 
              'Social_Brand':     {'Total Spend': 2984558.145521847,     'Upper Bound':0,   'Lower Bound': 0}, 
              'Video_Brand':      {'Total Spend': 13817339.29961512,     'Upper Bound':0,   'Lower Bound': 0}, 
              'TV_Brand':         {'Total Spend': 43505699.74999999,     'Upper Bound':0,   'Lower Bound': 0}, 
              'Search_Brand':     {'Total Spend': 1242234.3904006367,    'Upper Bound':0,   'Lower Bound': 0}, 


              'Display_ASI':   {'Total Spend': 2631859.4087892314,   'Upper Bound':0,   'Lower Bound': 0}, 
              'Social_ASI':    {'Total Spend': 4437897.12118652,     'Upper Bound':0,   'Lower Bound': 0}, 
              'Search_ASI':    {'Total Spend': 4632680.48,           'Upper Bound':0,   'Lower Bound': 0},

              'Print_RemainingEM':          {'Total Spend': 161126.56,                  'Upper Bound':0,   'Lower Bound': 0}, 
              'TV_RemainingEM':             {'Total Spend': 1600212.25,                 'Upper Bound':0,   'Lower Bound': 0}, 
              'Search_RemainingEM':         {'Total Spend': 1125171.890082923,          'Upper Bound':0,   'Lower Bound': 0}, 
              'Social_RemainingEM':         {'Total Spend': 524523.1399980248,          'Upper Bound':0,   'Lower Bound': 0}, 
              'Display_RemainingEM':        {'Total Spend': 3394329.087609186,          'Upper Bound':0,   'Lower Bound': 0}, 
              'Video_RemainingEM':          {'Total Spend': 4076713.431621416,          'Upper Bound':0,   'Lower Bound': 0}


              }

### COnverting to Df ##
opt_param_df = pd.DataFrame.from_dict(opt_param, orient='index').reset_index().rename(columns={'index':'Channel'})

## Creating New Column ##
opt_param_df['Lower Bound Spend'] = round((1 + opt_param_df['Lower Bound']/100)*opt_param_df['Total Spend'],2)
opt_param_df['Upper Bound Spend'] = round((1 + opt_param_df['Upper Bound']/100)*opt_param_df['Total Spend'],2)


opt_param_df = opt_param_df.rename(columns= {'Total Spend':'Current Spend'})
opt_param_df['Current Spend'] = round(opt_param_df['Current Spend'],2)

opt_param_df = opt_param_df[['Channel', 'Current Spend', 'Lower Bound', 'Upper Bound','Lower Bound Spend', 'Upper Bound Spend']]
opt_param_df.display()

# COMMAND ----------

#######################################
### Setting Constratints and Bounds ###
#######################################

factor = 1.00

## Setting Total Budget Constraint
total_spend = opt_param_df['Current Spend'].sum()*factor
num_channels = opt_param_df.shape[0]


total_join = 1876364
constraint = NonlinearConstraint(join_constraint, lb = total_join-100, ub = total_join + 100, keep_feasible=True)

## Setting Upper Bound and Lower Bound ##
bounds = [tuple(i) for i in opt_param_df[['Lower Bound Spend', 'Upper Bound Spend']].values]

## Initializing Spend ##
initial_spend = [i*factor for i in list(opt_param_df['Current Spend'].values)]



### Objective Function ###
res = minimize(objective_function, method='trust-constr', x0=initial_spend, constraints=constraint, bounds=bounds, options={'maxiter':300000, 'disp':True})
res.x

# COMMAND ----------

res.x

# COMMAND ----------

## Output Df ##
output_df = opt_param_df.copy()
output_df['Optimized_Spend'] = res.x

output_df['Delta wrt Initial Spend']  = round((output_df['Optimized_Spend']-output_df['Current Spend'])/output_df['Current Spend'],4)*100

## Adding mROI channel ##
output_df = output_df.merge(param_df[['Channel', 'mROI', 'Total_Contri_2023', 'k', 'n','x_min', 'x_max', 'y_min', 'y_max', 'num_rows', 'factor']])

## Calculating Contributon at Optimal Spend ## 
output_df = output_df.rename(columns= {'Total_Contri_2023':'Contribution_at_Current_Spend'})
output_df['Contribution_at_Opt_Spend'] = output_df.apply(lambda x: func5(x.Optimized_Spend,x.k, x.n, x.x_min, x.x_max, x.y_min, x.y_max, x.num_rows, x.factor),axis=1) 
output_df['Delta wrt Initial Contribution']  = round((output_df['Contribution_at_Opt_Spend']-output_df['Contribution_at_Current_Spend'])/output_df['Contribution_at_Current_Spend'],4)*100


output_df['Contribution_at_prop_Spend'] = output_df.apply(lambda x: func5(x['Current Spend']*factor,x.k, x.n, x.x_min, x.x_max, x.y_min, x.y_max, x.num_rows, x.factor),axis=1)

## mROI at Optimized Spend ##
ctv = 95.19
output_df['mROI at Opt_Spend'] = output_df.apply(lambda x: (func5(x.Optimized_Spend+1,x.k, x.n, x.x_min, x.x_max, x.y_min, x.y_max, x.num_rows, x.factor)- func5(x.Optimized_Spend,x.k, x.n, x.x_min, x.x_max, x.y_min, x.y_max, x.num_rows, x.factor))*ctv, axis=1)


## Adding 
cols = ['Channel', 'Upper Bound', 'Lower Bound', 'Upper Bound Spend', 'Lower Bound Spend', 'Current Spend', 'Optimized_Spend', 'Delta wrt Initial Spend', 'mROI', 'mROI at Opt_Spend', 'Contribution_at_Current_Spend',  'Contribution_at_Opt_Spend','Delta wrt Initial Contribution', 'Contribution_at_prop_Spend']
analysis_df = output_df[cols]
analysis_df

# COMMAND ----------

delta_contri = round(((analysis_df['Contribution_at_Opt_Spend'].sum()-analysis_df['Contribution_at_Current_Spend'].sum())/(analysis_df['Contribution_at_Current_Spend'].sum()))*100,2)
delta_contri

# COMMAND ----------

analysis_df.display()

# COMMAND ----------

'''
l = [['Email_Membership', 22313327.325831905],  
['Search_ICM', 10226289.107706903],
['LeadGen_Membership', 25604182.81817295],
['Search_Membership', 23343210.67156935], 
['TV_Membership', 16285619.847098596], 
['Display_ICM', 2678329.725302125], 
['Display_Brand', 4818342.821411859], 
['Display_RemainingEM', 3870144.516024971], 
['Search_ASI', 5052590.959083307], 
['Display_Membership', 6401320.54924173]]



for i in l:

  ch_name = i[0]
  a = func5(i[1],response_curve_params[ch_name]['k'], response_curve_params[ch_name]['n'],response_curve_params[ch_name]['x_min'], response_curve_params[ch_name]['x_max'], response_curve_params[ch_name]['y_min'], response_curve_params[ch_name]['y_max'], response_curve_params[ch_name]['num_rows'], response_curve_params[ch_name]['factor'])
  b = func5(i[1]+100000,response_curve_params[ch_name]['k'], response_curve_params[ch_name]['n'],response_curve_params[ch_name]['x_min'], response_curve_params[ch_name]['x_max'], response_curve_params[ch_name]['y_min'], response_curve_params[ch_name]['y_max'], response_curve_params[ch_name]['num_rows'], response_curve_params[ch_name]['factor'])

  print(ch_name, " ", b-a)


'''

# COMMAND ----------



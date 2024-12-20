# Databricks notebook source
# MAGIC %md
# MAGIC # 0.0 Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1.0 Reading Data

# COMMAND ----------

contri_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_contri_df.csv').drop('Unnamed: 0',axis=1)
spend_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_spend_df.csv').drop('Unnamed: 0',axis=1)

df = contri_df.merge(spend_df, on=['Date'])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 2.0 Creating Response Curves

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Function Definition

# COMMAND ----------

###################################
## Functions for Response Curves ##
###################################



## Func 1 ##

def hill_equation(x, kd, n):
  return x**n/(kd**n + x**n)


## Func 2 ##
def hill_func(x_data,y_data, params):

  if not params:
    # Fit the Hill equation to the data
    initial_guess = [1, 1]  # Initial guess for Kd and n
    params, covariance = curve_fit(hill_equation, x_data, y_data, p0=initial_guess, maxfev=10000)

    # Extract the fitted parameters
    Kd_fit, n_fit = params
  else:
    Kd_fit, n_fit = np.array(params)

  # Generate y values using the fitted parameters
  y_fit = hill_equation(x_data, Kd_fit, n_fit)

  x_data_inv = x_minmax.inverse_transform(np.array(x_data).reshape(-1,1))
  y_data_inv = y_minmax.inverse_transform(np.array(y_data).reshape(-1,1))
  y_fit_inv = y_minmax.inverse_transform(np.array(y_fit).reshape(-1,1))


  return y_fit,y_fit_inv,Kd_fit, n_fit

## Func 3 ##
def data_output(channel,df,X,y,y_fit_inv): 
  plot_df = pd.DataFrame()

  plot_df['Date'] = df['Date']
  plot_df[f'{channel}_Spend'] = X
  plot_df[f'{channel}_Contri'] = y

  y_fit_inv_v2 = []
  for i in range(len(y_fit_inv)):
      y_fit_inv_v2.append(y_fit_inv[i][0])

  plot_df['Fit_Data'] = y_fit_inv_v2
  return plot_df


## Func 4 ##

def input_data(df,spend_col,prospect_col):  
  X = np.array(df[spend_col].tolist())
  y = np.array(df[prospect_col].tolist())

  x_minmax = MinMaxScaler()
  x_scaled = x_minmax.fit_transform(df[[spend_col]])
  x_data = []
  for i in range(len(x_scaled)):
      x_data.append(x_scaled[i][0])

  y_minmax = MinMaxScaler()
  y_scaled =  y_minmax.fit_transform(df[[prospect_col]])
  y_data = []
  for i in range(len(y_scaled)):
      y_data.append(y_scaled[i][0])

  return X,y,x_data,y_data,x_minmax,y_minmax


## Func 5 ##

def func5(total_spend_2023,n,x_minmax, y_minmax, Kd_fit, n_fit, factor):

  x_scaled = x_minmax.transform( (total_spend_2023/n ).reshape(-1,1))
  y_scaled = hill_equation(x_scaled, Kd_fit, n_fit)
  y = y_minmax.inverse_transform(y_scaled.reshape(-1, 1))*n*factor
  return y




# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Creating Response Curves

# COMMAND ----------

### Craeting Dataframe to store channel parameter ###
param_list= []

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.1 Direct Mail

# COMMAND ----------

## Change it for Diff. Channels ##
spend_col = 'DirectMail_Membership_Spend'
prospect_col = 'DirectMail_Membership'

# params = None
params = [0.44, 0.53]
# params = [2.0, 0.53]

# COMMAND ----------

#####################################################################################
#####################################################################################
temp_df = df[df[spend_col]>0]
temp_df.reset_index(inplace=True)



X,y,x_data,y_data,x_minmax,y_minmax = input_data(temp_df,spend_col,prospect_col)
y_fit, y_fit_inv, Kd_fit, n_fit = hill_func(x_data,y_data, params)
output_data = data_output(prospect_col,temp_df,X,y,y_fit_inv)
#####################################################################################
#####################################################################################


#######################################
###### Calibrating Output Data ########
#######################################

total_spend_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][spend_col].sum() 
total_contri_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][prospect_col+'_Contri'].sum()

output_data_2023 = output_data[pd.to_datetime(output_data['Date']).dt.year==2023]
total_spend_2023 = output_data_2023[spend_col].sum() 
total_contri_2023 = output_data_2023[prospect_col+'_Contri'].sum()


num_of_rows = len(output_data_2023[output_data_2023[spend_col]>0])
factor = total_contri_2023/func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,1)[0][0]

output_data['calibrated_spend'] = output_data[spend_col]*num_of_rows
output_data['calibrated_fit'] = output_data['Fit_Data']*num_of_rows*factor





########################
###### Get mROI ########
########################


ctv = 95.19
mROI = (func5(total_spend_2023+1,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0] - func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0])*ctv 

print('k: ',Kd_fit)
print('n: ', n_fit)
print('mROI', mROI)


print("##########################")

print("Yearly Spend for 2022 is: ", total_spend_2022)
print("Yearly Spend for 2023 is: ", total_spend_2023)
print("Yearly Contribution for 2023 is: ", total_contri_2023)

### Saving in Dataframe ###
param_list.append({'Channel':prospect_col, 'k':Kd_fit, 'n':n_fit, 'mROI':mROI, 'Total_Spend_2023':total_spend_2023, 'Total_Contri_2023':total_contri_2023, 'x_min':x_minmax.data_min_[0], 'x_max': x_minmax.data_max_[0], 'y_min': y_minmax.data_min_[0], 'y_max': y_minmax.data_max_[0], 'num_rows': num_of_rows, 'factor':factor })

##################################
############## Plot ##############
##################################



# Set up a single figure for two subplots
plt.figure(figsize=(15,12))

# First subplot: Original data and fitted curve
plt.subplot(2, 1, 1)
plt.scatter(output_data[spend_col], output_data[prospect_col+'_Contri'], label='MMM Estimated Data')
plt.scatter(output_data[spend_col], output_data['Fit_Data'], label='Hill Equation Fit Data', color='red')

# Labels, title, and legend for the first subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Second subplot: Average monthly spend for 2023
plt.subplot(2, 1, 2)
plt.scatter(output_data['calibrated_spend'], output_data['calibrated_fit'], label='Hill Equation Fit Data')

## 2023 ##
plt.scatter(total_spend_2023, total_contri_2023, label='2023 Spend', color='green')
plt.axvline(total_spend_2023, color='green', linestyle='--')

## 2022 ##
plt.scatter(total_spend_2022, total_contri_2022, label='2022 Spend', color='orange')
plt.axvline(total_spend_2022, color='orange', linestyle='--')

# Labels, title, and legend for the second subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Show the entire figure with both subplots
plt.tight_layout()  # Adjusts spacing to prevent overlap
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.2 Alt Media

# COMMAND ----------

## Change it for Diff. Channels ##
spend_col = 'AltMedia_Membership_Spend'
prospect_col = 'AltMedia_Membership'

# params = None
params = [1.44, 0.53]

# COMMAND ----------

#####################################################################################
#####################################################################################
temp_df = df[df[spend_col]>0]
temp_df.reset_index(inplace=True)



X,y,x_data,y_data,x_minmax,y_minmax = input_data(temp_df,spend_col,prospect_col)
y_fit, y_fit_inv, Kd_fit, n_fit = hill_func(x_data,y_data, params)
output_data = data_output(prospect_col,temp_df,X,y,y_fit_inv)
#####################################################################################
#####################################################################################


#######################################
###### Calibrating Output Data ########
#######################################

total_spend_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][spend_col].sum() 
total_contri_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][prospect_col+'_Contri'].sum()

output_data_2023 = output_data[pd.to_datetime(output_data['Date']).dt.year==2023]
total_spend_2023 = output_data_2023[spend_col].sum() 
total_contri_2023 = output_data_2023[prospect_col+'_Contri'].sum()


num_of_rows = len(output_data_2023[output_data_2023[spend_col]>0])
factor = total_contri_2023/func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,1)[0][0]

output_data['calibrated_spend'] = output_data[spend_col]*num_of_rows
output_data['calibrated_fit'] = output_data['Fit_Data']*num_of_rows*factor





########################
###### Get mROI ########
########################


ctv = 95.19
mROI = (func5(total_spend_2023+1,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0] - func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0])*ctv 

print('k: ',Kd_fit)
print('n: ', n_fit)
print('mROI', mROI)


print("##########################")

print("Yearly Spend for 2022 is: ", total_spend_2022)
print("Yearly Spend for 2023 is: ", total_spend_2023)
print("Yearly Contribution for 2023 is: ", total_contri_2023)

### Saving in Dataframe ###
param_list.append({'Channel':prospect_col, 'k':Kd_fit, 'n':n_fit, 'mROI':mROI, 'Total_Spend_2023':total_spend_2023, 'Total_Contri_2023':total_contri_2023, 'x_min':x_minmax.data_min_[0], 'x_max': x_minmax.data_max_[0], 'y_min': y_minmax.data_min_[0], 'y_max': y_minmax.data_max_[0], 'num_rows': num_of_rows, 'factor':factor })

##################################
############## Plot ##############
##################################



# Set up a single figure for two subplots
plt.figure(figsize=(15,12))

# First subplot: Original data and fitted curve
plt.subplot(2, 1, 1)
plt.scatter(output_data[spend_col], output_data[prospect_col+'_Contri'], label='MMM Estimated Data')
plt.scatter(output_data[spend_col], output_data['Fit_Data'], label='Hill Equation Fit Data', color='red')

# Labels, title, and legend for the first subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Second subplot: Average monthly spend for 2023
plt.subplot(2, 1, 2)
plt.scatter(output_data['calibrated_spend'], output_data['calibrated_fit'], label='Hill Equation Fit Data')

## 2023 ##
plt.scatter(total_spend_2023, total_contri_2023, label='2023 Spend', color='green')
plt.axvline(total_spend_2023, color='green', linestyle='--')

## 2022 ##
plt.scatter(total_spend_2022, total_contri_2022, label='2022 Spend', color='orange')
plt.axvline(total_spend_2022, color='orange', linestyle='--')

# Labels, title, and legend for the second subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Show the entire figure with both subplots
plt.tight_layout()  # Adjusts spacing to prevent overlap
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.3 Leadgen

# COMMAND ----------

## Change it for Diff. Channels ##
spend_col = 'LeadGen_Membership_Spend'
prospect_col = 'LeadGen_Membership'

# params = None
# params = [0.44, 0.53]
params = [0.44, 0.53]

# COMMAND ----------

#####################################################################################
#####################################################################################
temp_df = df[df[spend_col]>0]
temp_df.reset_index(inplace=True)



X,y,x_data,y_data,x_minmax,y_minmax = input_data(temp_df,spend_col,prospect_col)
y_fit, y_fit_inv, Kd_fit, n_fit = hill_func(x_data,y_data, params)
output_data = data_output(prospect_col,temp_df,X,y,y_fit_inv)
#####################################################################################
#####################################################################################


#######################################
###### Calibrating Output Data ########
#######################################

total_spend_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][spend_col].sum() 
total_contri_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][prospect_col+'_Contri'].sum()

output_data_2023 = output_data[pd.to_datetime(output_data['Date']).dt.year==2023]
total_spend_2023 = output_data_2023[spend_col].sum() 
total_contri_2023 = output_data_2023[prospect_col+'_Contri'].sum()


num_of_rows = len(output_data_2023[output_data_2023[spend_col]>0])
factor = total_contri_2023/func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,1)[0][0]

output_data['calibrated_spend'] = output_data[spend_col]*num_of_rows
output_data['calibrated_fit'] = output_data['Fit_Data']*num_of_rows*factor





########################
###### Get mROI ########
########################


ctv = 95.19
mROI = (func5(total_spend_2023+1,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0] - func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0])*ctv 

print('k: ',Kd_fit)
print('n: ', n_fit)
print('mROI', mROI)


print("##########################")

print("Yearly Spend for 2022 is: ", total_spend_2022)
print("Yearly Spend for 2023 is: ", total_spend_2023)
print("Yearly Contribution for 2023 is: ", total_contri_2023)

### Saving in Dataframe ###
param_list.append({'Channel':prospect_col, 'k':Kd_fit, 'n':n_fit, 'mROI':mROI, 'Total_Spend_2023':total_spend_2023, 'Total_Contri_2023':total_contri_2023, 'x_min':x_minmax.data_min_[0], 'x_max': x_minmax.data_max_[0], 'y_min': y_minmax.data_min_[0], 'y_max': y_minmax.data_max_[0], 'num_rows': num_of_rows, 'factor':factor })

##################################
############## Plot ##############
##################################



# Set up a single figure for two subplots
plt.figure(figsize=(15,12))

# First subplot: Original data and fitted curve
plt.subplot(2, 1, 1)
plt.scatter(output_data[spend_col], output_data[prospect_col+'_Contri'], label='MMM Estimated Data')
plt.scatter(output_data[spend_col], output_data['Fit_Data'], label='Hill Equation Fit Data', color='red')

# Labels, title, and legend for the first subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Second subplot: Average monthly spend for 2023
plt.subplot(2, 1, 2)
plt.scatter(output_data['calibrated_spend'], output_data['calibrated_fit'], label='Hill Equation Fit Data')

## 2023 ##
plt.scatter(total_spend_2023, total_contri_2023, label='2023 Spend', color='green')
plt.axvline(total_spend_2023, color='green', linestyle='--')

## 2022 ##
plt.scatter(total_spend_2022, total_contri_2022, label='2022 Spend', color='orange')
plt.axvline(total_spend_2022, color='orange', linestyle='--')

# Labels, title, and legend for the second subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Show the entire figure with both subplots
plt.tight_layout()  # Adjusts spacing to prevent overlap
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.4 Email

# COMMAND ----------

## Change it for Diff. Channels ##
spend_col = 'Email_Membership_Spend'
prospect_col = 'Email_Membership'

# params = None
# params = [0.31,0.87]
params = [0.31,0.87]

# COMMAND ----------

#####################################################################################
#####################################################################################
temp_df = df[df[spend_col]>0]
temp_df.reset_index(inplace=True)



X,y,x_data,y_data,x_minmax,y_minmax = input_data(temp_df,spend_col,prospect_col)
y_fit, y_fit_inv, Kd_fit, n_fit = hill_func(x_data,y_data, params)
output_data = data_output(prospect_col,temp_df,X,y,y_fit_inv)
#####################################################################################
#####################################################################################


#######################################
###### Calibrating Output Data ########
#######################################

total_spend_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][spend_col].sum() 
total_contri_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][prospect_col+'_Contri'].sum()

output_data_2023 = output_data[pd.to_datetime(output_data['Date']).dt.year==2023]
total_spend_2023 = output_data_2023[spend_col].sum() 
total_contri_2023 = output_data_2023[prospect_col+'_Contri'].sum()


num_of_rows = len(output_data_2023[output_data_2023[spend_col]>0])
factor = total_contri_2023/func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,1)[0][0]

output_data['calibrated_spend'] = output_data[spend_col]*num_of_rows
output_data['calibrated_fit'] = output_data['Fit_Data']*num_of_rows*factor





########################
###### Get mROI ########
########################


ctv = 95.19
mROI = (func5(total_spend_2023+1,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0] - func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0])*ctv 

print('k: ',Kd_fit)
print('n: ', n_fit)
print('mROI', mROI)


print("##########################")

print("Yearly Spend for 2022 is: ", total_spend_2022)
print("Yearly Spend for 2023 is: ", total_spend_2023)
print("Yearly Contribution for 2023 is: ", total_contri_2023)

### Saving in Dataframe ###
param_list.append({'Channel':prospect_col, 'k':Kd_fit, 'n':n_fit, 'mROI':mROI, 'Total_Spend_2023':total_spend_2023, 'Total_Contri_2023':total_contri_2023, 'x_min':x_minmax.data_min_[0], 'x_max': x_minmax.data_max_[0], 'y_min': y_minmax.data_min_[0], 'y_max': y_minmax.data_max_[0], 'num_rows': num_of_rows, 'factor':factor })

##################################
############## Plot ##############
##################################



# Set up a single figure for two subplots
plt.figure(figsize=(15,12))

# First subplot: Original data and fitted curve
plt.subplot(2, 1, 1)
plt.scatter(output_data[spend_col], output_data[prospect_col+'_Contri'], label='MMM Estimated Data')
plt.scatter(output_data[spend_col], output_data['Fit_Data'], label='Hill Equation Fit Data', color='red')

# Labels, title, and legend for the first subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Second subplot: Average monthly spend for 2023
plt.subplot(2, 1, 2)
plt.scatter(output_data['calibrated_spend'], output_data['calibrated_fit'], label='Hill Equation Fit Data')

## 2023 ##
plt.scatter(total_spend_2023, total_contri_2023, label='2023 Spend', color='green')
plt.axvline(total_spend_2023, color='green', linestyle='--')

## 2022 ##
plt.scatter(total_spend_2022, total_contri_2022, label='2022 Spend', color='orange')
plt.axvline(total_spend_2022, color='orange', linestyle='--')

# Labels, title, and legend for the second subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Show the entire figure with both subplots
plt.tight_layout()  # Adjusts spacing to prevent overlap
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.5 Display

# COMMAND ----------

## Change it for Diff. Channels ##
spend_col = 'Display_Membership_Spend'
prospect_col = 'Display_Membership'

params = None
# params = [7.80,0.20]
params = [7.80,0.20]

# COMMAND ----------

#####################################################################################
#####################################################################################
temp_df = df[df[spend_col]>0]
temp_df.reset_index(inplace=True)



X,y,x_data,y_data,x_minmax,y_minmax = input_data(temp_df,spend_col,prospect_col)
y_fit, y_fit_inv, Kd_fit, n_fit = hill_func(x_data,y_data, params)
output_data = data_output(prospect_col,temp_df,X,y,y_fit_inv)
#####################################################################################
#####################################################################################


#######################################
###### Calibrating Output Data ########
#######################################

total_spend_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][spend_col].sum() 
total_contri_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][prospect_col+'_Contri'].sum()

output_data_2023 = output_data[pd.to_datetime(output_data['Date']).dt.year==2023]
total_spend_2023 = output_data_2023[spend_col].sum() 
total_contri_2023 = output_data_2023[prospect_col+'_Contri'].sum()


num_of_rows = len(output_data_2023[output_data_2023[spend_col]>0])
factor = total_contri_2023/func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,1)[0][0]

output_data['calibrated_spend'] = output_data[spend_col]*num_of_rows
output_data['calibrated_fit'] = output_data['Fit_Data']*num_of_rows*factor





########################
###### Get mROI ########
########################


ctv = 95.19
mROI = (func5(total_spend_2023+1,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0] - func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0])*ctv 

print('k: ',Kd_fit)
print('n: ', n_fit)
print('mROI', mROI)


print("##########################")

print("Yearly Spend for 2022 is: ", total_spend_2022)
print("Yearly Spend for 2023 is: ", total_spend_2023)
print("Yearly Contribution for 2023 is: ", total_contri_2023)

### Saving in Dataframe ###
param_list.append({'Channel':prospect_col, 'k':Kd_fit, 'n':n_fit, 'mROI':mROI, 'Total_Spend_2023':total_spend_2023, 'Total_Contri_2023':total_contri_2023, 'x_min':x_minmax.data_min_[0], 'x_max': x_minmax.data_max_[0], 'y_min': y_minmax.data_min_[0], 'y_max': y_minmax.data_max_[0], 'num_rows': num_of_rows, 'factor':factor })

##################################
############## Plot ##############
##################################



# Set up a single figure for two subplots
plt.figure(figsize=(15,12))

# First subplot: Original data and fitted curve
plt.subplot(2, 1, 1)
plt.scatter(output_data[spend_col], output_data[prospect_col+'_Contri'], label='MMM Estimated Data')
plt.scatter(output_data[spend_col], output_data['Fit_Data'], label='Hill Equation Fit Data', color='red')

# Labels, title, and legend for the first subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Second subplot: Average monthly spend for 2023
plt.subplot(2, 1, 2)
plt.scatter(output_data['calibrated_spend'], output_data['calibrated_fit'], label='Hill Equation Fit Data')

## 2023 ##
plt.scatter(total_spend_2023, total_contri_2023, label='2023 Spend', color='green')
plt.axvline(total_spend_2023, color='green', linestyle='--')

## 2022 ##
plt.scatter(total_spend_2022, total_contri_2022, label='2022 Spend', color='orange')
plt.axvline(total_spend_2022, color='orange', linestyle='--')

# Labels, title, and legend for the second subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Show the entire figure with both subplots
plt.tight_layout()  # Adjusts spacing to prevent overlap
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.6 Search

# COMMAND ----------

## Change it for Diff. Channels ##
spend_col = 'Search_Membership_Spend'
prospect_col = 'Search_Membership'


# params = None
# params = [0.129,0.441]
params = [0.129,0.441]

# COMMAND ----------

#####################################################################################
#####################################################################################
temp_df = df[df[spend_col]>0]
temp_df.reset_index(inplace=True)



X,y,x_data,y_data,x_minmax,y_minmax = input_data(temp_df,spend_col,prospect_col)
y_fit, y_fit_inv, Kd_fit, n_fit = hill_func(x_data,y_data, params)
output_data = data_output(prospect_col,temp_df,X,y,y_fit_inv)
#####################################################################################
#####################################################################################


#######################################
###### Calibrating Output Data ########
#######################################

total_spend_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][spend_col].sum() 
total_contri_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][prospect_col+'_Contri'].sum()

output_data_2023 = output_data[pd.to_datetime(output_data['Date']).dt.year==2023]
total_spend_2023 = output_data_2023[spend_col].sum() 
total_contri_2023 = output_data_2023[prospect_col+'_Contri'].sum()


num_of_rows = len(output_data_2023[output_data_2023[spend_col]>0])
factor = total_contri_2023/func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,1)[0][0]

output_data['calibrated_spend'] = output_data[spend_col]*num_of_rows
output_data['calibrated_fit'] = output_data['Fit_Data']*num_of_rows*factor





########################
###### Get mROI ########
########################


ctv = 95.19
mROI = (func5(total_spend_2023+1,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0] - func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0])*ctv 

print('k: ',Kd_fit)
print('n: ', n_fit)
print('mROI', mROI)


print("##########################")

print("Yearly Spend for 2022 is: ", total_spend_2022)
print("Yearly Spend for 2023 is: ", total_spend_2023)
print("Yearly Contribution for 2023 is: ", total_contri_2023)

### Saving in Dataframe ###
param_list.append({'Channel':prospect_col, 'k':Kd_fit, 'n':n_fit, 'mROI':mROI, 'Total_Spend_2023':total_spend_2023, 'Total_Contri_2023':total_contri_2023, 'x_min':x_minmax.data_min_[0], 'x_max': x_minmax.data_max_[0], 'y_min': y_minmax.data_min_[0], 'y_max': y_minmax.data_max_[0], 'num_rows': num_of_rows, 'factor':factor })

##################################
############## Plot ##############
##################################



# Set up a single figure for two subplots
plt.figure(figsize=(15,12))

# First subplot: Original data and fitted curve
plt.subplot(2, 1, 1)
plt.scatter(output_data[spend_col], output_data[prospect_col+'_Contri'], label='MMM Estimated Data')
plt.scatter(output_data[spend_col], output_data['Fit_Data'], label='Hill Equation Fit Data', color='red')

# Labels, title, and legend for the first subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Second subplot: Average monthly spend for 2023
plt.subplot(2, 1, 2)
plt.scatter(output_data['calibrated_spend'], output_data['calibrated_fit'], label='Hill Equation Fit Data')

## 2023 ##
plt.scatter(total_spend_2023, total_contri_2023, label='2023 Spend', color='green')
plt.axvline(total_spend_2023, color='green', linestyle='--')

## 2022 ##
plt.scatter(total_spend_2022, total_contri_2022, label='2022 Spend', color='orange')
plt.axvline(total_spend_2022, color='orange', linestyle='--')

# Labels, title, and legend for the second subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Show the entire figure with both subplots
plt.tight_layout()  # Adjusts spacing to prevent overlap
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.7 Social

# COMMAND ----------

## Change it for Diff. Channels ##
spend_col = 'Social_Membership_Spend'
prospect_col = 'Social_Membership'

# params = None
# params = [0.63,0.6]
params = [0.63,0.6]

# COMMAND ----------

#####################################################################################
#####################################################################################
temp_df = df[df[spend_col]>0]
temp_df.reset_index(inplace=True)



X,y,x_data,y_data,x_minmax,y_minmax = input_data(temp_df,spend_col,prospect_col)
y_fit, y_fit_inv, Kd_fit, n_fit = hill_func(x_data,y_data, params)
output_data = data_output(prospect_col,temp_df,X,y,y_fit_inv)
#####################################################################################
#####################################################################################


#######################################
###### Calibrating Output Data ########
#######################################

total_spend_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][spend_col].sum() 
total_contri_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][prospect_col+'_Contri'].sum()

output_data_2023 = output_data[pd.to_datetime(output_data['Date']).dt.year==2023]
total_spend_2023 = output_data_2023[spend_col].sum() 
total_contri_2023 = output_data_2023[prospect_col+'_Contri'].sum()


num_of_rows = len(output_data_2023[output_data_2023[spend_col]>0])
factor = total_contri_2023/func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,1)[0][0]

output_data['calibrated_spend'] = output_data[spend_col]*num_of_rows
output_data['calibrated_fit'] = output_data['Fit_Data']*num_of_rows*factor





########################
###### Get mROI ########
########################


ctv = 95.19
mROI = (func5(total_spend_2023+1,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0] - func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0])*ctv 

print('k: ',Kd_fit)
print('n: ', n_fit)
print('mROI', mROI)


print("##########################")

print("Yearly Spend for 2022 is: ", total_spend_2022)
print("Yearly Spend for 2023 is: ", total_spend_2023)
print("Yearly Contribution for 2023 is: ", total_contri_2023)

### Saving in Dataframe ###
param_list.append({'Channel':prospect_col, 'k':Kd_fit, 'n':n_fit, 'mROI':mROI, 'Total_Spend_2023':total_spend_2023, 'Total_Contri_2023':total_contri_2023, 'x_min':x_minmax.data_min_[0], 'x_max': x_minmax.data_max_[0], 'y_min': y_minmax.data_min_[0], 'y_max': y_minmax.data_max_[0], 'num_rows': num_of_rows, 'factor':factor })

##################################
############## Plot ##############
##################################



# Set up a single figure for two subplots
plt.figure(figsize=(15,12))

# First subplot: Original data and fitted curve
plt.subplot(2, 1, 1)
plt.scatter(output_data[spend_col], output_data[prospect_col+'_Contri'], label='MMM Estimated Data')
plt.scatter(output_data[spend_col], output_data['Fit_Data'], label='Hill Equation Fit Data', color='red')

# Labels, title, and legend for the first subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Second subplot: Average monthly spend for 2023
plt.subplot(2, 1, 2)
plt.scatter(output_data['calibrated_spend'], output_data['calibrated_fit'], label='Hill Equation Fit Data')

## 2023 ##
plt.scatter(total_spend_2023, total_contri_2023, label='2023 Spend', color='green')
plt.axvline(total_spend_2023, color='green', linestyle='--')

## 2022 ##
plt.scatter(total_spend_2022, total_contri_2022, label='2022 Spend', color='orange')
plt.axvline(total_spend_2022, color='orange', linestyle='--')

# Labels, title, and legend for the second subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Show the entire figure with both subplots
plt.tight_layout()  # Adjusts spacing to prevent overlap
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2.8 TV

# COMMAND ----------

## Change it for Diff. Channels ##
spend_col = 'TV_Membership_Spend'
prospect_col = 'TV_Membership'

# params = None
# params = [0.49,0.38]
params = [0.49,0.38]

# COMMAND ----------

#####################################################################################
#####################################################################################
temp_df = df[df[spend_col]>0]
temp_df.reset_index(inplace=True)



X,y,x_data,y_data,x_minmax,y_minmax = input_data(temp_df,spend_col,prospect_col)
y_fit, y_fit_inv, Kd_fit, n_fit = hill_func(x_data,y_data, params)
output_data = data_output(prospect_col,temp_df,X,y,y_fit_inv)
#####################################################################################
#####################################################################################


#######################################
###### Calibrating Output Data ########
#######################################

total_spend_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][spend_col].sum() 
total_contri_2022 = output_data[pd.to_datetime(output_data['Date']).dt.year==2022][prospect_col+'_Contri'].sum()

output_data_2023 = output_data[pd.to_datetime(output_data['Date']).dt.year==2023]
total_spend_2023 = output_data_2023[spend_col].sum() 
total_contri_2023 = output_data_2023[prospect_col+'_Contri'].sum()


num_of_rows = len(output_data_2023[output_data_2023[spend_col]>0])
factor = total_contri_2023/func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,1)[0][0]

output_data['calibrated_spend'] = output_data[spend_col]*num_of_rows
output_data['calibrated_fit'] = output_data['Fit_Data']*num_of_rows*factor





########################
###### Get mROI ########
########################


ctv = 95.19
mROI = (func5(total_spend_2023+1,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0] - func5(total_spend_2023,num_of_rows,x_minmax, y_minmax, Kd_fit, n_fit,factor)[0][0])*ctv 

print('k: ',Kd_fit)
print('n: ', n_fit)
print('mROI', mROI)


print("##########################")

print("Yearly Spend for 2022 is: ", total_spend_2022)
print("Yearly Spend for 2023 is: ", total_spend_2023)
print("Yearly Contribution for 2023 is: ", total_contri_2023)

### Saving in Dataframe ###
param_list.append({'Channel':prospect_col, 'k':Kd_fit, 'n':n_fit, 'mROI':mROI, 'Total_Spend_2023':total_spend_2023, 'Total_Contri_2023':total_contri_2023, 'x_min':x_minmax.data_min_[0], 'x_max': x_minmax.data_max_[0], 'y_min': y_minmax.data_min_[0], 'y_max': y_minmax.data_max_[0], 'num_rows': num_of_rows, 'factor':factor })

##################################
############## Plot ##############
##################################



# Set up a single figure for two subplots
plt.figure(figsize=(15,12))

# First subplot: Original data and fitted curve
plt.subplot(2, 1, 1)
plt.scatter(output_data[spend_col], output_data[prospect_col+'_Contri'], label='MMM Estimated Data')
plt.scatter(output_data[spend_col], output_data['Fit_Data'], label='Hill Equation Fit Data', color='red')

# Labels, title, and legend for the first subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Second subplot: Average monthly spend for 2023
plt.subplot(2, 1, 2)
plt.scatter(output_data['calibrated_spend'], output_data['calibrated_fit'], label='Hill Equation Fit Data')

## 2023 ##
plt.scatter(total_spend_2023, total_contri_2023, label='2023 Spend', color='green')
plt.axvline(total_spend_2023, color='green', linestyle='--')

## 2022 ##
plt.scatter(total_spend_2022, total_contri_2022, label='2022 Spend', color='orange')
plt.axvline(total_spend_2022, color='orange', linestyle='--')

# Labels, title, and legend for the second subplot
plt.xlabel('Spend')
plt.ylabel('Contribution')
plt.title(f'{prospect_col}')
plt.legend()

# Show the entire figure with both subplots
plt.tight_layout()  # Adjusts spacing to prevent overlap
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Retrieving Saved Data

# COMMAND ----------


## Converting List to Df ##
param_df = pd.DataFrame(param_list)

## Saving Dataframe ##
param_df.to_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_membership_curves_params.csv')

## Reading Dataframe ##
param_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_optimization_membership_curves_params.csv').drop('Unnamed: 0',axis=1)
param_df.display()

# COMMAND ----------



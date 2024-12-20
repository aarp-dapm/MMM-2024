# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Data

# COMMAND ----------

## Reading Model ##
model_df = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_Leadgen_SubModal_df.csv').drop('Unnamed: 0',axis=1)
model_df.head()

# COMMAND ----------

##############################
##############################

model_df['Radio_Spend_AdStock6L2Wk70Ad_Power50_2023'] = model_df.apply(lambda x: x['Radio_Spend_AdStock6L2Wk70Ad_Power50'] if pd.to_datetime(x['Date']).year==2023  else 0, axis=1)

##############################
###############################

# COMMAND ----------

## Reading Model ##
model_path =  '/dbfs/blend360/sandbox/mmm/model/MMM_StatsModel_submodel_LeadgenVo3.pkl'
with open(model_path, "rb") as f:
  model = pickle.load(f)

# COMMAND ----------

## Re-reading INput Variables from Model Object ##
input_vars = list(model.params.index)
input_vars.remove('Intercept')
input_vars.remove('DmaCode Var')
input_vars

# COMMAND ----------

# MAGIC %md
# MAGIC # Dignostics 

# COMMAND ----------

## Creating Model Predict ##
joins = model_df[['DmaCode', 'Date','Leadgen_contri_norm']]
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


print(f"Mean Absolute Percentage Error (MAPE): {mape}")
print(f"R-squared: {r_squared}")
print(f"Skewness: {skewness}")
print(f"Kurtosis: {kurt}")
print(f"DW Stat: {dw_stat}")
print(f"Condition Number: {np.linalg.cond(model.model.exog)}")


plt.tight_layout()
plt.show()

# COMMAND ----------

#Model Summary
Summary_Df = model.summary().tables[1].reset_index()
# Summary_Df = pd.concat([model.params.rename('Coeff.'), model.pvalues.rename('P Values'), model.bse.rename('Std. Err.'),model.conf_int(alpha=0.05, cols=None).rename(columns={0:'[.025', 1:'.975]'})], axis=1).round(3).reset_index()
Summary_Df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Analysing for Multi-Collinearity

# COMMAND ----------

# MAGIC %md
# MAGIC ## Condition Index Calculation

# COMMAND ----------



def colldiag(df, scale=True, center=False, add_intercept=True):
    """
    Performs collinearity diagnostics on a DataFrame.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing the data for collinearity diagnostics.

    scale : bool, optional (default=True)
        Whether to scale the variables. Default is True.

    center : bool, optional (default=False)
        Whether to center the variables. Default is False.

    add_intercept : bool, optional (default=True)
        Whether to add an intercept term to the model. Default is True.

    Returns:
    --------
    DataFrame
        A DataFrame containing the condition index and variance decomposition proportion.

    """

    # If centering, intercept should not be added
    if center:
        add_intercept = False

    # Initialize result dictionary
    result = {}

    # Copy DataFrame to avoid modifying original
    X = df.copy()

    # Drop rows with missing values
    X = X.dropna()

    # Scale and/or center the data
    X = scale_default(X, scale=scale, center=center)

    # Add intercept if requested
    if add_intercept:
        X.insert(0, 'intercept', 1)

    # Singular Value Decomposition (SVD)
    svdX = np.linalg.svd(X)

    # Calculate Condition Index
    condindx = svdX[1][0] / svdX[1]

    # Calculate Variance Decomposition Proportion
    Phi = np.square(svdX[2].T @ np.diag(1 / svdX[1]))
    pi = (Phi.T / np.sum(np.abs(Phi.T), axis=0))

    # Store results in dictionary
    result['cond_indx'] = condindx
    result['pi'] = pi

    # Create DataFrame to display results
    df_results = pd.DataFrame({'Condition Index': condindx})
    df_pi = pd.DataFrame(pi, columns=X.columns)

    # Concatenate condition index and variance decomposition proportion DataFrames
    df_results = pd.concat([df_results, df_pi], axis=1)

    return df_results


def scale_default(df, center=True, scale=True):
    """
    Scale and/or center the columns of a DataFrame.

    Parameters:
    -----------
    df : pandas DataFrame
        The input DataFrame containing the data to be scaled and/or centered.

    center : bool or array-like, optional (default=True)
        If True, center the data. If array-like, it should contain the center to be used for each column.
        If False, no centering is performed.

    scale : bool or array-like, optional (default=True)
        If True, scale the data. If array-like, it should contain the scale to be used for each column.
        If False, no scaling is performed.

    Returns:
    --------
    pandas DataFrame
        A DataFrame with scaled and/or centered data.

    Raises:
    -------
    ValueError
        If the length of 'center' or 'scale' does not match the number of columns in the DataFrame.

    """

    # Convert DataFrame to numpy array
    x = df.values

    # Number of columns in the DataFrame
    nc = x.shape[1]

    # Centering
    if isinstance(center, bool):
        if center:
            # Calculate column-wise mean
            center = np.nanmean(x, axis=0)
            # Subtract column-wise mean from each value
            x = x - center
    elif isinstance(center, (int, float)):
        if len(center) == nc:
            # Subtract provided center from each value
            x = x - center
        else:
            raise ValueError("Length of 'center' must equal the number of columns of 'x'")

    # Scaling
    if isinstance(scale, bool):
        if scale:
            # Define function to calculate scale for each column
            def f(v):
                v = v[~np.isnan(v)]  # Remove NaN values
                # Calculate scale using unbiased estimator
                return np.sqrt(np.sum(v**2) / max(1, len(v) - 1))

            # Apply the function column-wise to calculate scale
            scale = np.apply_along_axis(f, axis=0, arr=x)
            # Divide each value by the calculated scale
            x = x / scale
    elif isinstance(scale, (int, float)):
        if len(scale) == nc:
            # Divide each value by the provided scale
            x = x / scale
        else:
            raise ValueError("Length of 'scale' must equal the number of columns of 'x'")

    # Convert scaled data back to DataFrame with original column names
    result_df = pd.DataFrame(x, columns=df.columns)

    # Store center and scale values as attributes of the DataFrame
    if isinstance(center, (int, float)):
        result_df._scaled_center = center
    if isinstance(scale, (int, float)):
        result_df._scaled_scale = scale

    return result_df



# COMMAND ----------

'''
* If you find conditon index exceeding 10, it indicates that the regression coeff. may be unstable due to multicollinearity.
* Focus on variable wuth high variation decomposition values, as they contribute more to the multicollinearity issue.
* Consider strategies to address multi-collinearity such as removing redundant variables, transforming variables or using regualrization  tech like ridge regression.
'''

################################################
####### START: Model Variable Defination #######
################################################  

input_vars = [

'DirectMail_Imps_AdStock6L3Wk70Ad_Power90',
'Print_Imps_AdStock13L1Wk30Ad_Power30',
'AltMedia_Imps_AdStock6L5Wk30Ad_Power70',
'Seasonality_Mar23',
'Seasonality_Apr23',
'Seasonality_Sep23',
'Seasonality_Oct23'
         


 

       ]



################################################
######### END: Model Variable Defination #######
################################################  

model_vars = input_vars


df_cond_indx = colldiag(model_df.groupby('Date')[model_vars].sum())
df_cond_indx.round(2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## VIF Calculation

# COMMAND ----------

X = model_df[model_vars]
# X = sm.add_constant(X) ## Check if there is no intercept

## Creating 
vif_data = pd.DataFrame()
vif_data["feature"] = X.columns
vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]

vif_data.round(2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Contribution Calculation

# COMMAND ----------

## Adding Variable Contribution ##
coeff_value = model.fe_params[input_vars].values
coeff_names = model.fe_params[input_vars].index

input_model = model_df[input_vars].values

## Variable contribution##
contribution = pd.DataFrame(coeff_value*input_model, columns=coeff_names)

## Adding Joins and Prediction ##
contribution = pd.concat([pred_df, contribution], axis=1)

## Adding Intercept Effect ##
contribution['Fixed Dma Effect'] = model.fe_params['Intercept'] ## Adding fixed intercept effect 

random_effect_dict = {key:value.values[0] for key,value in model.random_effects.items()}
contribution['Random Dma Effect'] = contribution['DmaCode'].map(random_effect_dict)  ## Adding Random Effect ##

## Adding Residuals ##
contribution['Residual'] = model.resid

## Creating Additional Columns ##
# contribution.insert(1,'Dma', contribution['DmaCode'].str.split("-").str[0])
# contribution.insert(2,'Age', contribution['DmaCode'].str.split("-").str[1])

## Modifying Table for Analysis purpose ##
contribution['ratio_resid_join'] = contribution['Residual']/contribution['Joins']

## Adding Year ##
contribution['Year'] = pd.to_datetime(contribution['Date']).dt.year

## Display Data ##
contribution.head(100).display()

# COMMAND ----------

# ## Save Agg. Analysis Dataframe ##
# contribution.to_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_Leadgen_submodel_results.csv')

# ## Read Agg. Analysis Dataframe ##
# contribution = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/mmm2_Leadgen_submodel_results.csv').drop('Unnamed: 0',axis=1)
# contribution.display()

# COMMAND ----------

contribution.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregated Analysis Dataframe 

# COMMAND ----------

######################################################################################
##################### Enter List of All Model Variables ##############################
######################################################################################

## Model Creation ##
seasoanlity_vars =  [ 'Seasonality_Mar23', 'Seasonality_Apr23',  'Seasonality_Sep23', 'Seasonality_Oct23', ]

Holiday_vars = []

Promo_vars = []

dummy_vars = []

##### Media Vars #####


media_vars = ['DirectMail_Imps_AdStock6L3Wk70Ad_Power90', 'Print_Imps_AdStock13L1Wk30Ad_Power30',  'AltMedia_Imps_AdStock6L5Wk30Ad_Power70']


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

'''
1) Check if spend and contribution scatter plot is roughly at 45 Degrees
2) Check if Random Effect is distributed with the proprotion of population 
3) Plot 
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Contribution File 

# COMMAND ----------

# model_contribution.to_csv('/dbfs/blend360/sandbox/UMM/UMM_Join_Model_Contribution.csv')

# COMMAND ----------



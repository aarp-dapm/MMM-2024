# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/02 Model Development/02 Model Dev/99 LocalUtilityFile"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Reading Data

# COMMAND ----------

## Creating Model Dataframe ##

## Creating New Columns ##
# rawdata_df['DmaCode'] = rawdata_df['DmaCode'].astype('int')
# rawdata_df['Date'] = pd.to_datetime(rawdata_df['Date'])

## Merging ##
# model_df = rawdata_df.merge(transformed_df, on=['DmaCode', 'Date'])

## saving model dataframe ##
# model_df.to_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_df_statsmodel.csv')


# Reading model dataframe #
model_df  = pd.read_csv('/dbfs/blend360/sandbox/mmm/model/MMM_transformed_df_statsmodel.csv').drop('Unnamed: 0',axis=1)
model_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Adding More Variables

# COMMAND ----------

## Adding Year Column ##
# model_df['YearFlag_22'] = model_df['Date'].apply(lambda x: 1 if pd.to_datetime(x).year==2022 else 0)

# Adding flag for 7/11/2022 ##
model_df['dummy_20220711'] = model_df['Date'].apply(lambda x: 1 if x == '2022-07-11' else 0)

## Fill NaN with Zero ##
model_df = model_df.fillna(0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Reading Data for Model Development

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
# MAGIC ## 3.1 Model Creation with priors from previous model Vo3: Dropping Radio, Audio, Affiliate, OOH, Print, DMP

# COMMAND ----------

## Model Creation ##
seasoanlity_vars =  ['Seasonality_Sin2', 'Seasonality_Cos2', 'Seasonality_Sin4', 'Seasonality_Cos4', 'Seasonality_Sin6', 'Seasonality_Cos6'] # Double Check on Fourier Frequencies 

Holiday_vars = ['Holiday_22Christams', 'Holiday_23MemorialDay', 'Holiday_23LaborDay', 'Holiday_23Christams']

Promo_vars = ['PromoEvnt_22MembDrive','PromoEvnt_22MemorialDay', 'PromoEvnt_22LaborDay', 'PromoEvnt_22BlackFriday', 'PromoEvnt_22WsjTodayShow', 'PromoEvnt_22TikTok', 'PromoEvnt_23KaylaCoupon', 'PromoEvnt_23RollingStone']

dummy_vars = ['dummy_20220711']

##### Media Vars #####
best_corr_media_vars = []

prev_model_media_vars = ['Search_Clicks_AdStock6L1Wk80Ad_Power30', 'Email_Imps_AdStock6L1Wk80Ad_Power30', 'LeadGen_Imps_AdStock6L1Wk90Ad_Power90', 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90',
'DirectMail_Imps_AdStock6L4Wk90Ad_Power70', 'Display_Imps_AdStock6L4Wk30Ad_Power30', 'Social_Imps_AdStock6L4Wk30Ad_Power30','TV_Imps_AdStock6L1Wk90Ad_Power30','Video_Imps_AdStock6L2Wk30Ad_Power30']

media_vars = best_corr_media_vars+prev_model_media_vars 

# COMMAND ----------

## Defining Reg Penalty ##
reg_coeff = list({
'Intercept':0, 'Seasonality_Sin2' : 0, 'Seasonality_Cos2' : 0, 'Seasonality_Sin4' : 0, 'Seasonality_Cos4' : 0, 'Seasonality_Sin6' : 0, 'Seasonality_Cos6' : 0,
'Holiday_22Christams' : 0, 'Holiday_23MemorialDay' : 0, 'Holiday_23LaborDay' : 0, 'Holiday_23Christams' : 0,
'PromoEvnt_22MembDrive' : 0, 'PromoEvnt_22MemorialDay' : 0, 'PromoEvnt_22LaborDay' : 0, 'PromoEvnt_22BlackFriday' : 0, 'PromoEvnt_22WsjTodayShow' : 0,
'PromoEvnt_22TikTok' : 0, 'PromoEvnt_23KaylaCoupon' : 0, 'PromoEvnt_23RollingStone' : 0, 'dummy_20220711' : 0,
'Search_Clicks_AdStock6L1Wk80Ad_Power30' : 4, 'Email_Imps_AdStock6L1Wk80Ad_Power30' : 10,'LeadGen_Imps_AdStock6L1Wk90Ad_Power90' : 6, 'AltMedia_Imps_AdStock6L3Wk90Ad_Power90' : 0,
'DirectMail_Imps_AdStock6L4Wk90Ad_Power70' : 4, 'Display_Imps_AdStock6L4Wk30Ad_Power30' : 2, 'Social_Imps_AdStock6L4Wk30Ad_Power30' : 0, 'TV_Imps_AdStock6L1Wk90Ad_Power30' : 0,'Video_Imps_AdStock6L2Wk30Ad_Power30' : 4,
 'DmaCode Var':0
  }.values())

# COMMAND ----------

input_vars = seasoanlity_vars + Holiday_vars + Promo_vars + dummy_vars + media_vars
input_vars_str = " + ".join(input_vars)


## Model Fit ##
model = smf.mixedlm("Joins ~ {}".format(input_vars_str), data = model_df, groups =  'DmaCode' ).fit_regularized(alpha=reg_coeff)

# COMMAND ----------

# ## Saving Model ##
# model_path = ''
# with open(model_path,"wb") as f:
#   pickle.dump(model, f)


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
print(f"Condition Number: {np.linalg.cond(model.model.exog)}")


plt.tight_layout()
plt.show()

# COMMAND ----------

model.fe_params

# COMMAND ----------

# model.pvalues
# model.params
# model.conf_int(alpha=0.05, cols=None)
# model.bse

# COMMAND ----------

#Model Summary
model.summary().tables[1].reset_index().display()

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

## Display Data ##
contribution.head(100).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving Contribution File 

# COMMAND ----------



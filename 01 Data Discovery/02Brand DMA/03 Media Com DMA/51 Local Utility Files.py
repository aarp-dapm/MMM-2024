# Databricks notebook source
## Global Utility Function ##

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

## Local Utility Files ##

# COMMAND ----------

## Creating QoQ Map ##

folder_path = "dbfs:/blend360/sandbox/mmm/brand_offline/tv/QoQ Data for Brand TV/"

QoQ_map = {}

for i in dbutils.fs.ls(folder_path): 

  ## Reading File ##
  temp = read_excel(i.path).select('Impressions', 'Media Market Region Code').collect() 
  
  ## Creating Temp Dict ##
  key = i.name.split(".")[0]
  temp_dict = {str(int(i['Media Market Region Code'])):i['Impressions'] for i in temp}
  val_sum = np.sum(list(temp_dict.values()))
  
  ## Normalizing Values ##
  QoQ_map[key] = {k:float(v/val_sum) for k,v in temp_dict.items()}

# QoQ_map


# COMMAND ----------

### Creating Dataframe for above List ###
# brand_tv_mltplier = spark.createDataFrame(pd.DataFrame(QoQ_map).reset_index())
# brand_tv_mltplier = brand_tv_mltplier.withColumnRenamed( 'index', 'DMA_Code')
# brand_tv_mltplier.display()


# ### Saving this file ###
# save_df_func(brand_tv_mltplier, "temp.ja_blend_mmm2_BrandOfflineTV_multiplier")

# COMMAND ----------



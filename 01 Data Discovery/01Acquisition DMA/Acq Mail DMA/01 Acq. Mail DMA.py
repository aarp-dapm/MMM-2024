# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

#### PATH ####
FOLDER = "dbfs:/blend360/sandbox/mmm/acq_mail"
RESP_FILE = '2022-2023 Acq Mail Response DMA.xlsx'
MAILVOL_FILE = '2022-2023 Acq Mail MailVolume DMA.xlsx'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data From 04/11/2022

# COMMAND ----------

#### Loading Data ####
resp_file_df = read_excel(FOLDER+'/'+RESP_FILE) 
mailVol_file_df = read_excel(FOLDER+'/'+MAILVOL_FILE) 

## Changing Date format ##
resp_file_df = resp_file_df.withColumn('Date', f.to_date(f.col('Date'), 'M/d/y')).filter(f.year(f.col('Date')).isin([2022, 2023])) 
mailVol_file_df = mailVol_file_df.withColumn('Date',f.to_date(f.col('Date'),'M/d/y')).filter(f.year(f.col('Date')).isin([2022, 2023]))  

#### Making One Single Dataframe ####
df1 = mailVol_file_df.join(resp_file_df, on=['CampaignID', 'PackageCd', 'DmaName', 'Date'], how='left')


### Filtering Data to start from 04/11/2022 ###
'''
Since Response data is starting from 04/11/2022, 
we are filtering mail volume data as well to start from same date 
'''
df1 = df1.filter(f.col('Date')>'2022-04-10')


### Sub Channel Classification ###
df1 = df1.withColumn('job_type', when(f.substring(f.col('PackageCd'),4,1)=='W','WinbackMail').otherwise('DirectMail'))


### Agg Value ###
df1 = df1.groupBy('Date', 'DmaName', 'job_type').agg(f.sum(f.col('MailVolume')).alias('MailVolume'), f.sum(f.col('Response')).alias('Response'))
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Before 04/11/2022

# COMMAND ----------

'''
Reusing Code from Phase 1 Acq. Mail Notebook

'''

df2 = spark.sql("select * from default.acq_mail___winback_model_attrubution_data_output_2020_2022_1_csv")

### Filtering for Columns ###
cols = ['Mail_Date__DMAM_', 'Job_Number', 'Package_Code', 'Package_Demo', 'List_Number', 'List_Name', 'Major_Select', 'Minor_Select', 'Touch',   'Premium_Cost', 'List_Cost', 'Package_Cost', 'Total_Cost', 'Mail_Quantity', 'Actual_Responses']

df2 = df2.select(cols).dropDuplicates()
df2 = df2.withColumn('Date_Mail', f.to_date(f.col('Mail_Date__DMAM_'), "M/d/y")).drop(f.col('Mail_Date__DMAM_'))


#### Append Df for H1 2023 ####
append_df = spark.sql("select * from default.acq_mail_multi_media_model_csv")
append_df = append_df.select(cols).dropDuplicates()
append_df = append_df.withColumn('Date_Mail', f.to_date(f.col('Mail_Date__DMAM_'), "M/d/y")).drop(f.col('Mail_Date__DMAM_'))
append_df = append_df.filter(f.col('Date_Mail')<=f.to_date(f.lit('2023-06-30')))


#### Append Df for H2 2023 ####
append_df2 = spark.sql("select * from default.model_attribution_data_pull_acquisition_mail_jul23___dec23_csv")
append_df2 = append_df2.select(cols).dropDuplicates()
append_df2 = append_df2.withColumn('Date_Mail', f.to_date(f.col('Mail_Date__DMAM_'), "M/d/y")).drop(f.col('Mail_Date__DMAM_'))
append_df2 = append_df2.filter(f.col('Date_Mail')<=f.to_date(f.lit('2023-12-31')))


#### Adding Both Data Source ####
df2 = df2.union(append_df)
df2 = df2.union(append_df2)

############################################################################################
## creating winback flag ##
df2 = df2.withColumn('job_type', when(f.col('Date_Mail')<f.to_date(f.lit('2020-07-01')),\
                                    when(f.substring(f.col('Job_Number'), 3, 2)=='AE','WinbackEmail').\
                                    when(f.substring(f.col('Job_Number'), -1, 1)=='W','WinbackMail').otherwise('DirectMail')).\
                               otherwise(when(f.substring(f.col('Package_Code'), 4, 1)=='W','WinbackMail').\
                                        when(f.substring(f.col('Package_Code'), 4, 1)=='E','WinbackEmail').otherwise('DirectMail')))




#### Filters #### 
df2 = df2.filter(f.col('Date_Mail').between('2022-01-01', '2022-04-10')) ## Date Filter ##
df2 = df2.filter(f.col('job_type').isin(['WinbackMail', 'DirectMail']))


#### Adding New Col ####
df2 = df2.withColumn('DmaName',f.lit('National'))

#### Renaming Cols ####
df2 = rename_cols_func(df2, {'Date_Mail':'Date', 'Mail_Quantity':'MailVolume', 'Actual_Responses':'Response'})

#### Selecting cols ####
cols = ['Date', 'DmaName', 'job_type', 'MailVolume', 'Response']
df2 = df2.select(cols)
df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combining both data ### 

# COMMAND ----------

### Union Both DF ###
df = df2.union(df1)

### Creating Week COlumns ###
df = df.withColumn('Date', f.date_format(f.date_sub(f.col('Date'),f.dayofweek(f.col('Date'))-2),'yyyy-MM-dd'))

### Agg Value ###
df = df.groupBy('Date', 'DmaName', 'job_type').agg(f.sum(f.col('MailVolume')).alias('MailVolume'), f.sum(f.col('Response')).alias('Response'))

## Filtering for only Mail ##
df = df.filter(f.col('job_type').isin(['DirectMail', 'WinbackMail']))
df.display()

# COMMAND ----------

### Estimating Spend Column Value ###
@udf
def get_spend(job_type, MailVolume, Response):

  ## DirectMail ##
  prem_cost_dm = 6.43 
  list_cost_dm = 0.0124
  pkg_cost_dm = 0.310

  ## WinBackMail ##
  prem_cost_wb = 6.43 
  list_cost_wb = 0
  pkg_cost_wb = 0.310

  '''
  estimated spend = (list_cost*MailVolume)+(prem_cost*Response)+(pkg_cost*MailVolume)

  '''

  if job_type == 'DirectMail':
    return (list_cost_dm*MailVolume)+(prem_cost_dm*Response)+(pkg_cost_dm*MailVolume)
  
  if job_type == 'WinbackMail':
    return (list_cost_wb*MailVolume)+(prem_cost_wb*Response)+(pkg_cost_wb*MailVolume)
  

## Get Spend as Column Value ##
df =df.withColumn('Spend',get_spend(f.col('job_type'), f.col('MailVolume'), f.col('Response')))
df.display()


# COMMAND ----------

### Making Data Structre aligned for downstream code ###

## Adding New Column ##
df = df.withColumn('Campaign', f.col('job_type'))

## Renaming Few Columns ##
df = df.withColumnRenamed('job_type','SubChannel')

## Re-Ordering Columns ##
cols  =['Date', 'DmaName', 'SubChannel', 'Campaign', 'Spend', 'MailVolume', 'Response']
final_model_df = df.select(cols)
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

dma_mapping = {'WATERTOWN': 549,
 'AMARILLO': 634,
 'IDAHO FALLS - POCATELLO (JCKSN)': 758,
 'MARQUETTE': 553,
 'PORTLAND - AUBURN': 500,
 'FT. MYERS - NAPLES': 571,
 'MINOT - BISMARCK - DICKINSON(WLSTN)': 687,
 'MERIDIAN': 711,
 'SAVANNAH': 507,
 'INDIANAPOLIS': 527,
 'ATLANTA': 524,
 'TRAVERSE CITY - CADILLAC': 540,
 'WACO - TEMPLE - BRYAN': 625,
 'National': 1000,
 'MEMPHIS': 640,
 'BOWLING GREEN': 736,
 'WHEELING - STEUBENVILLE': 554,
 'RENO': 811,
 'MONTEREY - SALINAS': 828,
 'LITTLE ROCK - PINE BLUFF': 693,
 'PRESQUE ISLE': 552,
 'SPRINGFIELD, MO': 619,
 'EL PASO (LAS CRUCES)': 765,
 'BILOXI - GULFPORT': 746,
 'DAVENPORT - R. ISLAND - MOLINE': 682,
 'MACON': 503,
 'TALLAHASSEE - THOMASVILLE': 530,
 'GREEN BAY - APPLETON': 658,
 'DAYTON': 542,
 'JONESBORO': 734,
 'JOPLIN - PITTSBURG': 603,
 'ANCHORAGE': 743,
 'DENVER': 751,
 'RALEIGH - DURHAM (FAYETVLLE)': 560,
 'CLEVELAND - AKRON (CANTON)': 510,
 'CHEYENNE - SCOTTSBLUF': 759,
 'EUREKA': 802,
 'FLINT - SAGINAW - BAY CITY': 513,
 'CHARLESTON, SC': 519,
 'TYLER - LONGVIEW (LFKN & NCGD)': 709,
 'PARKERSBURG': 597,
 'COLUMBUS - TUPELO - WEST POINT': 673,
 'LAREDO': 749,
 'TOLEDO': 547,
 'SIOUX FALLS (MITCHELL)': 725,
 'BANGOR': 537,
 'SANTABARBRA - SANMAR - SANLUOB': 855,
 'BALTIMORE': 512,
 'FRESNO - VISALIA': 866,
 'HARRISONBURG': 569,
 'FT. WAYNE': 509,
 'MADISON': 669,
 'HOUSTON': 618,
 'TULSA': 671,
 'JACKSONVILLE': 561,
 'OTTUMWA - KIRKSVILLE': 631,
 'CHAMPAIGN & SPRNGFLD - DECATUR': 648,
 'BEND, OR': 821,
 'SAN ANGELO': 661,
 'BOSTON (MANCHESTER)': 506,
 'GREENSBORO - H. POINT - W. SALEM': 518,
 'ALBANY - SCHENECTADY - TROY': 532,
 'TOPEKA': 605,
 'LA CROSSE - EAU CLAIRE': 702,
 'ROANOKE - LYNCHBURG': 573,
 'SACRAMNTO - STKTN - MODESTO': 862,
 'MIAMI - FT. LAUDERDALE': 528,
 'BAKERSFIELD': 800,
 'HATTIESBURG - LAUREL': 710,
 'ROCHESTER, NY': 538,
 'SPRINGFIELD - HOLYOKE': 543,
 'MILWAUKEE': 617,
 'BURLINGTON - PLATTSBURGH': 523,
 'AUSTIN': 635,
 'DOTHAN': 606,
 'VICTORIA': 626,
 'MINNEAPOLIS - ST. PAUL': 613,
 'WEST PALM BEACH - FT. PIERCE': 548,
 'ALBUQUERQUE - SANTA FE': 790,
 'NASHVILLE': 659,
 'COLUMBIA, SC': 546,
 'LOUISVILLE': 529,
 'PITTSBURGH': 508,
 'SAN ANTONIO': 641,
 'FT. SMITH - FAY - SPRNGDL - RGRS': 670,
 'QUINCY - HANNIBAL - KEOKUK': 717,
 'RAPID CITY': 764,
 'LEXINGTON': 541,
 'ST. JOSEPH': 638,
 'FAIRBANKS': 745,
 'MONROE - EL DORADO': 628,
 'SIOUX CITY': 624,
 'ODESSA - MIDLAND': 633,
 'AUGUSTA - AIKEN': 520,
 'ALBANY, GA': 525,
 'CORPUS CHRISTI': 600,
 'BUTTE - BOZEMAN': 754,
 'COLUMBIA - JEFFERSON CITY': 604,
 'LAFAYETTE, IN': 582,
 'CHARLOTTE': 517,
 'WAUSAU - RHINELANDER': 705,
 'CHICO - REDDING': 868,
 'GRAND RAPIDS - KALMZOO - B. CRK': 563,
 'NEW ORLEANS': 622,
 'BLUEFIELD - BECKLEY - OAK HILL': 559,
 'GREAT FALLS': 755,
 'BIRMINGHAM (ANN & TUSC)': 630,
 'JACKSON, TN': 639,
 'PANAMA CITY': 656,
 'WASHINGTON, DC (HAGRSTWN)': 511,
 'BATON ROUGE': 716,
 'CINCINNATI': 515,
 'CHICAGO': 602,
 'ST. LOUIS': 609,
 'HELENA': 766,
 'FARGO - VALLEY CITY': 724,
 'MEDFORD - KLAMATH FALLS': 813,
 'HONOLULU': 744,
 'DULUTH - SUPERIOR': 676,
 'DETROIT': 505,
 'SALISBURY': 576,
 'NORFOLK - PORTSMTH - NEWPT NWS': 544,
 'TERRE HAUTE': 581,
 'BINGHAMTON': 502,
 'TAMPA - ST. PETE (SARASOTA)': 539,
 'MISSOULA': 762,
 'WICHITA FALLS & LAWTON': 627,
 'PADUCAH - CAPE GIRAR D - HARSBG': 632,
 'LUBBOCK': 651,
 'LANSING': 551,
 'ROCKFORD': 610,
 'ALEXANDRIA, LA': 644,
 'NORTH PLATTE': 740,
 'ALPENA': 583,
 'LOS ANGELES': 803,
 'GRAND JUNCTION - MONTROSE': 773,
 'MOBILE - PENSACOLA (FT WALT)': 686,
 'MANKATO': 737,
 'PHILADELPHIA': 504,
 'ELMIRA (CORNING)': 565,
 'PORTLAND, OR': 820,
 'BUFFALO': 514,
 'HARLINGEN - WSLCO - BRNSVL - MCA': 636,
 'SAN DIEGO': 825,
 'JOHNSTOWN - ALTOONA - ST COLGE': 574,
 'ORLANDO - DAYTONA BCH - MELBRN': 534,
 'ABILENE - SWEETWATER': 662,
 'BOISE': 757,
 'CEDAR RAPIDS - WTRLO - IWC & DUB': 637,
 'SHREVEPORT': 612,
 'LAKE CHARLES': 643,
 'PEORIA - BLOOMINGTON': 675,
 'HARTFORD & NEW HAVEN': 533,
 'COLUMBUS, OH': 535,
 'PROVIDENCE - NEW BEDFORD': 521,
 'GLENDIVE': 798,
 'WILMINGTON': 550,
 'YOUNGSTOWN': 536,
 'EUGENE': 801,
 'KANSAS CITY': 616,
 'SOUTH BEND - ELKHART': 588,
 'LAFAYETTE, LA': 642,
 'UTICA': 526,
 'SYRACUSE': 555,
 'PHOENIX (PRESCOTT)': 753,
 'GREENVILLE - N. BERN - WASHNGTN': 545,
 'PALM SPRINGS': 804,
 'MONTGOMERY (SELMA)': 698,
 'CASPER - RIVERTON': 767,
 'NEW YORK': 501,
 'LAS VEGAS': 839,
 'WICHITA - HUTCHINSON PLUS': 678,
 'KNOXVILLE': 557,
 'SHERMAN - ADA': 657,
 'COLUMBUS, GA (OPELIKA, AL)': 522,
 'DES MOINES - AMES': 679,
 'GREENVLL - SPART - ASHEVLL - AND': 567,
 'ROCHESTR - MASON CITY - AUSTIN': 611,
 'CLARKSBURG - WESTON': 598,
 'GAINESVILLE': 592,
 'COLORADO SPRINGS - PUEBLO': 752,
 'YUMA - EL CENTRO': 771,
 'HARRISBURG - LNCSTR - LEB - YORK': 566,
 'CHATTANOOGA': 575,
 'ERIE': 516,
 'TRI-CITIES, TN - VA': 531,
 'RICHMOND - PETERSBURG': 556,
 'SALT LAKE CITY': 770,
 'GREENWOOD - GREENVILLE': 647,
 'DALLAS - FT. WORTH': 623,
 'TUCSON (SIERRA VISTA)': 789,
 'SAN FRANCISCO - OAK - SAN JOSE': 807,
 'HUNTSVILLE - DECATUR (FLOR)': 691,
 'BILLINGS': 756,
 'JACKSON, MS': 718,
 'WILKES BARRE - SCRANTON - HZTN': 577,
 'YAKIMA - PASCO - RCHLND - KNNWCK': 810,
 'TWIN FALLS': 760,
 'CHARLOTTESVILLE': 584,
 'SEATTLE - TACOMA': 819,
 'MYRTLE BEACH - FLORENCE': 570,
 'CHARLESTON - HUNTINGTON': 564,
 'JUNEAU': 747,
 'OKLAHOMA CITY': 650,
 'ZANESVILLE': 596,
 'LIMA': 558,
 'BEAUMONT - PORT ARTHUR': 692,
 'EVANSVILLE': 649,
 'LINCOLN & HSTNGS - KRNY': 722,
 'SPOKANE': 881}

# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  
dma_col = "DmaName"
cols = ["Date", "DMA_Code", "SubChannel", "Campaign"]

final_model_df = final_model_df.withColumn("DMA_Code", map_dma_name_to_code(dma_col))
final_model_df = final_model_df.groupBy(cols).agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('MailVolume')).alias('MailVolume'), f.sum(f.col('Response')).alias('Response')).fillna(0, subset=['Spend'])
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

## Seperating National and DMA level information ##
final_model_df_national = final_model_df.filter(f.col('DMA_Code')==1000).drop('DMA_Code')
final_model_df_dma = final_model_df.filter(f.col('DMA_Code')!=1000)




## Creating Key-Value Dataframe ##
data_tuples = [(key,value) for key, value in norm_ch_dma_code_prop.items()]
norm_pop_prop_df = spark.createDataFrame(data_tuples, schema = ['DmaCode', 'Prop'])

## Joining above df with final_model_df ##
final_model_df_national = final_model_df_national.crossJoin(norm_pop_prop_df)

## Dividing Metrics by 210 to distribute data at national level ##
cols = ["Spend", "MailVolume", "Response"]
for col in cols:
  final_model_df_national = final_model_df_national.withColumn(col,f.col(col)*f.col('Prop'))




## Re-arranging COlumns ##
cols = ['Date', 'DmaCode', 'SubChannel', 'Campaign', 'Spend', 'MailVolume', 'Response']
final_model_df_national = final_model_df_national.select(cols)


## Combining back with DMA level Data ##
save_df = final_model_df_national.union(final_model_df_dma)
save_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Some More Processing

# COMMAND ----------

# save_df = save_df.withColumn('SubChannel', f.lit('DirectMail')) ## Updating SubChannel Column: 'WinbackMail' and 'DirectMail' to ---> 'DirectMail'
# save_df = save_df.withColumn('Campaign', f.lit('DirectMail')) ## Updating Campaign Column: 'WinbackMail' and 'DirectMail' to ---> 'DirectMail'

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_AcqMail_DirectWinbackMail" 
save_df_func(save_df, table_name)

# COMMAND ----------

# df = read_table("temp.ja_blend_mmm2_AcqMail_DirectWinbackMail")
# df.groupBy(f.year(f.col('Date')).alias('Year'), f.col('SubChannel')).agg(f.sum(f.col('MailVolume')).alias('MailVolume'), f.sum(f.col('Response')).alias('Response') ).display()
save_df.groupBy(f.year(f.col('Date')).alias('Year'), f.col('SubChannel')).agg(f.sum(f.col('MailVolume')).alias('MailVolume'), f.sum(f.col('Response')).alias('Response'), f.sum(f.col('Spend')).alias('Spend'), ).display()

# COMMAND ----------



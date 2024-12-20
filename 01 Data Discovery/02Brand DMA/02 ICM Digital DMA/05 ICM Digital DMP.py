# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

df = read_excel('dbfs:/blend360/sandbox/mmm/icm_digital/DMP Campaigns 2021 - 2024 Vo2.xlsx')

## Adding New Column ##
df = df.withColumn('SubChannel', f.lit('DMP'))

## Selecting Columns ##
cols = ['Date created', 'DMA region', 'SubChannel', 'Campaign name', 'Amount spent (USD)', 'Impressions', 'Link clicks']
df = df.select(cols).fillna(0, subset=['Amount spent (USD)', 'Impressions', 'Link clicks'])

## Changing Date to same format ##
df = df.withColumn('Date created', f.date_format(f.col('Date created'),'yyyy-MM-dd'))
df = df.withColumn('Date created', f.date_format( f.date_sub(f.col('Date created'), f.dayofweek(f.col('Date created'))-2 ) ,'yyyy-MM-dd'))



## Renaming Columns ##
df = df.withColumnRenamed('Date created', 'Date')
df = df.withColumnRenamed('DMA region', 'DmaName')

df = df.withColumnRenamed('Amount spent (USD)', 'Spend')
df = df.withColumnRenamed('Impressions', 'Imps')
df = df.withColumnRenamed('Link clicks', 'Clicks')
df = df.withColumnRenamed('Campaign name', 'Campaign')


## No Campaign Grouping ##
df = df.withColumn('Campaign', f.col('SubChannel'))

## show ##
df.display()

# COMMAND ----------

### Aggregating metrics ###
cols = ["Date", "DmaName", "SubChannel", "Campaign"]

spend_col = 'Spend'
imps_col = 'Imps'
click_col = 'Clicks'

final_model_df = df.groupBy(cols).agg(f.sum(f.col(spend_col)).alias('spend'), f.sum(f.col(imps_col)).alias('Imps'), f.sum(f.col(click_col)).alias('clicks'))
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

dma_mapping = {
    "Fairbanks": 745,
    "Hartford & New Haven": 533,
    "Bowling Green": 736,
    "Butte-Bozeman": 754,
    "Sherman-Ada": 657,
    "Savannah": 507,
    "Lima": 558,
    "Omaha": 652,
    "Anchorage": 743,
    "Idaho Fals-Pocatllo(Jcksn)": 758,
    "Montgomery-Selma": 698,
    "Ottumwa-Kirksville": 631,
    "El Paso (Las Cruces)": 765,
    "Wheeling-Steubenville": 554,
    "Chico-Redding": 868,
    "Little Rock-Pine Bluff": 693,
    "Monroe-El Dorado": 628,
    "Chattanooga": 575,
    "Great Falls": 755,
    "Birmingham (Ann And Tusc)": 630,
    "Laredo": 749,
    "Columbus, OH": 535,
    "Zanesville": 596,
    "Traverse City-Cadillac": 540,
    "San Antonio": 641,
    "Bakersfield": 800,
    "Greenvll-Spart-Ashevll-And": 545, ## incorrect spelling ##
    "Philadelphia": 504,
    "Lincoln & Hastings-Krny": 722,
    "Louisville": 529,
    "Dayton": 542,
    "Los Angeles": 803,
    "Utica": 526,
    "Wichita-Hutchinson Plus": 678, # Imputed through Google Search #
    "Mobile-Pensacola (Ft Walt)": 686,
    "Presque Isle": 552,
    "Columbus, GA (Opelika, Al)": 522,
    "Dothan": 606,
    "Grand Junction-Montrose": 773,
    "Indianapolis": 527,
    "Champaign&Sprngfld-Decatur": 648,
    "Charleston, SC": 519,
    "Fargo-Valley City": 724,
    "Topeka": 605,
    "Victoria": 626,
    "Nashville": 659,
    "Oklahoma City": 650,
    "San Diego": 825,
    "South Bend-Elkhart": 588,
    "Rockford": 610,
    "Ft. Smith-Fay-Sprngdl-Rgrs": 670,
    "Albany, GA": 525,
    "Detroit": 505,
    "Mankato": 737,
    "Bluefield-Beckley-Oak Hill": 559,
    "Huntsville-Decatur (Flor)": 691,
    "Harrisburg-Lncstr-Leb-York": 566,
    "Springfield-Holyoke": 543,
    "Tri-Cities, TN-VA": 531,
    "Columbia-Jefferson City": 604,
    "Boise": 757,
    "Baton Rouge": 716,
    "Minneapolis-St. Paul": 613,
    "Orlando-Daytona Bch-Melbrn": 534,
    "St. Louis": 609,
    "Beaumont-Port Arthur": 692,
    "Clarksburg-Weston": 598,
    "Colorado Springs-Pueblo": 752,
    "Parkersburg": 597,
    "Myrtle Beach-Florence": 570,
    "Jackson, MS": 639,
    "Youngstown": 536,
    "Abilene-Sweetwater": 662,
    "Evansville": 649,
    "Tyler-Longview(Lfkn&Ncgd)": 709,
    "Memphis": 640,
    "Minot-Bsmrck-Dcknsn(Wlstn)": 687,
    "Harlingen-Wslco-Brnsvl-Mca": 636,
    "Austin": 635,
    "Dallas-Ft. Worth": 623,
    "Grand Rapids-Kalmzoo-B.Crk": 563,
    "Madison": 669,
    "Milwaukee": 617,
    "Casper-Riverton": 767,
    "St. Joseph": 638,
    "Pittsburgh": 508,
    "Richmond-Petersburg": 556,
    "Waco-Temple-Bryan": 625,
    "Green Bay-Appleton": 658,
    "Unknown": 1000, ## Unknown ##
    "Lansing": 551,
    "Tampa-St. Pete (Sarasota)": 539,
    "Biloxi-Gulfport": 746,
    "Cedar Rapids-Wtrlo-Iwc&Dub": 637,
    "Sioux Falls(Mitchell)": 725,
    "Chicago": 602,
    "Lubbock": 651,
    "Seattle-Tacoma": 819,
    "Providence-New Bedford": 521,
    "Albuquerque-Santa Fe": 790,
    "Norfolk-Portsmth-Newpt Nws": 544,
    "Wausau-Rhinelander": 705,
    "Quincy-Hannibal-Keokuk": 717,
    "Toledo": 538,
    "Albany-Schenectady-Troy": 532,
    "Boston (Manchester)": 506,
    "Erie": 516,
    "Corpus Christi": 600,
    "Shreveport": 612,
    "Wilmington": 550,
    "Amarillo": 634,
    "Yuma-El Centro": 771,
    "Raleigh-Durham (Fayetvlle)": 560,
    "Des Moines-Ames": 679,
    "San Francisco-Oak-San Jose": 807,
    "Macon": 503,
    "Glendive": 798,
    "Reno": 811,
    "Atlanta": 524,
    "New Orleans": 622,
    "Las Vegas": 839,
    "Lexington": 541,
    "Bend, OR": 821,
    "Washington, DC (Hagrstwn)": 511,
    "Wilkes Barre-Scranton-Hztn": 577,
    "Charlottesville": 584,
    "Honolulu": 744,
    "Johnstown-Altoona-St Colge": 574,
    "Cheyenne-Scottsbluff": 759,
    "Peoria-Bloomington": 675,
    "Binghamton": 502,
    "San Angelo": 661,
    "Joplin-Pittsburg": 603,
    "Roanoke-Lynchburg": 573,
    "Gainesville": 592,
    "Cleveland-Akron (Canton)": 510,
    "Davenport-R.Island-Moline": 682,
    "Columbus-Tupelo-W Pnt-Hstn": 673,
    "Odessa-Midland": 633,
    "Palm Springs": 804,
    "Rochester-Mason City-Austin": 611,
    "Harrisonburg": 569,
    "Lake Charles": 643,
    "Charleston-Huntington": 564,
    "Hattiesburg-Laurel": 710,
    "Duluth-Superior": 676,
    "Ft. Myers-Naples": 571,
    "Kansas City": 616,
    "Eureka": 802,
    "Spokane": 881,
    "Lafayette, LA": 582,
    "Missoula": 762,
    "Salisbury": 576,
    "Burlington-Plattsburgh": 523,
    "Juneau": 747,
    "Marquette": 553,
    "Billings": 756,
    "Bangor": 537,
    "Greenville-N.Bern-Washngtn": 567,
    "Helena": 766,
    "Greenwood-Greenville": 647,
    "Terre Haute": 581,
    "Columbia, SC": 546,
    "Houston": 618,
    "Medford-Klamath Falls": 813,
    "La Crosse-Eau Claire": 702,
    "Portland-Auburn": 500,
    "Twin Falls": 760,
    "Jacksonville": 561,
    "Jonesboro": 734,
    "Paducah-Cape Girard-Harsbg": 632,
    "Tulsa": 671,
    "Rochester, NY": 514,
    "Sacramnto-Stkton-Modesto": 862,
    "West Palm Beach-Ft. Pierce": 548,
    "Flint-Saginaw-Bay City": 513,
    "Fresno-Visalia": 866,
    "Jackson, TN": 718,
    "Alpena": 583,
    "Buffalo": 514,
    "Knoxville": 557,
    "Miami-Ft. Lauderdale": 528,
    "Tucson (Sierra Vista)": 789,
    "Lafayette, IN": 642,
    "Portland, OR": 820,
    "Wichita Falls & Lawton": 627,
    "Cincinnati": 515,
    "Alexandria, LA": 644,
    "Watertown": 549,
    "Panama City": 656,
    "Santabarbra-Sanmar-Sanluob": 855,
    "Syracuse": 555,
    "Salt Lake City": 770,
    "Baltimore": 512,
    "Yakima-Pasco-Rchlnd-Knnwck": 810,
    "Elmira (Corning)": 565,
    "Ft. Wayne": 509,
    "New York": 501,
    "Sioux City": 624,
    "Charlotte": 517,
    "Monterey-Salinas": 828,
    "North Platte": 740,
    "Eugene": 801,
    "Meridian": 711,
    "Augusta-Aiken": 520,
    "Greensboro-H.Point-W.Salem": 518,
    "Denver": 751,
    "Rapid City": 764,
    "Phoenix (Prescott)": 753,
    "Tallahassee-Thomasville": 530,
    "Springfield, MO": 619
}



# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  
dma_col = "DmaName"
cols = ["Date", "DMA_Code", "SubChannel", "Campaign"]

final_model_df = final_model_df.withColumn("DMA_Code", map_dma_name_to_code(dma_col))
final_model_df = final_model_df.groupBy(cols).agg(f.sum(f.col('spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('clicks')).alias('Clicks'))

## Impute NULL DMAs with '1000' ( to streamline with previous code) ##
final_model_df = final_model_df.fillna('1000',subset=['DMA_Code'])
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

ch_dma_code = list(set([str(value) for key, value in dma_mapping.items()]))
ch_dma_code.remove('1000')

# COMMAND ----------

from functools import reduce
@udf
def get_pop_prop(dma_code):
  return norm_ch_dma_code_prop.get(dma_code)


def new_df_func(col1, col2, col3, col4, col5, col6, col7):

  original_row = Row(Date=col1, DMA_Code=col2, SubChannel=col3, Campaign=col4, Spend=col5, Imps=col6, Clicks=col7)
  new_df = spark.sparkContext.parallelize([original_row]*len(ch_dma_code)).toDF()

  ## National to DMA ##
  labels_udf = f.udf(lambda indx: ch_dma_code[indx-1], StringType()) 
  new_df = new_df.withColumn('row_num', f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
  new_df = new_df.withColumn('DMA_Code', labels_udf('row_num'))

  ## Updating Spend ##
  new_df = new_df.withColumn('Spend', f.col('Spend')*get_pop_prop('DMA_Code'))
  
  ## Updating Impression ##
  new_df = new_df.withColumn('Imps', f.col('Imps')*get_pop_prop('DMA_Code'))

  ## Updating Clicks ##
  new_df = new_df.withColumn('Clicks', f.col('Clicks')*get_pop_prop('DMA_Code'))


  return new_df

###################################################################################

df_national = final_model_df.filter(f.col('DMA_Code')==1000)
df_null = None  

df_remain = final_model_df.filter(f.col('DMA_Code')!=1000)

## Creating empty dataframe ##
schema = final_model_df.schema
df1 = spark.createDataFrame([],schema)

cols = ['Date', 'DMA_Code', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
df_list = []

for row in df_national.collect():

  temp_df = new_df_func(row['Date'], row['DMA_Code'], row['SubChannel'], row['Campaign'], row['Spend'], row['Imps'], row['Clicks'])
  df_list.append(temp_df)
 
print(f"Stacking {len(df_list)} Dataframes")
df1 = reduce(DataFrame.unionAll, df_list)
## union data ##
final_model_df = df_remain.select(cols).union(df1.select(cols))

# COMMAND ----------

# MAGIC %md
# MAGIC # Some More Updates

# COMMAND ----------

## Adding Channel Name ##
cols = ['Date', 'DMA_Code', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.withColumn('Channel', f.lit('DMP')).select(cols) ## Re-arranging Columns 
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File 

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_IcmDigital_Dmp" 
save_df_func(final_model_df, table_name)

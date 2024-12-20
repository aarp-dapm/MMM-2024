# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

#######################################################
################# ARCHIVED CODE #######################
#######################################################
# folder_path= "dbfs:/blend360/sandbox/mmm/drtv/QoQData"
# file_dict = {}
# cols = ['Media Market Region Code', 'Impressions']

# df_22 = None
# df_23 = None

# df = None

# for file in dbutils.fs.ls(folder_path):

#   key = file.path.split("/")[-1].split(".")[0]
#   year = key.split("_")[-1] ## Year Flag ##

#   val = read_csv(file.path).select(cols)
#   val = val.withColumnRenamed('Impressions', key) ## rename column ##
#   val = val.withColumnRenamed('Media Market Region Code', 'DMA_Code') ## rename column ##


  

  

#   ## Appending Columns ##

#   if year == '22':
#     if df_22:
#       df_22 = df_22.join(val, on='DMA_Code', how='left')
#     else:
#       df_22 = val.alias('df_22')


#   else:
#     if df_23:
#       df_23 = df_23.join(val, on='DMA_Code', how='left')
#     else:
#       df_23 = val.alias('df_23')

#   if df:
#     df = df.join(val, on='DMA_Code', how='left')
#   else:
#     df = val.alias('df')

# cols = ['DMA_Code', 'Q1_22', 'Q2_22', 'Q3_22', 'Q4_22', 'Q1_23', 'Q2_23', 'Q3_23', 'Q4_23']
# df = df.select(cols)
# rename_dict = {'Q1_22':'22_q1', 'Q2_22':'22_q2', 'Q3_22':'22_q3', 'Q4_22':'22_q4', 'Q1_23':'23_q1', 'Q2_23':'23_q2', 'Q3_23':'23_q3', 'Q4_23':'23_q4'}
# df = rename_cols_func(df, rename_dict)




# ## normalizing
# cols = [ '22_q1', '22_q2', '22_q3', '22_q4', '23_q1', '23_q2', '23_q3', '23_q4']
# norm_dict = {}
# for col in cols:
#   tot = df.select(col).agg(f.sum(f.col(col))).collect()[0][0]
#   norm_dict[col] = tot



# for col in cols:
#   df = df.withColumn(col, f.col(col)/norm_dict[col])

# df.display()



# table_name = "temp.ja_blend_mmm2_DRTV_multiplier"
# save_df_func(df, table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Alert: Spend for last two weeks of 2022 is missing  

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

## Reading CSV File List ##
excel_file_path = "dbfs:/blend360/sandbox/mmm/drtv/week_data"
file_list = [i for i in list(dbutils.fs.ls(excel_file_path)) if i.size > 0] ## Filtering out folder 

## Appending Data for 2022 and 2023 ##
df_tv_22 = read_csv(file_list[0].path,1)
df_tv_23 = read_csv(file_list[1].path,1)
df = df_tv_22.union(df_tv_23)

## Renaming Columns ##
rename_dict = {'Est. National TV Spend':'Spend', 
               'Parent/Spot Title': 'Campaign',
               'Impressions': 'Imps'
               }
df = rename_cols_func(df, rename_dict)

## Create column with Monday as starting Date  ##
df = df.withColumn('Date',f.date_format( f.date_sub(f.col('Air Date ET'),f.dayofweek(f.col('Air Date ET'))-2), 'yyyy-MM-dd'))


## Imputing Columns ##
df = df.withColumn('DmaName', f.lit('National'))
df = df.withColumn('SubChannel', f.lit('LinearTV'))



## Selecting Columns ##
cols = ['Date', 'DmaName', 'Campaign', 'SubChannel', 'Show Type', 'Daypart', 'Duration', 'Media Type', 'network', 'Show Type', 'Pod', 'Pod Order', 'Spend', 'Imps'] # based on netwrok we can maybe figure out if it is connected tv or linear tv
df.select(cols).display()


# COMMAND ----------

# MAGIC %md
# MAGIC # Campaign Grouping

# COMMAND ----------

'''
Glossary:
Show Type: National - This indicates the show is designed to be broadcast across an entire country.
Media Type: Regional - Despite being nationally targeted, the distribution or availability of this show is limited to specific regions within the country. This could be due to regional interests or broadcast rights.
'''
cols = ['Show Type', 'Daypart', 'Network'] #skipping Media Type

## Grouping Networks and removing spaces from string names ##
df = df.withColumn('NetworkGroup', when(f.col('network').isin(other_drtv_ch_group),"Other").otherwise(f.col('network'))) ## Use 90-10 rule to group channels based on spend
df = df.withColumn('NetworkGroup', f.regexp_replace( 'NetworkGroup'," ", ""))

## Daypart Grouping ##
condition = when(f.col('Daypart').isin(['Early Morning', 'Day Time']), 'Morning').\
            when(f.col('Daypart').isin(['Early Fringe', 'Weekend Afternoon', 'Weekend Day']), 'Afternoon').\
            when(f.col('Daypart').isin(['Prime Time']), 'Prime Time').\
            when(f.col('Daypart').isin(['Late Fringe AM', 'Late Fringe PM', 'Over Night']), 'NightTime').otherwise("NULL")
df = df.withColumn('DaypartGroup', condition)


## Creating One Single Columns ##
# df = df.withColumn("Campaign", f.concat_ws('_', f.col('Show Type'), f.col('DaypartGroup'))) ## Skipping Network Group out as well
df = df.withColumn("Campaign", f.col('Duration').cast("string")) ## Repalced previous definetion of campaign with Duration on 8/16/2024
df.display()

# COMMAND ----------

### Aggregating metrics ###
cols = ["Date", "DmaName", "SubChannel", "Campaign"]

spend_col = 'Spend'
imps_col = 'Imps'
# click_col = 'Clicks' ## No Clix Col

final_model_df = df.groupBy(cols).agg(f.sum(f.col(spend_col)).alias('spend'), f.sum(f.col(imps_col)).alias('Imps'))
final_model_df.display()

# COMMAND ----------

final_model_df.groupBy(f.year(f.col('Date')).alias('Year'), 'Campaign').agg(f.sum(f.col('spend')).alias('spend'), f.sum(f.col('Imps')).alias('Imps')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding Spend Data from Data Shared By Joe-Harr ###

# COMMAND ----------

### Calculating Multiplier for spend data ###
window = Window.partitionBy(f.col('Date')).orderBy(f.col('Date'))
final_model_df = final_model_df.withColumn( 'multiplier', f.count(f.col('Campaign')).over(window) )




### Reading data from Joe Harr's File ###
df2 = read_table("temp.ja_blend_mmm_DRTV").withColumnRenamed('Mail_Date','Date').fillna(0,subset=['DRTV_Monitored_Spend', 'DRTV_Brand_Spend', 'DRTV_Unmonitored_Spend'])
# df2 = df2.withColumn('Cost', f.col('DRTV_Monitored_Spend')+f.col('DRTV_Brand_Spend')+f.col('DRTV_Unmonitored_Spend')).select('Date', 'Cost')
df2 = df2.withColumn('Cost', f.col('DRTV_Monitored_Spend')+f.col('DRTV_Unmonitored_Spend')).select('Date', 'Cost') ## Removing Brand TV as per Joe Harr feedback

## Changing Datatype of Date ##
df2 = df2.withColumn('Date', f.date_format(f.col('Date'),'yyyy-MM-dd'))


### Adding Spend Data to iSPot Data ###
final_model_df = final_model_df.join(df2, on=['Date'], how='left')
final_model_df = final_model_df.withColumn('spend', f.col('Cost')/f.col('multiplier')).select('Date', 'DmaName', 'SubChannel', 'Campaign', 'spend', 'Imps')
final_model_df.display()

# COMMAND ----------

##################################################################################################################################
##################################################  ARCHIEVE #####################################################################
##################################################################################################################################







# ispot = final_model_df.filter(f.year(f.col('Date')).isin([2022, 2023])).groupBy('Date').agg(f.sum(f.col('Imps')).alias('iSpot'))

# ## Joe Harr ##
# model_df = spark.sql("select * from temp.ja_blend_mmm_DRTV").fillna(0)
# joe_harr = model_df.withColumn( 'Linear_Imp_jh', f.col('DRTV_Linear_Monitored_Impressions')).withColumn( 'Connected_Imp_jh', f.col('DRTV_Roku_CTV_Impressions')+f.col('DRTV_Samsung_CTV_Impressions')+f.col('DRTV_Publisher_Direct_CTV_Impressions')+f.col('DRTV_Yahoo_CTV_Impressions')+f.col('DRTV_Roku_CTV_Impressions_Direct_Mail_Campaign')).select('Mail_Date', 'Linear_Imp_jh', 'Connected_Imp_jh')


# ## Join both ##
# ispot.join(joe_harr, ispot.Date == joe_harr.Mail_Date, 'left').select('Date', 'iSpot',  'Linear_Imp_jh', 'Connected_Imp_jh').display()


# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

dma_mapping = {
    "Wilmington, North Carolina": 550,
    "Harlingen-Weslaco-McAllen, Texas": 636,
    "Richmond-Petersburg, Virginia": 556,
    "Sacramento-Stockton-Modesto, California": 862,
    "Peoria-Bloomington, Illinois": 675,
    "Lincoln & Hastings-Kearney, Nebraska": 722,
    "New York, New York": 501,
    "Sherman, Texas-Ada, Oklahoma": 657,
    "Baltimore, Maryland": 512,
    "El Paso, Texas": 765,
    "San Diego, California": 825,
    "Macon, Georgia": 503,
    "Milwaukee, Wisconsin": 617,
    "Ottumwa, Iowa-Kirksville, Missouri": 631,
    "Florence-Myrtle Beach, South Carolina": 570,
    "Billings, Montana": 756,
    "Albany, Georgia": 525,
    "Jackson, Tennessee": 718,
    "Lake Charles, Louisiana": 643,
    "Tucson (Sierra Vista), Arizona": 789,
    "Shreveport, Louisiana": 612,
    "Bangor, Maine": 537,
    "West Palm Beach-Ft. Pierce, Florida": 548,
    "Tallahassee, Florida-Thomasville, Georgia": 530,
    "Rockford, Illinois": 610,
    "Albany-Schenectady-Troy, New York": 532,
    "Knoxville, Tennessee": 557,
    "Charleston, South Carolina": 519,
    "Santa Barbara-San Luis Obispo, California": 855,
    "St. Louis, Missouri": 609,
    "Terre Haute, Indiana": 581,
    "Glendive, Montana": 798,
    "Charlottesville, Virginia": 584,
    "Spokane, Washington": 881,
    "Springfield-Holyoke, Massachusetts": 543,
    "Wheeling, West Virginia-Steubenville, Ohio": 554,
    "Miami-Ft. Lauderdale, Florida": 528,
    "Topeka, Kansas": 605,
    "Twin Falls, Idaho": 760,
    "Jackson, Mississippi": 639,
    "Las Vegas, Nevada": 839,
    "Dothan, Alabama": 606,
    # "JP_KANTO,Japan": None,
    "Savannah, Georgia": 507,
    "Baton Rouge, Louisiana": 716,
    "St. Joseph, Missouri": 638,
    "Greenwood-Greenville, Mississippi": 647,
    "Beaumont-Port Arthur, Texas": 692,
    "Rochester-Austin, Minnesota-Mason City, Iowa": 611,
    "Casper-Riverton, Wyoming": 767,
    "Buffalo, New York": 514,
    "Greenville-Spartanburg, South Carolina": 545,
    "Washington, DC (Hagerstown, Maryland)": 511,
    "Eureka, California": 802,
    "Greenville-New Bern-Washington, North Carolina": 567,
    "Zanesville, Ohio": 596,
    "Corpus Christi, Texas": 600,
    "Springfield, Missouri": 619,
    "Syracuse, New York": 555,
    "Helena, Montana": 766,
    "Salt Lake City, Utah": 770,
    "Tampa-St Petersburg (Sarasota), Florida": 539,
    "Butte-Bozeman, Montana": 754,
    "Los Angeles, California": 803,
    "Medford-Klamath Falls, Oregon": 813,
    "Flint-Saginaw-Bay City, Michigan": 513,
    "Lafayette, Louisiana": 582,
    "Utica, New York": 526,
    "Atlanta, Georgia": 524,
    "Norfolk-Portsmouth-Newport News,Virginia": 544,
    "Indianapolis, Indiana": 527,
    "Tyler-Longview(Nacogdoches), Texas": 709,
    "Dayton, Ohio": 542,
    "Evansville, Indiana": 649,
    "Unknown": 1000,
    "Boston, Massachusetts-Manchester, New Hampshire": 506,
    "Sioux City, Iowa": 624,
    "Charleston-Huntington, West Virginia": 564,
    "Lafayette, Indiana": 642,
    "Panama City, Florida": 656,
    "Reno, Nevada": 811,
    "Bluefield-Beckley-Oak Hill, West Virginia": 559,
    "Grand Junction-Montrose, Colorado": 773,
    "Mankato, Minnesota": 737,
    "Binghamton, New York": 502,
    "Idaho Falls-Pocatello, Idaho": 758,
    "Austin, Texas": 635,
    "Juneau, Alaska": 747,
    "Phoenix, Arizona": 753,
    "Fairbanks, Alaska": 745,
    "Detroit, Michigan": 505,
    "Madison, Wisconsin": 669,
    "North Platte, Nebraska": 740,
    "Chattanooga, Tennessee": 575,
    "Sioux Falls(Mitchell), South Dakota": 725,
    "Hattiesburg-Laurel, Mississippi": 710,
    "Lima, Ohio": 558,
    "Minot-Bismarck-Dickinson, North Dakota": 687,
    "Monroe, Louisiana-El Dorado, Arkansas": 628,
    "Birmingham, Alabama": 630,
    "Joplin, Missouri-Pittsburg, Kansas": 603,
    "Montgomery (Selma), Alabama": 698,
    "Orlando-Daytona Beach, Florida": 534,
    "Houston, Texas": 618,
    "Little Rock-Pine Bluff, Arkansas": 693,
    "Columbus, Georgia": 522,
    "Johnstown-Altoona, Pennsylvania": 574,
    "Palm Springs, California": 804,
    "Greensboro-Winston Salem, North Carolina": 518,
    "Raleigh-Durham (Fayetteville), North Carolina": 560,
    "Honolulu, Hawaii": 744,
    "Mobile, Alabama-Pensacola, Florida": 686,
    "Erie, Pennsylvania": 516,
    "San Angelo, Texas": 661,
    "Elmira, New York": 565,
    "Portland-Auburn, Maine": 500,
    "Harrisonburg, Virginia": 569,
    "San Antonio, Texas": 641,
    "Denver, Colorado": 751,
    "Augusta, Georgia": 520,
    "Toledo, Ohio": 538,
    "Alpena, Michigan": 583,
    "San Francisco-Oakland-San Jose, California": 807,
    "Yakima-Pasco-Richland-Kennewick, Washington": 810,
    "Yuma, Arizona-El Centro, California": 771,
    "Columbia, South Carolina": 546,
    "Lansing, Michigan": 551,
    "Waco-Temple-Bryan, Texas": 625,
    "Cincinnati, Ohio": 515,
    "Columbia-Jefferson City, Missouri": 604,
    "Providence, Rhode Island-New Bedford, Massachusetts": 521,
    "Wausau-Rhinelander, Wisconsin": 705,
    "Alexandria, Louisiana": 644,
    "Minneapolis-St. Paul, Minnesota": 613,
    "Pittsburgh, Pennsylvania": 508,
    "Quincy, Illinois-Hannibal, Missouri-Keokuk, Iowa": 717,
    "Rochester, New York": 611,
    "Eugene, Oregon": 801,
    "Omaha, Nebraska": 652,
    "Lexington, Kentucky": 541,
    "Gainesville, Florida": 592,
    "Albuquerque-Santa Fe, New Mexico": 790,
    "Burlington, Vermont-Plattsburgh, New York": 523,
    "Ft Smith-Springdale, Arkansas": 670,
    "New Orleans, Louisiana": 622,
    "South Bend-Elkhart, Indiana": 588,
    "Wilkes Barre-Scranton, Pennsylvania": 577,
    "Fresno-Visalia, California": 866,
    "Cedar Rapids-Waterloo-Iowa City, Iowa": 637,
    "Grand Rapids-Kalamazoo, Michigan": 563,
    "La Crosse-Eau Claire, Wisconsin": 702,
    "Wichita Falls, Texas & Lawton, Oklahoma": 627,
    "Great Falls, Montana": 755,
    "Salisbury, Maryland": 576,
    "Harrisburg-Lancaster-York, Pennsylvania": 566,
    "Oklahoma City, Oklahoma": 650,
    "Monterey-Salinas, California": 828,
    "Anchorage, Alaska": 743,
    "Clarksburg-Weston, West Virginia": 598,
    "Bowling Green, Kentucky": 736,
    "Lubbock, Texas": 651,
    "Columbus-Tupelo-West Point, Mississippi": 673,
    "Tri-Cities, Tennessee-Virginia": 531,
    # "JP_OTHER,Japan": None,
    "Louisville, Kentucky": 529,
    "Biloxi-Gulfport, Mississippi": 746,
    "Boise, Idaho": 757,
    "Nashville, Tennessee": 659,
    "Hartford & New Haven, Connecticut": 533,
    "Bakersfield, California": 800,
    "Victoria, Texas": 626,
    "Chicago, Illinois": 602,
    "Kansas City, Missouri": 616,
    "Amarillo, Texas": 634,
    "Columbus, Ohio": 535,
    "Chico-Redding, California": 868,
    "Dallas-Ft. Worth, Texas": 623,
    "Davenport,Iowa-Rock Island-Moline,Illinois": 682,
    "Green Bay-Appleton, Wisconsin": 658,
    "Huntsville-Decatur (Florence), Alabama": 691,
    "Roanoke-Lynchburg, Virginia": 573,
    "Ft. Wayne, Indiana": 509,
    "Laredo, Texas": 749,
    "Presque Isle, Maine": 552,
    "Cleveland-Akron (Canton), Ohio": 510,
    "Marquette, Michigan": 553,
    "Seattle-Tacoma, Washington": 819,
    "Duluth, Minnesota-Superior, Wisconsin": 676,
    "Jacksonville, Florida": 561,
    "Paducah, Kentucky-Harrisburg, Illinois": 632,
    "Parkersburg, West Virginia": 597,
    "Tulsa, Oklahoma": 671,
    "Watertown, New York": 549,
    "Bend, Oregon": 821,
    "Traverse City-Cadillac, Michigan": 540,
    "Cheyenne, Wyoming-Scottsbluff, Nebraska": 759,
    "Fargo-Valley City, North Dakota": 724,
    "Abilene-Sweetwater, Texas": 662,
    "Ft. Myers-Naples, Florida": 571,
    "Portland, Oregon": 820,
    "Missoula, Montana": 762,
    "Wichita-Hutchinson, Kansas": 678,
    "Colorado Springs-Pueblo, Colorado": 752,
    "Charlotte, North Carolina": 517,
    "Des Moines-Ames, Iowa": 679,
    "Jonesboro, Arkansas": 734,
    "Philadelphia, Pennsylvania": 504,
    "Rapid City, South Dakota": 764,
    "Memphis, Tennessee": 640,
    "Odessa-Midland, Texas": 633,
    "Youngstown, Ohio": 536,
    "Champaign & Springfield-Decatur,Illinois": 648,
    "Meridian, Mississippi": 711, 
    "National": 1000
}

# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  
dma_col = "DmaName"
cols = ["Date", "DMA_Code", "SubChannel", "Campaign"]

final_model_df = final_model_df.withColumn("DMA_Code", map_dma_name_to_code(dma_col))
final_model_df = final_model_df.groupBy(cols).agg(f.sum(f.col('spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'))
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

## Expanidng Data to DMA Level and Updating Spend and Imps ##

drtv_multipler = read_table("temp.ja_blend_mmm2_DRTV_multiplier") ## Reading Multiplier Table
final_model_df = final_model_df.drop('DMA_Code').crossJoin(drtv_multipler) ## Cross Join to update table to DMA Level

condition = when(f.year(f.col('Date'))==2022, \
                 when(f.month(f.col('Date')).between(1,3), f.col('22_q1')).\
                 when(f.month(f.col('Date')).between(4,6), f.col('22_q2')).\
                 when(f.month(f.col('Date')).between(7,9), f.col('22_q3')).otherwise(f.col('22_q4'))      
                 ).\
            when(f.year(f.col('Date'))==2023,\
                 when(f.month(f.col('Date')).between(1,3), f.col('23_q1')).\
                 when(f.month(f.col('Date')).between(4,6), f.col('23_q2')).\
                 when(f.month(f.col('Date')).between(7,9), f.col('23_q3')).otherwise(f.col('23_q4'))\
                ).otherwise(1000000) ## Condition for Multiplier
            
final_model_df = final_model_df.withColumn("Multiplier", condition)
final_model_df = final_model_df.withColumn("Spend",f.col("Spend")*f.col("Multiplier"))
final_model_df = final_model_df.withColumn("Imps",f.col("Imps")*f.col("Multiplier"))
final_model_df.display()

# COMMAND ----------

## Rename columns and Adding dummy cols to streamline same data format ##
### Adding dummy Vars ###
final_model_df = final_model_df.withColumn("Clicks",f.lit(0))

### Re-ordering cols ###
cols = ['Date', 'DMA_Code', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.select(cols).fillna(0, subset=['Spend', 'Imps', 'Clicks'])
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # QC Block

# COMMAND ----------

# QC_df = unit_test( df, final_model_df, ['Spend','Spend'], ['Date','Date'])
# QC_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Some More Updates

# COMMAND ----------

## Adding Channel Name ##
cols = ['Date', 'DMA_Code', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.withColumn('Channel', f.lit('TV')).select(cols) ## Re-arranging Columns 
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_DRTV_Linear" 
save_df_func(final_model_df, table_name)

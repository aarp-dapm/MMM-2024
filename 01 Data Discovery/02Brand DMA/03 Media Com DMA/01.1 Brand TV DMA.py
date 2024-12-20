# Databricks notebook source
# MAGIC %md
# MAGIC # Utility Files and Function 

# COMMAND ----------

# MAGIC %run "./51 Local Utility Files"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

## Reading CSV File List ##
path_list = ['dbfs:/blend360/sandbox/mmm/brand_offline/tv/2022.csv', 'dbfs:/blend360/sandbox/mmm/brand_offline/tv/2023.csv']

## Appending Data for 2022 and 2023 ##
df_brand_tv_22 = read_csv(path_list[0],0)
df_brand_tv_23 = read_csv(path_list[1],0)
df = df_brand_tv_22.union(df_brand_tv_23)

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
df = df.withColumn('SubChannel', f.lit('BrandTV'))



## Selecting Columns ##
cols = ['Date', 'DmaName', 'Campaign', 'SubChannel', 'Show Type', 'Daypart', 'Media Type', 'network', 'Pod', 'Pod Order', 'Duration', 'Spend', 'Imps'] # based on netwrok we can maybe figure out if it is connected tv or linear tv
df.select(cols).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating More Flags

# COMMAND ----------

## Creating More flags to match spend data ##
df = df.withColumn("Buys", when(f.col('Media Type')=='National', 'National TV').otherwise('Home Team Sports'))
df = df.withColumn("DaypartNew",  when( f.col('Buys') == 'National TV', when(f.col('network').isin(['ABC', 'CBS', 'NBC', 'FOX']),\
                                                                          when(f.col('Daypart').isin(['Late Fringe PM', 'Over Night', 'Prime Time']), f.lit('Prime Time'))\
                                                                          .otherwise(f.lit('Early Morning')))\
                                                                     .otherwise(f.lit('Cable')) )\
                              .otherwise(f.lit("Sports"))   )

## Reducing Dayparts t


## Creating Year and Quarter flag ##
df = df.withColumn('Year', f.year(f.col('Date')))
df = df.withColumn('Quarter', f.concat(f.lit('Q'),f.quarter(f.col('Date'))))


'''
Get Media Com confirmation on Channel Grouping 
'''

# COMMAND ----------

# MAGIC %md
# MAGIC #### Updating Spend Columnm 

# COMMAND ----------

## Reading Spend Data ##
spend_tv = read_excel("dbfs:/blend360/sandbox/mmm/brand_offline/tv/MidasTv_2022_2023.xlsx")
spend_tv = spend_tv.groupBy('Year', 'Quarter').agg(f.sum(f.col('Net Spend')).alias('Spend'))

## Creating Spend Factor ##
SpendFactorDf = df.groupBy('Year', 'Quarter').agg(f.count(f.col('Buys')).alias('SpendFactor'))

## Joining Data ##
SpendFactorDf = spend_tv.join(SpendFactorDf, on=['Year', 'Quarter'], how='left')
SpendFactorDf = SpendFactorDf.withColumn('Spend', f.col('Spend')/f.col('SpendFactor'))

## Joining with main Dataframe ##
df = df.drop('Spend').join(SpendFactorDf, on=['Year', 'Quarter'], how='left')
# df.display()

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
df = df.withColumn('NetworkGroup', when(f.col('network').isin(other_brand_ch_group),"Other").otherwise(f.col('network'))) ## Use 90-10 rule to group channels based on spend
df = df.withColumn('NetworkGroup', f.regexp_replace( 'NetworkGroup'," ", ""))

## Daypart Grouping ##
condition = when(f.col('Daypart').isin(['Early Morning', 'Day Time']), 'Morning').\
            when(f.col('Daypart').isin(['Early Fringe', 'Weekend Afternoon', 'Weekend Day']), 'Afternoon').\
            when(f.col('Daypart').isin(['Prime Time']), 'Prime Time').\
            when(f.col('Daypart').isin(['Late Fringe AM', 'Late Fringe PM', 'Over Night']), 'NightTime').otherwise("NULL")
df = df.withColumn('DaypartGroup', condition)


## Creating One Single Columns ##
# df = df.withColumn("Campaign", f.concat_ws('_', f.col('Show Type'), f.col('DaypartGroup'))) ## Skipping Network Group out as well
df = df.withColumn("Campaign", f.col('Duration'))
# df.display()

# COMMAND ----------

### Aggregating metrics ###
cols = ["Date", "DmaName", "SubChannel", "Campaign"]

spend_col = 'Spend'
imps_col = 'Imps'
# click_col = 'Clicks' ## No Clix Col

final_model_df = df.groupBy(cols).agg(f.sum(f.col(spend_col)).alias('spend'), f.sum(f.col(imps_col)).alias('Imps'))
final_model_df.display()

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
# final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

#######################################################
################# ARCHIVED CODE #######################
#######################################################

# ch_dma_code = list(set([str(value) for key, value in dma_mapping.items()]))
# ch_dma_code.remove('1000')

# from functools import reduce
# @udf
# def get_pop_prop(dma_code, year, month):

#   ## FY 2022 ##
#   if year == 2022:
#     if month in [1,2,3]:
#       return QoQ_map['22_q1'].get(dma_code) # Q1
#     elif month in [4,5,6]: 
#       return QoQ_map['22_q2'].get(dma_code) # Q2
#     elif month in [7,8,9]:
#       return QoQ_map['22_q3'].get(dma_code) # Q3
#     elif month in [10,11,12]:
#       return QoQ_map['22_q4'].get(dma_code) # Q4
    
#   ## FY 2023 ##
#   if year == 2023:
#     if month in [1,2,3]:
#       return QoQ_map['23_q1'].get(dma_code) # Q1
#     elif month in [4,5,6]: 
#       return QoQ_map['23_q2'].get(dma_code) # Q2
#     elif month in [7,8,9]:
#       return QoQ_map['23_q3'].get(dma_code) # Q3
#     elif month in [10,11,12]:
#       return QoQ_map['23_q4'].get(dma_code) # Q4



# def new_df_func(col1, col2, col3, col4, col5, col6):

#   original_row = Row(Date=col1, DMA_Code=col2, SubChannel=col3, Campaign=col4, Spend=col5, Imps=col6)
#   new_df = spark.sparkContext.parallelize([original_row]*210).toDF()

#   ## National to DMA ##
#   labels_udf = f.udf(lambda indx: dma_code[indx-1], StringType()) 
#   new_df = new_df.withColumn('row_num', f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
#   new_df = new_df.withColumn('DMA_Code', labels_udf('row_num'))

#   ## Updating Spend ##
#   new_df = new_df.withColumn('Spend', f.col('Spend')*get_pop_prop('DMA_Code', f.year(f.col('Date')), f.month(f.col('Date'))))
  
#   ## Updating Impression ##
#   new_df = new_df.withColumn('Imps', f.col('Imps')*get_pop_prop('DMA_Code', f.year(f.col('Date')), f.month(f.col('Date'))))


#   return new_df

# ###################################################################################

# df_national = final_model_df.filter(f.col('DMA_Code')==1000)
# df_null = None  

# df_remain = final_model_df.filter(f.col('DMA_Code')!=1000)

# ## Creating empty dataframe ##
# schema = final_model_df.schema
# df1 = spark.createDataFrame([],schema)

# cols = ['Date', 'DMA_Code', 'SubChannel', 'Campaign', 'Spend', 'Imps']
# df_list = []

# for row in df_national.collect():

#   temp_df = new_df_func(row['Date'], row['DMA_Code'], row['SubChannel'], row['Campaign'], row['Spend'], row['Imps'])
#   df_list.append(temp_df)
 
# print(f"Stacking {len(df_list)} Dataframes")
# df1 = reduce(DataFrame.unionAll, df_list)

# COMMAND ----------

brand_tv_multipler = read_table("temp.ja_blend_mmm2_BrandOfflineTV_multiplier") 
# brand_tv_multipler.display()

# COMMAND ----------

## Expanidng Data to DMA Level and Updating Spend and Imps ##

brand_tv_multipler = read_table("temp.ja_blend_mmm2_BrandOfflineTV_multiplier") ## Reading Multiplier Table
final_model_df = final_model_df.drop('DMA_Code').crossJoin(brand_tv_multipler) ## Cross Join to update table to DMA Level

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
# final_model_df.display()

# COMMAND ----------

## Rename columns and Adding dummy cols to streamline same data format ##
### Adding dummy Vars ###
final_model_df = final_model_df.withColumn("Clicks",f.lit(0))

### Re-ordering cols ###
cols = ['Date', 'DMA_Code', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.select(cols).fillna(0, subset=['Spend', 'Imps', 'Clicks'])
# final_model_df.display()

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
# final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_BrandOfflineTV" 
save_df_func(final_model_df, table_name)

# COMMAND ----------



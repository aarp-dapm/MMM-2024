# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

df= spark.sql('select * from default.icm_datadigital_dv360_csv')

## Change Date Type ##
df = df.withColumn('Date', f.date_format(f.to_date(f.col('Week'),'M/d/y'),'yyyy-MM-dd'))

## Update Date to first day of week as Monday ##
# day_of_week = df.select(f.dayofweek(f.col('Date'))).dropDuplicates().collect()[0][0] ## Day of week was Sunday 

df = df.withColumn('Date',f.date_add(f.col('Date'),1))
day_of_week = df.select(f.dayofweek(f.col('Date'))).dropDuplicates().collect()[0][0]
assert day_of_week == 2
print("Day of week updated to Monday")

## Imputing NULL channels value with keywords present in campaign name ##
condition = when( f.col('Channel').isNull(),
            when( f.col('CampaignName').like('%Display%'), 'Display').\
            when( f.col('CampaignName').like('%Native%'), 'Native').\
            when( f.col('CampaignName').like('%YouTube%'), 'YouTube').\
            otherwise('Other')).otherwise(f.col('Channel'))

df = df.withColumn( "Channel", condition)




### Standardizing Names ###
df = df.withColumnRenamed('DMA_NAME','DmaName')\
               .withColumnRenamed('CampaignName','Campaign')\
               .withColumnRenamed('Channel','SubChannel')

## Changing 'Native Video' to Native ##
df = df.withColumn('Subchannel', when(f.col('Subchannel').isin(['Native Video']), 'Native').otherwise(f.col('Subchannel')) )

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Campaign Grouping 

# COMMAND ----------

condition = when( f.lower(f.col('Campaign')).like('%site traffic%'), 'SiteTraffic' ).\
            when( f.lower(f.col('Campaign')).like('%conversions%'), 'Conversions' ).\
            otherwise('Other')

df = df.withColumn('Campaign',condition)
df.display()

# COMMAND ----------

### Aggregating metrics ###
cols = ["Date", "DmaName", "SubChannel", "Campaign"]

spend_col = 'Spend'
imps_col = 'Impressions'
click_col = 'Clicks'

final_model_df = df.groupBy(cols).agg(f.sum(f.col(spend_col)).alias('spend'), f.sum(f.col(imps_col)).alias('Imps'), f.sum(f.col(click_col)).alias('clicks'))
final_model_df.display()

# COMMAND ----------

## Filter for wrong DMA Names ##
not_dma_names = ["JP_KANTO,Japan", "JP_OTHER,Japan"]
final_model_df = final_model_df.filter(~f.col('DmaName').isin(not_dma_names))
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
    "Meridian, Mississippi": 711
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
condition = when(f.col('SubChannel').isin(['Native', 'YouTube']), 'Video').when(f.col('SubChannel').isin(['Display', 'Other']), 'Display').otherwise('Other')
final_model_df = final_model_df.withColumn('Channel', condition)

## Re-arranging Columns ##
cols = ['Date', 'DMA_Code', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.select(cols) 
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_IcmDigital_Dv360" 
save_df_func(final_model_df, table_name)

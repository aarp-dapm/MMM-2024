# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

df = spark.sql('select * from default.icm_datadigital_googlesearch_csv')

## Change Date Type ##
df = df.withColumn('Date', f.date_format(f.to_date(f.col('Week'),'M/d/y'), 'yyyy-MM-dd'))

## Update Date to first day of week as Monday ##
day_of_week = df.select(f.dayofweek(f.col('Date'))).dropDuplicates().collect()[0][0]
assert day_of_week == 2 

### Standardizing Cols Name ###
df = df.withColumnRenamed('DMA_Region__Matched_','DmaName')\
                    .withColumnRenamed('Campaign_Name','Campaign')\
                    .withColumnRenamed('Cost','Spend')\
                    .withColumnRenamed('Impr.', 'Imps')


## Creating New Column  ##
df = df.withColumn('SubChannel', f.lit('GoogleSearch'))

## No Campaign Grouping ##
df = df.withColumn('Campaign', f.col('SubChannel'))

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
    "Odessa-Midland TX": 633,
    "North Platte NE": 740,
    "Flint-Saginaw-Bay City MI": 513,
    "Hattiesburg-Laurel MS": 710,
    "Toledo OH": 538,
    "Austin TX": 635,
    "Portland-Auburn ME": 500,
    "Rochester-Mason City-Austin IA": 611,
    "Billings MT": 756,
    "Buffalo NY": 514,
    "Chicago IL": 602,
    "Washington DC (Hagerstown MD)": 511,
    "Raleigh-Durham (Fayetteville) NC": 560,
    "Portland OR": 820,
    "Springfield MO": 619,
    "Wichita-Hutchinson KS": 678,
    "Columbus-Tupelo-West Point MS": 673,
    "Harrisonburg VA": 569,
    "Zanesville OH": 596,
    "Topeka KS": 605,
    "Jackson MS": 639,
    "Bowling Green KY": 736,
    "Lexington KY": 541,
    "Boston MA-Manchester NH": 506,
    "La Crosse-Eau Claire WI": 702,
    "Baton Rouge LA": 716,
    "Lafayette LA": 582,
    "Montgomery-Selma AL": 698,
    "Albany GA": 525,
    "Kansas City MO": 616,
    "Paducah KY-Cape Girardeau MO-Harrisburg-Mount Vernon IL": 632,
    "Quincy IL-Hannibal MO-Keokuk IA": 717,
    "Louisville KY": 529,
    "Santa Barbara-Santa Maria-San Luis Obispo CA": 855,
    "Victoria TX": 626,
    "Sherman-Ada OK": 657,
    "Charleston SC": 519,
    "Cheyenne WY-Scottsbluff NE": 759,
    "Eugene OR": 801,
    "Macon GA": 503,
    "Anchorage AK": 743,
    "Providence-New Bedford MA": 521,
    "Dayton OH": 542,
    "Waco-Temple-Bryan TX": 625,
    "Youngstown OH": 536,
    "Lake Charles LA": 643,
    "Madison WI": 669,
    "West Palm Beach-Ft. Pierce FL": 548,
    "Fresno-Visalia CA": 866,
    "Medford-Klamath Falls OR": 813,
    "Tulsa OK": 671,
    "Roanoke-Lynchburg VA": 573,
    "St. Louis MO": 609,
    "Twin Falls ID": 760,
    "Johnstown-Altoona-State College PA": 574,
    "Alexandria LA": 644,
    "Erie PA": 516,
    "Sioux City IA": 624,
    "Traverse City-Cadillac MI": 540,
    "Lima OH": 558,
    "Fairbanks AK": 745,
    "Tallahassee FL-Thomasville GA": 530,
    "Miami-Ft. Lauderdale FL": 528,
    "Richmond-Petersburg VA": 556,
    "New Orleans LA": 622,
    "Savannah GA": 507,
    "Panama City FL": 656,
    "Rochester NY": 612,
    "Grand Rapids-Kalamazoo-Battle Creek MI": 563,
    "Greensboro-High Point-Winston Salem NC": 518,
    "Huntsville-Decatur (Florence) AL": 691,
    "Tampa-St. Petersburg (Sarasota) FL": 539,
    "Salisbury MD": 576,
    "Laredo TX": 749,
    "Spokane WA": 881,
    "Albuquerque-Santa Fe NM": 790,
    "Idaho Falls-Pocatello ID": 758,
    "Meridian MS": 711,
    "Charlottesville VA": 584,
    "Nashville TN": 659,
    "Wilkes Barre-Scranton PA": 577,
    "Los Angeles CA": 803,
    "Bakersfield CA": 800,
    "Little Rock-Pine Bluff AR": 693,
    "Marquette MI": 553,
    "Norfolk-Portsmouth-Newport News VA": 544,
    "Albany-Schenectady-Troy NY": 532,
    "Indianapolis IN": 527,
    "Palm Springs CA": 804,
    "Lansing MI": 551,
    "South Bend-Elkhart IN": 588,
    "Rockford IL": 610,
    "San Diego CA": 825,
    "Missoula MT": 762,
    "Columbia SC": 546,
    "Springfield-Holyoke MA": 543,
    "Watertown NY": 549,
    "Philadelphia PA": 504,
    "Greenville-Spartanburg-Asheville-Anderson": 567,
    "Monterey-Salinas CA": 828,
    "Pittsburgh PA": 508,
    "Joplin MO-Pittsburg KS": 603,
    "Charlotte NC": 517,
    "Corpus Christi TX": 600,
    "Ft. Wayne IN": 509,
    "Birmingham (Ann and Tusc) AL": 630,
    "Wausau-Rhinelander WI": 705,
    "Chattanooga TN": 575,
    "Ft. Smith-Fayetteville-Springdale-Rogers AR": 670,
    "Monroe LA-El Dorado AR": 628,
    "Atlanta GA": 524,
    "Gainesville FL": 592,
    "Memphis TN": 640,
    "Reno NV": 811,
    "Ottumwa IA-Kirksville MO": 631,
    "Terre Haute IN": 581,
    "Glendive MT": 798,
    "Phoenix AZ": 753,
    "Lubbock TX": 651,
    "Cleveland-Akron (Canton) OH": 510,
    "Evansville IN": 649,
    "Sacramento-Stockton-Modesto CA": 862,
    "Eureka CA": 802,
    "Chico-Redding CA": 868,
    "Clarksburg-Weston WV": 598,
    "Tyler-Longview(Lufkin & Nacogdoches) TX": 709,
    "Biloxi-Gulfport MS": 746,
    "Yakima-Pasco-Richland-Kennewick WA": 810,
    "Minneapolis-St. Paul MN": 613,
    "Elmira (Corning) NY": 565,
    "Denver CO": 751,
    "Honolulu HI": 744,
    "San Angelo TX": 661,
    "Ft. Myers-Naples FL": 571,
    "Parkersburg WV": 597,
    "Wheeling WV-Steubenville OH": 554,
    "Lafayette IN": 642,
    "Salt Lake City UT": 770,
    "Burlington VT-Plattsburgh NY": 523,
    "Des Moines-Ames IA": 679,
    "Green Bay-Appleton WI": 658,
    "Peoria-Bloomington IL": 675,
    "Amarillo TX": 634,
    "Fargo-Valley City ND": 724,
    "Harlingen-Weslaco-Brownsville-McAllen TX": 636,
    "St. Joseph MO": 638,
    "Bluefield-Beckley-Oak Hill WV": 559,
    "Dothan AL": 606,
    "Knoxville TN": 557,
    "Augusta GA": 520,
    "Las Vegas NV": 839,
    "Shreveport LA": 612,
    "Yuma AZ-El Centro CA": 771,
    "Lincoln & Hastings-Kearney NE": 722,
    "Baltimore MD": 512,
    "Mankato MN": 737,
    "Florence-Myrtle Beach SC": 570,
    "Hartford & New Haven CT": 533,
    "Harrisburg-Lancaster-Lebanon-York PA": 566,
    "Greenwood-Greenville MS": 647,
    "Boise ID": 757,
    "Oklahoma City OK": 650,
    "Dallas-Ft. Worth TX": 623,
    "Seattle-Tacoma WA": 819,
    "Minot-Bismarck-Dickinson(Williston) ND": 687,
    "Rapid City SD": 764,
    "Utica NY": 526,
    "Sioux Falls(Mitchell) SD": 725,
    "Colorado Springs-Pueblo CO": 752,
    "Grand Junction-Montrose CO": 773,
    "Detroit MI": 505,
    "Syracuse NY": 555,
    "Wilmington NC": 550,
    "Orlando-Daytona Beach-Melbourne FL": 534,
    "Charleston-Huntington WV": 564,
    "Champaign & Springfield-Decatur IL": 648,
    "Duluth MN-Superior WI": 676,
    "Columbus OH": 535,
    "Alpena MI": 583,
    "El Paso TX": 765,
    "Tucson (Sierra Vista) AZ": 789,
    "Abilene-Sweetwater TX": 662,
    "Jacksonville FL": 561,
    "Cedar Rapids-Waterloo-Iowa City & Dubuque IA": 637,
    "Greenville-New Bern-Washington NC": 567,
    "San Antonio TX": 641,
    "Beaumont-Port Arthur TX": 692,
    "Great Falls MT": 755,
    "Omaha NE": 652,
    "Columbus GA": 522,
    "Mobile AL-Pensacola (Ft. Walton Beach) FL": 686,
    "Tri-Cities TN-VA": 531,
    "Bangor ME": 537,
    "Butte-Bozeman MT": 754,
    "Cincinnati OH": 515,
    "Helena MT": 766,
    "Binghamton NY": 502,
    "Houston TX": 618,
    "Jonesboro AR": 734,
    "Columbia-Jefferson City MO": 604,
    "San Francisco-Oakland-San Jose CA": 807,
    "Juneau AK": 747,
    "Wichita Falls TX & Lawton OK": 627,
    "Presque Isle ME": 552,
    "Casper-Riverton WY": 767,
    "Bend OR": 821,
    "Jackson TN": 718,
    "Davenport IA-Rock Island-Moline IL": 682,
    "Milwaukee WI": 617,
    "Rochester-Mason City-Austin,IA": 611,
    "Montgomery-Selma, AL": 698,
    "Providence-New Bedford,MA": 521,
    "Sherman-Ada, OK": 657,
    "Greenville-Spartanburg-Asheville-Anderson": 545,
    "New York, NY": 501,
    "Billings, MT": 756
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

'''
No Data at National level to be distributed into DMA level
'''

# COMMAND ----------

# MAGIC %md
# MAGIC # Some More Updates

# COMMAND ----------

## Adding Channel Name ##
cols = ['Date', 'DMA_Code', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.withColumn('Channel', f.lit('Search')).select(cols) ## Re-arranging Columns 
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Save File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_IcmDigital_GoogleSearch" 
save_df_func(final_model_df, table_name)

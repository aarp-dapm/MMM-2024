# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

## Reading Files ##
file_path = "dbfs:/blend360/sandbox/mmm/icm_digital/bing"
file_list = dbutils.fs.ls(file_path)

df_list = []
for file in file_list:
  df_list.append(read_csv(file.path))

df = reduce(DataFrame.unionAll, df_list)

## Filter for Irrelevant rows ##
filter_keywords_list = ['Total', '©2024 Microsoft Corporation. All rights reserved. ']
df = df.filter(f.col('Week').isNotNull()).filter(~f.col('Week').isin(filter_keywords_list))

## Change Date Type ##
df = df.withColumn('Date', f.coalesce(f.date_format(f.to_date(f.col('Week'),'M/d/y'), 'yyyy-MM-dd'), f.to_date(f.col('Week'), 'yyyy-MM-dd')) )

## Update Date to first day of week as Monday ##
day_of_week = df.select(f.dayofweek(f.col('Date'))).dropDuplicates().collect()[0][0]
assert day_of_week == 2

## Adding Channel ##
df = df.withColumn("SubChannel", f.lit("BingSearch"))

## Renaming Column Name ##
df = df.withColumnRenamed('Account_name', 'Campaign')
df = df.withColumnRenamed('Metro area (physical location)', 'Metro_Area')
df = df.withColumnRenamed('Country/region (physical location)', 'Country')

## No Campaign Grouping ##
df = df.withColumn('Campaign', f.col('SubChannel'))

## Filters ##
df = df.filter(f.year(f.col('Date')).isin([2022, 2023]))
df = df.filter(f.col('Country').isin('United States'))

## NULL Imputation ##
df = df.fillna('National', subset=['Metro_Area'])

df.display()

# COMMAND ----------

### Aggregating metrics ###
cols = ["Date", "Metro_Area", "SubChannel", "Campaign"]

spend_col = 'Spend'
imps_col = 'Impressions'
click_col = 'Clicks'

final_model_df = df.groupBy(cols).agg(f.sum(f.col(spend_col)).alias('spend'), f.sum(f.col(imps_col)).alias('Imps'), f.sum(f.col(click_col)).alias('clicks'))
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

dma_mapping = {
    "Gainesville, FL": 592,
    "Jonesboro, AR": 734,
    "Ottumwa, IA-Kirksville, MO": 631,
    "Flint-Saginaw-Bay City, MI": 513,
    "Palm Springs, CA": 804,
    "Wilkes Barre-Scranton-Hazleton, PA": 577,
    "Paducah, KY-Cape Girardeau, MO-Harrisburg, IL": 632,
    "Wichita Falls, TX & Lawton, OK": 627,
    "Charleston-Huntington, WV": 564,
    "Eureka, CA": 802,
    "Ft. Smith-Fayetteville-Springdale-Rogers, AR": 670,
    "Corpus Christi, TX": 600,
    "Greenville-New Bern-Washington, NC": 567,
    "Minneapolis-St. Paul, MN": 613,
    "El Paso, TX (Las Cruces, NM)": 765,
    "Harrisonburg, VA": 569,
    "Harrisburg-Lancaster-Lebanon-York, PA": 566,
    "Sacramento-Stockton-Modesto, CA": 862,
    "Reno, NV": 811,
    "Greenwood-Greenville, MS": 647,
    "Columbus, OH": 535,
    "Ft. Wayne, IN": 509,
    "Honolulu, HI": 744,
    "Bakersfield, CA": 800,
    "Montgomery-Selma, AL": 698,
    "Lima, OH": 558,
    "Laredo, TX": 749,
    "Baton Rouge, LA": 716,
    "San Francisco-Oakland-San Jose, CA": 807,
    "Topeka, KS": 605,
    "Dayton, OH": 542,
    "Elmira (Corning), NY": 565,
    "Fresno-Visalia, CA": 866,
    "Wausau-Rhinelander, WI": 705,
    "Clarksburg-Weston, WV": 598,
    "Missoula, MT": 762,
    "Kansas City, MO": 616,
    "Louisville, KY": 529,
    "Biloxi-Gulfport, MS": 746,
    "Cincinnati, OH": 515,
    # "Skeena-Queen Charlotte": None,
    "Colorado Springs-Pueblo, CO": 752,
    "Las Vegas, NV": 839,
    "Charleston, SC": 519,
    "Columbus-Tupelo-West Point-Houston, MS": 673,
    "Atlanta, GA": 524,
    "Charlottesville, VA": 584,
    "Victoria": 626,
    "Presque Isle, ME": 552,
    "Juneau, AK": 747,
    "Lexington, KY": 541,
    "La Crosse-Eau Claire, WI": 702,
    "Phoenix (Prescott), AZ": 753,
    "Green Bay-Appleton, WI": 658,
    "Albany, GA": 525,
    "San Angelo, TX": 661,
    "Beaumont-Port Arthur, TX": 692,
    "Anchorage, AK": 743,
    "Tri-Cities, TN-VA": 531,
    "Columbia-Jefferson City, MO": 604,
    "Tulsa, OK": 671,
    "Monroe, LA-El Dorado, AR": 628,
    "Milwaukee, WI": 617,
    "Yuma, AZ-El Centro, CA": 771,
    "Greensboro-High Point-Winston Salem, NC": 518,
    "Medford-Klamath Falls, OR": 813,
    "Hartford & New Haven, CT": 533,
    "Houston, TX": 618,
    "Helena, MT": 766,
    "Huntsville-Decatur (Florence), AL": 691,
    "Traverse City-Cadillac, MI": 540,
    "Twin Falls, ID": 760,
    "Raleigh-Durham (Fayetteville), NC": 560,
    "Burlington, VT-Plattsburgh, NY": 523,
    "Jackson, MS": 639,
    "Alpena, MI": 583,
    "Cedar Rapids-Waterloo-Iowa City & Dubuque, IA": 637,
    "Harlingen-Weslaco-Brownsville-McAllen, TX": 636,
    "Miami-Ft. Lauderdale, FL": 528,
    "Waco-Temple-Bryan, TX": 625,
    "Great Falls, MT": 755,
    "Watertown, NY": 549,
    "Champaign & Springfield-Decatur, IL": 648,
    "Nashville, TN": 659,
    "Butte-Bozeman, MT": 754,
    "Minot-Bismarck-Dickinson(Williston), ND": 687,
    "Bluefield-Beckley-Oak Hill, WV": 559,
    "Los Angeles, CA": 803,
    "Washington, DC (Hagerstown, MD)": 511,
    "Lubbock, TX": 651,
    "Glendive, MT": 798,
    "Grand Junction-Montrose, CO": 773,
    "Marquette, MI": 553,
    "Greenville-Spartanburg, SC-Asheville, NC-Anderson, SC": 545,
    "Knoxville, TN": 557,
    "Bangor, ME": 537,
    "Spokane, WA": 881,
    "Syracuse, NY": 555,
    "Wheeling, WV-Steubenville, OH": 554,
    "Little Rock-Pine Bluff, AR": 693,
    "Rochester, MN-Mason City, IA-Austin, MN": 611,
    "Evansville, IN": 649,
    "Lincoln & Hastings-Kearney, NE": 722,
    "Joplin, MO-Pittsburg, KS": 603,
    "San Diego, CA": 825,
    "Chicago, IL": 602,
    "Dothan, AL": 606,
    "Providence, RI-New Bedford, MA": 521,
    "Detroit, MI": 505,
    "Grand Rapids-Kalamazoo-Battle Creek, MI": 563,
    "Toledo, OH": 538,
    "Seattle-Tacoma, WA": 819,
    "Birmingham (Anniston and Tuscaloosa), AL": 630,
    "Charlotte, NC": 517,
    "Roanoke-Lynchburg, VA": 573,
    "Cleveland-Akron (Canton), OH": 510,
    "Sherman, TX-Ada, OK": 657,
    "Peoria-Bloomington, IL": 675,
    "Wichita-Hutchinson, KS Plus": 678,
    "North Platte, NE": 740,
    "St. Louis, MO": 609,
    "Albuquerque-Santa Fe, NM": 790,
    "St. Joseph, MO": 638,
    "Amarillo, TX": 634,
    "Chico-Redding, CA": 868,
    "Rockford, IL": 610,
    "Bend, OR": 821,
    "Myrtle Beach-Florence, SC": 570,
    "Johnstown-Altoona-State College, PA": 574,
    "Tyler-Longview(Lufkin & Nacogdoches), TX": 709,
    "Abilene-Sweetwater, TX": 662,
    "Parkersburg, WV": 597,
    "Pittsburgh, PA": 508,
    "Norfolk-Portsmouth-Newport News, VA": 544,
    "Cheyenne, WY-Scottsbluff, NE": 759,
    "Memphis, TN": 640,
    "Bowling Green, KY": 736,
    "Baltimore, MD": 512,
    "New York, NY": 501,
    "Binghamton, NY": 502,
    "Santa Barbara-Santa Maria-San Luis Obispo, CA": 855,
    "Oklahoma City, OK": 650,
    "Monterey-Salinas, CA": 828,
    "Omaha, NE": 652,
    "Meridian, MS": 711,
    "South Bend-Elkhart, IN": 588,
    "Orlando-Daytona Beach-Melbourne, FL": 534,
    "Des Moines-Ames, IA": 679,
    "Lansing, MI": 551,
    "Albany-Schenectady-Troy, NY": 532,
    "Boston, MA (Manchester, NH)": 506,
    "Chattanooga, TN": 575,
    "Eugene, OR": 801,
    "Springfield-Holyoke, MA": 543,
    "Sioux Falls(Mitchell), SD": 725,
    "Salisbury, MD": 576,
    "Jacksonville, FL": 561,
    "Davenport, IA-Rock Island-Moline, IL": 682,
    "Fairbanks, AK": 745,
    "Mankato, MN": 737,
    "Savannah, GA": 503,
    "Buffalo, NY": 514,
    "Ft. Myers-Naples, FL": 571,
    "Tampa-St. Petersburg (Sarasota), FL": 539,
    "Mobile, AL-Pensacola (Ft. Walton Beach), FL": 686,
    "Lafayette, LA": 582,
    "Idaho Falls-Pocatello, ID (Jackson, WY)": 758,
    "Indianapolis, IN": 527,
    "Lake Charles, LA": 643,
    "Portland-Auburn, ME": 500,
    "Wilmington, NC": 550,
    "Utica, NY": 526,
    "Richmond-Petersburg, VA": 556,
    "Victoria, TX": 626,
    "Duluth, MN-Superior, WI": 676,
    "Madison, WI": 669,
    "Columbia, SC": 546,
    "Hattiesburg-Laurel, MS": 710,
    "Columbus, GA (Opelika, AL)": 630,
    "West Palm Beach-Ft. Pierce, FL": 548,
    "Tucson (Sierra Vista), AZ": 789,
    "Erie, PA": 516,
    "Macon, GA": 503,
    "Terre Haute, IN": 581,
    "Youngstown, OH": 536,
    "Rochester, NY": 555,
    "Dallas-Ft. Worth, TX": 623,
    "Jackson, TN": 718,
    "Shreveport, LA": 612,
    "Lafayette, IN": 642,
    "Portland, OR": 820,
    "Austin, TX": 635,
    "Billings, MT": 756,
    "Casper-Riverton, WY": 767,
    "San Antonio, TX": 641,
    "Alexandria, LA": 644,
    "Quincy, IL-Hannibal, MO-Keokuk, IA": 717,
    "Rapid City, SD": 764,
    "Tallahassee, FL-Thomasville, GA": 530,
    "New Orleans, LA": 622,
    "Zanesville, OH": 596,
    "Panama City, FL": 656,
    "Salt Lake City, UT": 770,
    "Sioux City, IA": 624,
    "Boise, ID": 757,
    "Denver, CO": 751,
    "Odessa-Midland, TX": 633,
    "Philadelphia, PA": 504,
    # "Kootenay Boundary": None,
    "Yakima-Pasco-Richland-Kennewick, WA": 810,
    "Augusta, GA-Aiken, SC": 520,
    "Springfield, MO": 619,
    "Helena, MT": 766,
    "Huntsville-Decatur (Florence), AL": 691,
    "Traverse City-Cadillac, MI": 540,
    "Twin Falls, ID": 760,
    "Raleigh-Durham (Fayetteville), NC": 560,
    "Burlington, VT-Plattsburgh, NY": 523,
    "Jackson, MS": 639,
    "Alpena, MI": 583,
    "Cedar Rapids-Waterloo-Iowa City & Dubuque, IA": 637,
    "Harlingen-Weslaco-Brownsville-McAllen, TX": 636,
    "Miami-Ft. Lauderdale, FL": 528,
    "Waco-Temple-Bryan, TX": 625,
    "Great Falls, MT": 755,
    "Watertown, NY": 549,
    "Champaign & Springfield-Decatur, IL": 648,
    "Nashville, TN": 659,
    "Butte-Bozeman, MT": 754,
    "Minot-Bismarck-Dickinson(Williston), ND": 687,
    "Bluefield-Beckley-Oak Hill, WV": 559,
    "Los Angeles, CA": 803,
    "Washington, DC (Hagerstown, MD)": 511,
    "Lubbock, TX": 651,
    "Glendive, MT": 798,
    "Grand Junction-Montrose, CO": 773,
    "Marquette, MI": 553,
    "Greenville-Spartanburg, SC-Asheville, NC-Anderson, SC": 545,
    "Knoxville, TN": 557,
    "Bangor, ME": 537,
    "Spokane, WA": 881,
    "Syracuse, NY": 555,
    "Wheeling, WV-Steubenville, OH": 554,
    "Little Rock-Pine Bluff, AR": 693,
    "Rochester, MN-Mason City, IA-Austin, MN": 611,
    "Evansville, IN": 649,
    "Lincoln & Hastings-Kearney, NE": 722,
    "Joplin, MO-Pittsburg, KS": 603,
    "San Diego, CA": 825,
    "Chicago, IL": 602,
    "Dothan, AL": 606,
    "Providence, RI-New Bedford, MA": 521,
    "Detroit, MI": 505,
    "Grand Rapids-Kalamazoo-Battle Creek, MI": 563,
    "Toledo, OH": 538,
    "Seattle-Tacoma, WA": 819,
    "Birmingham (Anniston and Tuscaloosa), AL": 630,
    "Charlotte, NC": 517,
    "Roanoke-Lynchburg, VA": 573,
    "Cleveland-Akron (Canton), OH": 510,
    "Sherman, TX-Ada, OK": 657,
    "Peoria-Bloomington, IL": 675,
    "Wichita-Hutchinson, KS Plus": 678,
    "North Platte, NE": 740,
    "St. Louis, MO": 609,
    "Albuquerque-Santa Fe, NM": 790,
    "St. Joseph, MO": 638,
    "Amarillo, TX": 634,
    "Chico-Redding, CA": 868,
    "Rockford, IL": 610,
    "Bend, OR": 821,
    "Myrtle Beach-Florence, SC": 570,
    "Johnstown-Altoona-State College, PA": 574,
    "Tyler-Longview(Lufkin & Nacogdoches), TX": 709,
    "Abilene-Sweetwater, TX": 662,
    "Parkersburg, WV": 597,
    "Pittsburgh, PA": 508,
    "Norfolk-Portsmouth-Newport News, VA": 544,
    "Cheyenne, WY-Scottsbluff, NE": 759,
    "Memphis, TN": 640,
    "Bowling Green, KY": 736,
    "Baltimore, MD": 512,
    "New York, NY": 501,
    "Binghamton, NY": 502,
    "Santa Barbara-Santa Maria-San Luis Obispo, CA": 855,
    "Oklahoma City, OK": 650,
    "Monterey-Salinas, CA": 828,
    "Omaha, NE": 652,
    "Meridian, MS": 711,
    "South Bend-Elkhart, IN": 588,
    "Orlando-Daytona Beach-Melbourne, FL": 534,
    "Des Moines-Ames, IA": 679,
    "Lansing, MI": 551,
    "Albany-Schenectady-Troy, NY": 532,
    "Boston, MA (Manchester, NH)": 506,
    "Chattanooga, TN": 575,
    "Eugene, OR": 801,
    "Springfield-Holyoke, MA": 543,
    "Sioux Falls(Mitchell), SD": 725,
    "Salisbury, MD": 576,
    "Jacksonville, FL": 561,
    "Davenport, IA-Rock Island-Moline, IL": 682,
    "Fairbanks, AK": 745,
    "Mankato, MN": 737,
    "Savannah, GA": 503,
    "Buffalo, NY": 514,
    "Ft. Myers-Naples, FL": 571,
    "Tampa-St. Petersburg (Sarasota), FL": 539,
    "Mobile, AL-Pensacola (Ft. Walton Beach), FL": 686,
    "Lafayette, LA": 582,
    "Idaho Falls-Pocatello, ID (Jackson, WY)": 758,
    "Indianapolis, IN": 527,
    "Lake Charles, LA": 643,
    "Portland-Auburn, ME": 500,
    "Wilmington, NC": 550,
    "Utica, NY": 526,
    "Richmond-Petersburg, VA": 556,
    "Victoria, TX": 626,
    "Duluth, MN-Superior, WI": 676,
    "Madison, WI": 669,
    "Columbia, SC": 546,
    "Hattiesburg-Laurel, MS": 710,
    "Columbus, GA (Opelika, AL)": 630,
    "West Palm Beach-Ft. Pierce, FL": 548,
    "Tucson (Sierra Vista), AZ": 789,
    "Erie, PA": 516,
    "Macon, GA": 503,
    "Terre Haute, IN": 581,
    "Youngstown, OH": 536,
    "Rochester, NY": 555,
    "Dallas-Ft. Worth, TX": 623,
    "Jackson, TN": 718,
    "Shreveport, LA": 612,
    "Lafayette, IN": 642,
    "Portland, OR": 820,
    "Austin, TX": 635,
    "Billings, MT": 756,
    "Casper-Riverton, WY": 767,
    "San Antonio, TX": 641,
    "Alexandria, LA": 644,
    "Quincy, IL-Hannibal, MO-Keokuk, IA": 717,
    "Rapid City, SD": 764,
    "Tallahassee, FL-Thomasville, GA": 530,
    "New Orleans, LA": 622,
    "Zanesville, OH": 596,
    "Panama City, FL": 656,
    "Salt Lake City, UT": 770,
    "Sioux City, IA": 624,
    "Boise, ID": 757,
    "Denver, CO": 751,
    "Odessa-Midland, TX": 633,
    "Philadelphia, PA": 504,
    "Yakima-Pasco-Richland-Kennewick, WA": 810,
    "Augusta, GA-Aiken, SC": 520,
    "Springfield, MO": 619,
    "WILKES BARRE-SCRANTON-HAZLETON, PA": 577,
    "JOHNSTOWN-ALTOONA-STATE COLLEGE, PA": 574,
    "IDAHO FALLS-POCATELLO, ID (JACKSON, WY)": 758,
    "COLUMBUS, GA (OPELIKA, AL)": 522,
    "National": 1000

}


# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  
dma_col = "Metro_Area"
cols = ["Date", "DMA_Code", "SubChannel", "Campaign"]

final_model_df = final_model_df.withColumn("DMA_Code", map_dma_name_to_code(dma_col))
final_model_df = final_model_df.groupBy(cols).agg(f.sum(f.col('spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('clicks')).alias('Clicks'))
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
norm_pop_prop_df = spark.createDataFrame(data_tuples, schema = ['DMA_Code', 'Prop'])

## Joining above df with final_model_df ##
final_model_df_national = final_model_df_national.crossJoin(norm_pop_prop_df)

## Dividing Metrics by 210 to distribute data at national level ##
cols = ["Spend", "Imps", "Clicks"]
for col in cols:
  final_model_df_national = final_model_df_national.withColumn(col,f.col(col)*f.col('Prop'))




## Re-arranging COlumns ##
cols = ['Date', 'DMA_Code', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df_national = final_model_df_national.select(cols)


## Combining back with DMA level Data ##
save_df = final_model_df_national.union(final_model_df_dma)
save_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Some More Updates

# COMMAND ----------

## Adding Channel Name ##
cols = ['Date', 'DMA_Code', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
save_df = save_df.withColumn('Channel', f.lit('Search')).select(cols) ## Re-arranging Columns 
save_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Save File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_IcmDigital_Microsoft" 
save_df_func(save_df, table_name)

# COMMAND ----------



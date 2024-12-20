# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

file_path = 'dbfs:/blend360/sandbox/mmm/brand_online/search_2022_2023.xlsx'
df = read_excel(file_path)

## Renaming column ##
rename_dict = {'Week Starting Monday':'Week',
                # 'Campaign Name': 'Campaign', 
                'Platform':'SubChannel',
                'DMA name or DMA number':'DmaName',
                'Impressions':'Imps'}
df = rename_cols_func(df, rename_dict)

## Removing comma from DmaName values ##
df = df.withColumn('DmaName',f.regexp_replace(df["DmaName"], ",", ""))

## Changing Date to First day of week as Monday and in required format (yyyy-MM-dd) ##
df = df.withColumn("Date", f.date_format( f.date_sub(f.to_date(f.col('Week'), 'yyyy-MM-dd'),f.dayofweek(f.to_date(f.col('Week'), 'yyyy-MM-dd'))-2) ,'yyyy-MM-dd'))


## Rearranging Columns ##
cols = ['Date', 'DmaName', 'SubChannel', 'Campaign Name', 'Spend', 'Imps', 'Clicks']
df = df.select(cols)

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Campaign Grouping

# COMMAND ----------

## Campaign Grouping ##
campaign_grp_file = 'dbfs:/blend360/sandbox/mmm/brand_online/campaign_groups/EM Updated - To Upload - search_2022_2023_campaigns.xlsx'
campaign_grp = read_excel(campaign_grp_file).drop('Campaign')
df = df.join(campaign_grp, on=['Campaign Name'], how='left')

## Renaming Columns ##
df = df.withColumnRenamed('Line of Business Updated', 'Campaign')

## Rearranging Columns ##
cols = ['Date', 'DmaName', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
df = df.select(cols)

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

dma_mapping = {'Odessa-Midland TX': 633,
 'North Platte NE': 740,
 'Flint-Saginaw-Bay City MI': 513,
 'Hattiesburg-Laurel MS': 710,
 'Phoenix (Prescott) AZ': 753,
 'Toledo OH': 547,
 'Austin TX': 635,
 'Portland-Auburn ME': 500,
 'Buffalo NY': 514,
 'Chicago IL': 602,
 'Washington DC (Hagerstown MD)': 511,
 'Raleigh-Durham (Fayetteville) NC': 560,
 'Portland OR': 820,
 'Harrisonburg VA': 569,
 'JOHNSTOWN-ALTOONA-STATE COLLEGE PA': 574,
 'Springfield MO': 619,
 'Columbus-Tupelo-West Point MS': 673,
 'Wichita-Hutchinson KS': 678,
 'Zanesville OH': 596,
 'Jackson MS': 718,
 'Topeka KS': 605,
 'Bowling Green KY': 736,
 'Columbus-Tupelo-West Point-Houston MS': 673,
 'Lexington KY': 541,
 'La Crosse-Eau Claire WI': 702,
 'Boston MA-Manchester NH': 506,
 'Baton Rouge LA': 716,
 'Lafayette LA': 642,
 'Albany GA': 525,
 'Kansas City MO': 616,
 'Paducah KY-Cape Girardeau MO-Harrisburg-Mount Vernon IL': 632,
 'Quincy IL-Hannibal MO-Keokuk IA': 717,
 'Louisville KY': 529,
 'Santa Barbara-Santa Maria-San Luis Obispo CA': 855,
 'Victoria TX': 626,
 'Charleston SC': 519,
 'Cheyenne WY-Scottsbluff NE': 759,
 'Providence-New BedfordMA': 521,
 'Eugene OR': 801,
 'Macon GA': 503,
 'Anchorage AK': 743,
 'COLUMBUS GA (OPELIKA AL)': 522,
 'Dayton OH': 542,
 'Lake Charles LA': 643,
 'Madison WI': 669,
 'Waco-Temple-Bryan TX': 625,
 'Youngstown OH': 536,
 'West Palm Beach-Ft. Pierce FL': 548,
 'Fresno-Visalia CA': 866,
 'Medford-Klamath Falls OR': 813,
 'Paducah KY-Cape Girardeau MO-Harrisburg IL': 632,
 'Tulsa OK': 671,
 'Roanoke-Lynchburg VA': 573,
 'St. Louis MO': 609,
 'Twin Falls ID': 760,
 'Wichita-Hutchinson KS Plus': 678,
 'Johnstown-Altoona-State College PA': 574,
 'Alexandria LA': 644,
 'Erie PA': 516,
 'Sioux City IA': 624,
 'Traverse City-Cadillac MI': 540,
 'Lima OH': 558,
 'Fairbanks AK': 745,
 'Myrtle Beach-Florence SC': 570,
 'Miami-Ft. Lauderdale FL': 528,
 'Tallahassee FL-Thomasville GA': 530,
 'Richmond-Petersburg VA': 556,
 'New Orleans LA': 622,
 'FARGO': 724,
 'Savannah GA': 507,
 'Panama City FL': 656,
 'Rochester NY': 538,
 'WILKES BARRE-SCRANTON-HAZLETON PA': 577,
 'Grand Rapids-Kalamazoo-Battle Creek MI': 563,
 'Greensboro-High Point-Winston Salem NC': 518,
 'Huntsville-Decatur (Florence) AL': 691,
 'Tampa-St. Petersburg (Sarasota) FL': 539,
 'New York NY': 501,
 'Augusta GA-Aiken SC': 520,
 'Laredo TX': 749,
 'Salisbury MD': 576,
 'Spokane WA': 881,
 'Albuquerque-Santa Fe NM': 790,
 'Idaho Falls-Pocatello ID': 758,
 'Meridian MS': 711,
 'Charlottesville VA': 584,
 'Nashville TN': 659,
 'Providence RI-New Bedford MA': 521,
 'Los Angeles CA': 803,
 'Wilkes Barre-Scranton PA': 577,
 'Bakersfield CA': 800,
 'Little Rock-Pine Bluff AR': 693,
 'Marquette MI': 553,
 'Norfolk-Portsmouth-Newport News VA': 544,
 'Albany-Schenectady-Troy NY': 532,
 'Indianapolis IN': 527,
 'Lansing MI': 551,
 'Palm Springs CA': 804,
 'South Bend-Elkhart IN': 588,
 'Missoula MT': 762,
 'Rockford IL': 610,
 'San Diego CA': 825,
 'Sherman TX-Ada OK': 657,
 'Columbia SC': 546,
 'Springfield-Holyoke MA': 543,
 'Watertown NY': 549,
 'Philadelphia PA': 504,
 'Greenville-Spartanburg-Asheville-Anderson': 567,
 'Joplin MO-Pittsburg KS': 603,
 'Monterey-Salinas CA': 828,
 'Pittsburgh PA': 508,
 'Charlotte NC': 517,
 'Corpus Christi TX': 600,
 'Ft. Wayne IN': 509,
 'Birmingham (Ann and Tusc) AL': 630,
 'Chattanooga TN': 575,
 'Wausau-Rhinelander WI': 705,
 'El Paso TX (Las Cruces NM)': 765,
 'Ft. Smith-Fayetteville-Springdale-Rogers AR': 670,
 'Monroe LA-El Dorado AR': 628,
 'Atlanta GA': 524,
 'Gainesville FL': 592,
 'Memphis TN': 640,
 'Reno NV': 811,
 'Ottumwa IA-Kirksville MO': 631,
 'Terre Haute IN': 581,
 'Glendive MT': 798,
 'Lubbock TX': 651,
 'Phoenix AZ': 753,
 'Cleveland-Akron (Canton) OH': 510,
 'Evansville IN': 649,
 'Sacramento-Stockton-Modesto CA': 862,
 'Eureka CA': 802,
 'Sherman-Ada OK': 657,
 'Chico-Redding CA': 868,
 'Clarksburg-Weston WV': 598,
 'Tyler-Longview(Lufkin & Nacogdoches) TX': 709,
 'Biloxi-Gulfport MS': 746,
 'Yakima-Pasco-Richland-Kennewick WA': 810,
 'Elmira (Corning) NY': 565,
 'Minneapolis-St. Paul MN': 613,
 'Denver CO': 751,
 'Honolulu HI': 744,
 'Boston MA (Manchester NH)': 506,
 'Ft. Myers-Naples FL': 571,
 'San Angelo TX': 661,
 'Lafayette IN': 582,
 'Parkersburg WV': 597,
 'Wheeling WV-Steubenville OH': 554,
 'Salt Lake City UT': 770,
 'Burlington VT-Plattsburgh NY': 523,
 'Des Moines-Ames IA': 679,
 'Green Bay-Appleton WI': 658,
 'Peoria-Bloomington IL': 675,
 'Amarillo TX': 634,
 'Harlingen-Weslaco-Brownsville-McAllen TX': 636,
 'Fargo-Valley City ND': 724,
 'Rochester-Mason City-AustinIA': 611,
 'St. Joseph MO': 638,
 'Montgomery-Selma AL': 698,
 'Bluefield-Beckley-Oak Hill WV': 559,
 'Dothan AL': 606,
 'Knoxville TN': 557,
 'Las Vegas NV': 839,
 'Augusta GA': 520,
 'Shreveport LA': 612,
 'Billings MT': 756,
 'Lincoln & Hastings-Kearney NE': 722,
 'Yuma AZ-El Centro CA': 771,
 'Baltimore MD': 512,
 'Mankato MN': 737,
 'Hartford & New Haven CT': 533,
 'Florence-Myrtle Beach SC': 570,
 'Greenwood-Greenville MS': 647,
 'Harrisburg-Lancaster-Lebanon-York PA': 566,
 'Boise ID': 757,
 'Oklahoma City OK': 650,
 'Dallas-Ft. Worth TX': 623,
 'Seattle-Tacoma WA': 819,
 'Minot-Bismarck-Dickinson(Williston) ND': 687,
 'IDAHO FALLS-POCATELLO ID (JACKSON WY)': 758,
 'Rapid City SD': 764,
 'Utica NY': 526,
 'Colorado Springs-Pueblo CO': 752,
 'Sioux Falls(Mitchell) SD': 725,
 'Grand Junction-Montrose CO': 773,
 'Detroit MI': 505,
 'Syracuse NY': 555,
 'Birmingham (Anniston and Tuscaloosa) AL': 630,
 'Wilmington NC': 550,
 'Charleston-Huntington WV': 564,
 'Orlando-Daytona Beach-Melbourne FL': 534,
 'Champaign & Springfield-Decatur IL': 648,
 'Duluth MN-Superior WI': 676,
 'Columbus OH': 535,
 'Alpena MI': 583,
 'El Paso TX': 765,
 'Abilene-Sweetwater TX': 662,
 'Tucson (Sierra Vista) AZ': 789,
 'Jacksonville FL': 561,
 'Cedar Rapids-Waterloo-Iowa City & Dubuque IA': 637,
 'Greenville-New Bern-Washington NC': 545,
 'Greenville-Spartanburg SC-Asheville NC-Anderson SC': 567,
 'San Antonio TX': 641,
 'Beaumont-Port Arthur TX': 692,
 'Great Falls MT': 755,
 'Omaha NE': 652,
 'Columbus GA': 522,
 '(blank)': 1000,
 'Mobile AL-Pensacola (Ft. Walton Beach) FL': 686,
 'Bangor ME': 537,
 'Tri-Cities TN-VA': 531,
 'Butte-Bozeman MT': 754,
 'Cincinnati OH': 515,
 'Helena MT': 766,
 'Binghamton NY': 502,
 'Houston TX': 618,
 'Jonesboro AR': 734,
 'Columbia-Jefferson City MO': 604,
 'Rochester MN-Mason City IA-Austin MN': 611,
 'Juneau AK': 747,
 'San Francisco-Oakland-San Jose CA': 807,
 'Wichita Falls TX & Lawton OK': 627,
 'Casper-Riverton WY': 767,
 'Presque Isle ME': 552,
 'Bend OR': 821,
 'Jackson TN': 639,
 'Davenport IA-Rock Island-Moline IL': 682,
 'Milwaukee WI': 617}

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
  new_df = spark.sparkContext.parallelize([original_row]*210).toDF()

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
final_model_df = df_remain.union(df1.select(cols))

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
final_model_df = final_model_df.withColumn('Channel', f.lit('Search')).select(cols) ## Re-arranging Columns 
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_MediaCom_Search" 
save_df_func(final_model_df, table_name)

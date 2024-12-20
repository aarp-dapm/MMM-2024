# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

file_path = 'dbfs:/blend360/sandbox/mmm/brand_online/social_2022_2023.xlsx'
df = read_excel(file_path)

## Renaming column ##
rename_dict = {'Week Starting Monday':'Week',
                # 'Campaign Name': 'Campaign', 
                'Platform':'SubChannel',
                'DMA name or DMA number':'DmaName',
                'Impressions':'Imps'}
df = rename_cols_func(df, rename_dict)

## Removing comma from DmaName values ##
df = df.withColumn('DmaName',f.regexp_replace(df["DmaName"], ",DMA®", ""))
df = df.withColumn('DmaName',f.regexp_replace(df["DmaName"], ", US", ""))

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
campaign_grp_file = 'dbfs:/blend360/sandbox/mmm/brand_online/campaign_groups/EM Updated - To Upload - social_2022_2023_campaigns.xlsx' 
campaign_grp = read_excel(campaign_grp_file).drop('Campaign')
df = df.join(campaign_grp, on=['Campaign Name'], how='left')


## Renaming Columns ##
df = df.withColumnRenamed('Line of Business Renamed', 'Campaign')

## Rearranging Columns ##
cols = ['Date', 'DmaName', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
df = df.select(cols)

df.display()

# COMMAND ----------

'''
Below Campaign Grouping is commendted out for now , above campaign groups are used 

'''
# SubChannel_col  = 'SubChannel'
# campaign_col = 'Campaign'

# condition = when(f.col(SubChannel_col)=='Meta',
#                  when(f.lower(f.col(campaign_col)).like('%reach%'), 'Reach').
#                  when(f.lower(f.col(campaign_col)).like('%relevance%'), 'Relevance').
#                  when(f.lower(f.col(campaign_col)).like('%reaction%'), 'Reaction').otherwise('Tragetted Com.'))\
#             .when(f.col(SubChannel_col)=='TikTok',
#                  when(f.lower(f.col(campaign_col)).like('%relevance%'), 'Relevance').
#                  when(f.lower(f.col(campaign_col)).like('%reaction%'), 'Reaction').otherwise('Other'))\
#             .otherwise(f.col(SubChannel_col))
            

# df = df.withColumn('Campaign', condition)

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

dma_mapping = {'Fairbanks': 745,
 'Hartford & New Haven': 533,
 'Bowling Green': 736,
 'Odessa-Midland TX': 633,
 'North Platte NE': 740,
 'Butte-Bozeman': 754,
 'Sherman-Ada': 657,
 'Flint-Saginaw-Bay City MI': 513,
 'Hattiesburg-Laurel MS': 710,
 'Champaign-Springfield-Decatur': 648,
 'Savannah': 507,
 'Lima': 558,
 'Toledo OH': 547,
 'Omaha': 652,
 'Anchorage': 743,
 'Idaho Fals-Pocatllo(Jcksn)': 758,
 'Austin TX': 635,
 'Portland-Auburn ME': 500,
 'Montgomery-Selma': 698,
 'Ottumwa-Kirksville': 631,
 'Columbus, GA (Opelika, AL)': 522,
 'El Paso (Las Cruces)': 765,
 'Wheeling-Steubenville': 554,
 'Buffalo NY': 514,
 'Chicago IL': 602,
 'Chico-Redding': 868,
 'Little Rock-Pine Bluff': 693,
 'Monroe-El Dorado': 628,
 'Chattanooga': 575,
 'Rochestr-Mason City-Austin': 611,
 'Great Falls': 755,
 'Portland OR': 820,
 'Birmingham (Ann And Tusc)': 630,
 'Laredo': 749,
 'Columbus-Tupelo-West Point MS': 673,
 'Harrisonburg VA': 569,
 'Springfield MO': 619,
 'Wichita-Hutchinson KS': 678,
 'Cedar Rapids-Waterloo-Iowa City-Dubuque': 637,
 'Zanesville OH': 596,
 'Jackson MS': 718,
 'Topeka KS': 605,
 'Columbus, OH': 535,
 'Bowling Green KY': 736,
 'Cleveland-Akron OH': 510,
 'Lexington KY': 541,
 'Traverse City-Cadillac': 540,
 'Boston MA-Manchester NH': 506,
 'La Crosse-Eau Claire WI': 702,
 'Bakersfield': 800,
 'Greenvll-Spart-Ashevll-And': 567,
 'San Antonio': 641,
 'Baton Rouge LA': 716,
 'Lafayette LA': 642,
 'Albany GA': 525,
 'Kansas City MO': 616,
 'Mobile-Pensacola (Navarre)': 686,
 'Lincoln & Hastings-Krny': 722,
 'Louisville': 529,
 'Philadelphia': 504,
 'Paducah KY-Cape Girardeau MO-Harrisburg-Mount Vernon IL': 632,
 'Dayton': 542,
 'Quincy IL-Hannibal MO-Keokuk IA': 717,
 'Santa Barbara-Santa Maria-San Luis Obispo CA': 855,
 'Los Angeles': 803,
 'Victoria TX': 626,
 'Charleston SC': 519,
 'Cheyenne WY-Scottsbluff NE': 759,
 'Greenville-New Bern-Washington': 545,
 'Eugene OR': 801,
 'Macon GA': 503,
 'Johnstown-Altoona-State College': 574,
 'Utica': 526,
 'Wichita-Hutchinson Plus': 678,
 'Anchorage AK': 743,
 'Dayton OH': 542,
 'Mobile-Pensacola (Ft Walt)': 686,
 'Lake Charles LA': 643,
 'Madison WI': 669,
 'Waco-Temple-Bryan TX': 625,
 'Youngstown OH': 536,
 'Columbus, GA (Opelika, Al)': 522,
 'Presque Isle': 552,
 'Dothan': 606,
 'Grand Junction-Montrose': 773,
 'Washington (Hagerstown)': 511,
 'Fresno-Visalia CA': 866,
 'Medford-Klamath Falls OR': 813,
 'Sacramento-Stockton-Modesto': 862,
 'Indianapolis': 527,
 'Tulsa OK': 671,
 'Roanoke-Lynchburg VA': 573,
 'St. Louis MO': 609,
 'Twin Falls ID': 760,
 'Champaign&Sprngfld-Decatur': 648,
 'Charleston, SC': 519,
 'Fargo-Valley City': 724,
 'Alexandria LA': 644,
 'Erie PA': 516,
 'Topeka': 605,
 'Victoria': 626,
 'Nashville': 659,
 'Oklahoma City': 650,
 'Rockford': 610,
 'San Diego': 825,
 'South Bend-Elkhart': 588,
 'Mobile AL-Pensacola FL': 686,
 'Sioux City IA': 624,
 'Traverse City-Cadillac MI': 540,
 'Ft. Smith-Fay-Sprngdl-Rgrs': 670,
 'Lima OH': 558,
 'Wichita-Hutchinson': 678,
 'Albany, GA': 525,
 'Davenport-Rock Island-Moline': 682,
 'Detroit': 505,
 'Mankato': 737,
 'Fairbanks AK': 745,
 'Bluefield-Beckley-Oak Hill': 559,
 'Huntsville-Decatur (Flor)': 691,
 'Tallahassee FL-Thomasville GA': 530,
 'Miami-Fort Lauderdale': 528,
 'Harrisburg-Lncstr-Leb-York': 566,
 'Richmond-Petersburg VA': 556,
 'Wilkes-Barre–Scranton–Hazleton': 577,
 'Columbia-Jefferson City': 604,
 'Springfield-Holyoke': 543,
 'Tri-Cities, TN-VA': 531,
 'New Orleans LA': 622,
 'Boise': 757,
 'Savannah GA': 507,
 'West Palm Beach-Fort Pierce FL': 548,
 'Baton Rouge': 716,
 'Minneapolis-St. Paul': 613,
 'Orlando-Daytona Bch-Melbrn': 534,
 'Panama City FL': 656,
 'Rochester NY': 538,
 'St. Louis': 609,
 'Beaumont-Port Arthur': 692,
 'Clarksburg-Weston': 598,
 'Grand Rapids-Kalamazoo-Battle Creek MI': 563,
 'Colorado Springs-Pueblo': 752,
 'Greensboro-High Point-Winston Salem NC': 518,
 'Parkersburg': 597,
 'Myrtle Beach-Florence': 570,
 'New York NY': 501,
 'Laredo TX': 749,
 'Salisbury MD': 576,
 'Jackson, MS': 718,
 'Spokane WA': 881,
 'Abilene-Sweetwater': 662,
 'Evansville': 649,
 'Youngstown': 536,
 'Memphis': 640,
 'Minot-Bsmrck-Dcknsn(Wlstn)': 687,
 'Tyler-Longview(Lfkn&Nacogdoches)': 709,
 'Albuquerque-Santa Fe NM': 790,
 'Idaho Falls-Pocatello ID': 758,
 'Harlingen-Wslco-Brnsvl-Mca': 636,
 'Meridian MS': 711,
 'Austin': 635,
 'Dallas-Ft. Worth': 623,
 'Grand Rapids-Kalmzoo-B.Crk': 563,
 'Charlottesville VA': 584,
 'Norfolk-Portsmouth-Newport News': 544,
 'Madison': 669,
 'Nashville TN': 659,
 'Providence RI-New Bedford MA': 521,
 'Milwaukee': 617,
 'Casper-Riverton': 767,
 'Pittsburgh': 508,
 'St. Joseph': 638,
 'Los Angeles CA': 803,
 'Wilkes Barre-Scranton PA': 577,
 'Richmond-Petersburg': 556,
 'Bakersfield CA': 800,
 'Little Rock-Pine Bluff AR': 693,
 'Marquette MI': 553,
 'Green Bay-Appleton': 658,
 'Waco-Temple-Bryan': 625,
 'Unknown': 1000,
 'Lincoln-Hastings-Kearney': 722,
 'Lansing': 551,
 'Tampa-St. Pete (Sarasota)': 539,
 'Norfolk-Portsmouth-Newport News VA': 544,
 'Biloxi-Gulfport': 746,
 'Cedar Rapids-Wtrlo-Iwc&Dub': 682,
 'Chicago': 602,
 'Sioux Falls(Mitchell)': 725,
 'Lubbock': 651,
 'Tyler-Longview TX': 709,
 'Greensboro–High Point–Winston-Salem': 518,
 'Minot-Bismarck-Dickinson (Williston)': 687,
 'Albany-Schenectady-Troy NY': 532,
 'Indianapolis IN': 527,
 'Lansing MI': 551,
 'Palm Springs CA': 804,
 'Providence-New Bedford': 521,
 'Seattle-Tacoma': 819,
 'South Bend-Elkhart IN': 588,
 'Missoula MT': 762,
 'Rockford IL': 610,
 'San Diego CA': 825,
 'Sherman TX-Ada OK': 657,
 'Albuquerque-Santa Fe': 790,
 'Norfolk-Portsmth-Newpt Nws': 544,
 'Columbia SC': 546,
 'Wausau-Rhinelander': 705,
 'Springfield-Holyoke MA': 543,
 'Watertown NY': 549,
 'Quincy-Hannibal-Keokuk': 717,
 'Toledo': 547,
 'Philadelphia PA': 504,
 'Albany-Schenectady-Troy': 532,
 'Boston (Manchester)': 506,
 'Erie': 516,
 'Corpus Christi': 600,
 'Greenville-Spartanburg-Asheville-Anderson': 567,
 'Shreveport': 612,
 'Wilmington': 550,
 'Huntsville-Decatur AL': 691,
 'Joplin MO-Pittsburg KS': 603,
 'Monterey-Salinas CA': 828,
 'Pittsburgh PA': 508,
 'Fort Smith-Fayetteville-Springdale-Rogers': 670,
 'Amarillo': 634,
 'Raleigh-Durham (Fayetvlle)': 560,
 'Yuma-El Centro': 771,
 'Charlotte NC': 517,
 'Corpus Christi TX': 600,
 'Des Moines-Ames': 679,
 'San Francisco-Oak-San Jose': 807,
 'Sioux Falls SD': 725,
 'Chattanooga TN': 575,
 'Wausau-Rhinelander WI': 705,
 'Tyler-Longview (Lufkin-Nacogdoches)': 709,
 'Macon': 503,
 'Monroe LA-El Dorado AR': 628,
 'Glendive': 798,
 'Atlanta GA': 524,
 'Gainesville FL': 592,
 'Memphis TN': 640,
 'Atlanta': 524,
 'Reno': 811,
 'Reno NV': 811,
 'Tampa-St. Petersburg (Sarasota)': 539,
 'New Orleans': 622,
 'Ottumwa IA-Kirksville MO': 631,
 'Las Vegas': 839,
 'Lexington': 541,
 'Terre Haute IN': 582,
 'Santa Barbara-Santa Maria-San Luis Obispo': 855,
 'Glendive MT': 798,
 'Bend, OR': 821,
 'Washington, DC (Hagrstwn)': 511,
 'Wilkes Barre-Scranton-Hztn': 577,
 'Lubbock TX': 651,
 'Phoenix AZ': 753,
 'Charlottesville': 584,
 'Honolulu': 744,
 'Evansville IN': 649,
 'Eureka CA': 802,
 'Sacramento-Stockton-Modesto CA': 862,
 'Johnstown-Altoona-St Colge': 574,
 'Cheyenne-Scottsbluff': 759,
 'Chico-Redding CA': 868,
 'Binghamton': 502,
 'Peoria-Bloomington': 675,
 'Birmingham AL': 630,
 'Clarksburg-Weston WV': 598,
 'San Angelo': 661,
 'Yakima-Pasco-Richland-Kennewick': 810,
 'Hartford-New Haven': 533,
 'Joplin-Pittsburg': 603,
 'Biloxi-Gulfport MS': 746,
 'San Francisco-Oakland-San Jose': 807,
 'Gainesville': 592,
 'Roanoke-Lynchburg': 573,
 'Cleveland-Akron (Canton)': 510,
 'Davenport-R.Island-Moline': 682,
 'Yakima-Pasco-Richland-Kennewick WA': 810,
 'Minneapolis-St. Paul MN': 613,
 'Columbus-Tupelo-W Pnt-Hstn': 673,
 'Odessa-Midland': 633,
 'Denver CO': 751,
 'Honolulu HI': 744,
 'Palm Springs': 804,
 'Minot-Bismarck-Dickinson ND': 687,
 'Rochester-Mason City-Austin': 611,
 'Harlingen-Weslaco-Brownsville-McAllen': 636,
 'Harrisonburg': 569,
 'Lake Charles': 643,
 'San Angelo TX': 661,
 'Lafayette IN': 582,
 'Parkersburg WV': 597,
 'Wheeling WV-Steubenville OH': 554,
 'Charleston-Huntington': 564,
 'Birmingham (Anniston-Tuscaloosa)': 630,
 'Hattiesburg-Laurel': 710,
 'Salt Lake City UT': 770,
 'Duluth-Superior': 676,
 'Burlington VT-Plattsburgh NY': 523,
 'Des Moines-Ames IA': 679,
 'Harrisburg-Lancaster-Lebanon-York': 566,
 'Paducah-Cape Girardeau-Harrisburg': 632,
 'Green Bay-Appleton WI': 658,
 'Ft. Myers-Naples': 571,
 'Kansas City': 616,
 'Miami-Fort Lauderdale FL': 528,
 'Peoria-Bloomington IL': 675,
 'Eureka': 802,
 'Amarillo TX': 634,
 'Fargo-Valley City ND': 724,
 'Harlingen-Weslaco-Brownsville-McAllen TX': 636,
 'St. Joseph MO': 638,
 'Spokane': 881,
 'Washington DC': 511,
 'Lafayette, LA': 642,
 'Bluefield-Beckley-Oak Hill WV': 559,
 'Dothan AL': 606,
 'Knoxville TN': 557,
 'Missoula': 762,
 'Salisbury': 576,
 'Augusta GA': 520,
 'Las Vegas NV': 839,
 'Shreveport LA': 612,
 'Burlington-Plattsburgh': 523,
 'Billings MT': 756,
 'Lincoln & Hastings-Kearney NE': 722,
 'Yuma AZ-El Centro CA': 771,
 'Juneau': 747,
 'Marquette': 553,
 'Baltimore MD': 512,
 'Mankato MN': 737,
 'Florence-Myrtle Beach SC': 570,
 'Hartford & New Haven CT': 533,
 'Billings': 756,
 'Greenwood-Greenville MS': 647,
 'Harrisburg-Lancaster-Lebanon-York PA': 566,
 'Dallas-Fort Worth': 623,
 'Bangor': 537,
 'Greenville-N.Bern-Washngtn': 545,
 'Helena': 766,
 'Boise ID': 757,
 'Oklahoma City OK': 650,
 'Tucson AZ': 789,
 'Greenwood-Greenville': 647,
 'Seattle-Tacoma WA': 819,
 'Tampa-St. Petersburg FL': 539,
 'Columbus-Tupelo-West Point-Houston': 673,
 'Columbia, SC': 546,
 'Houston': 618,
 'Terre Haute': 581,
 'Rapid City SD': 764,
 'Utica NY': 526,
 'Medford-Klamath Falls': 813,
 'Colorado Springs-Pueblo CO': 752,
 'Montgomery AL': 698,
 'La Crosse-Eau Claire': 702,
 'Jacksonville': 561,
 'Portland-Auburn': 500,
 'Twin Falls': 760,
 'Grand Junction-Montrose CO': 773,
 'Detroit MI': 505,
 'Jonesboro': 734,
 'Paducah-Cape Girard-Harsbg': 632,
 'Syracuse NY': 555,
 'Tulsa': 671,
 'Wilmington NC': 550,
 'Charleston-Huntington WV': 564,
 'Orlando-Daytona Beach-Melbourne FL': 534,
 'Flint-Saginaw-Bay City': 513,
 'Rochester, NY': 538,
 'Sacramnto-Stkton-Modesto': 862,
 'West Palm Beach-Ft. Pierce': 548,
 'Fresno-Visalia': 866,
 'Jackson, TN': 639,
 'Alpena': 583,
 'Buffalo': 514,
 'Knoxville': 557,
 'Miami-Ft. Lauderdale': 528,
 'Duluth MN-Superior WI': 676,
 'Fort Myers-Naples FL': 571,
 'Grand Rapids-Kalamazoo-Battle Creek': 563,
 'Tucson (Sierra Vista)': 789,
 'Columbus OH': 535,
 'Lafayette, IN': 582,
 'Alpena MI': 583,
 'El Paso TX': 765,
 'Portland, OR': 820,
 'Wichita Falls & Lawton': 627,
 'Cincinnati': 515,
 'Abilene-Sweetwater TX': 662,
 'Fort Smith-Fayetteville-Springdale-Rogers AR': 670,
 'Fort Myers-Naples': 571,
 'Raleigh-Durham (Fayetteville)': 560,
 'Alexandria, LA': 644,
 'Jacksonville FL': 561,
 'Johnstown-Altoona PA': 574,
 'Cedar Rapids-Waterloo-Iowa City & Dubuque IA': 637,
 'Greenville-New Bern-Washington NC': 545,
 'Watertown': 549,
 'Elmira NY': 565,
 'Panama City': 656,
 'Santabarbra-Sanmar-Sanluob': 855,
 'Syracuse': 555,
 'Fort Wayne IN': 509,
 'Orlando-Daytona Beach-Melbourne': 534,
 'Salt Lake City': 770,
 'Greenville-Spartanburg SC-Asheville NC-Anderson SC': 567,
 'San Antonio TX': 641,
 'Huntsville-Decatur (Florence)': 691,
 'Baltimore': 512,
 'Elmira (Corning)': 565,
 'Yakima-Pasco-Rchlnd-Knnwck': 810,
 'Beaumont-Port Arthur TX': 692,
 'Great Falls MT': 755,
 'Columbus GA': 522,
 'Omaha NE': 652,
 'Ft. Wayne': 509,
 'Dallas-Fort Worth TX': 623,
 'Duluth-Superior, WI': 676,
 'Bangor ME': 537,
 'Raleigh-Durham NC': 560,
 'Tri-Cities TN-VA': 531,
 'Fargo-Moorhead-Grand Forks': 724,
 'New York': 501,
 'Butte-Bozeman MT': 754,
 'Cincinnati OH': 515,
 'Helena MT': 766,
 'Binghamton NY': 502,
 'Houston TX': 618,
 'Jonesboro AR': 734,
 'Idaho Falls-Pocatello (Jackson)': 758,
 'Charlotte': 517,
 'Monterey-Salinas': 828,
 'Sioux City': 624,
 'North Platte': 740,
 'Columbia-Jefferson City MO': 604,
 'Eugene': 801,
 'Meridian': 711,
 'Rochester MN-Mason City IA-Austin MN': 611,
 'Augusta-Aiken': 520,
 'Greensboro-H.Point-W.Salem': 518,
 'Juneau AK': 747,
 'San Francisco-Oakland-San Jose CA': 807,
 'Wichita Falls TX & Lawton OK': 627,
 'West Palm Beach-Fort Pierce': 548,
 'Denver': 751,
 'Rapid City': 764,
 'Casper-Riverton WY': 767,
 'Presque Isle ME': 552,
 'Phoenix (Prescott)': 753,
 'Tallahassee-Thomasville': 530,
 'Bend OR': 821,
 'Jackson TN': 639,
 'Wheeling-Steubenville, OH': 554,
 'Springfield, MO': 619,
 'Davenport IA-Rock Island-Moline IL': 682,
 'Milwaukee WI': 617,
 'Champaign & Springfield-Decatur,IL': 648,
 'Fort Wayne': 509,
 'Louisville KY': 529,
 'Tyler-Longview(Lfkn&Ncgd)': 709,
 'Zanesville': 596}

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
try: 
  ch_dma_code.remove('1000')
except:
  pass

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
final_model_df = final_model_df.withColumn('Channel', f.lit('Social')).select(cols) ## Re-arranging Columns 
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_MediaCom_Social" 
save_df_func(final_model_df, table_name)

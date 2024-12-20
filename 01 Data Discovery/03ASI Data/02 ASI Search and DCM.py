# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## DCM: Health and Discount ##

# COMMAND ----------

# DCM: Health and Discount #
excel_file_path = "dbfs:/blend360/sandbox/mmm/asi_phase2/Discounts/DCM 22-23/CM360_AARP_DMA_Display_weekly_.xlsx" ## Health and Discount : DCM
df_dcm = read_excel(excel_file_path) ## first day of week is sunday ##

## Changing First day to Monday ##
df_dcm = df_dcm.withColumn("Date",f.date_add(f.col('Week'),1))

## Creating Column for Discount and Health ##
health_list = ['AARP_Health_Member Advantages_DIS','AARP_Health_Member Advantages_EMAIL','AARPHEALTH-2020-Display','AARP-HEALTH-2022','AARP_Health_Member Advantages_DIS',
'AARP_Health_Member Advantages_EMAIL','AARPHEALTH-2020-Display','AARP-HEALTH-2023','H2 2023 AARP HEALTH','H2 2023 AARP HEALTH V2']
discount_list = ['AARP_Discounts_Member Advantages_DIS','AARPDISCOUNTS-2020-DISPLAY','AARP-DISCOUNTS-2021','AARP-DISCOUNTS-2022-1','AARP_Discounts_Member Advantages_DIS',
'AARPDISCOUNTS-2020-DISPLAY','AARP-DISCOUNTS-2021','AARP-DISCOUNTS-2022-1','AARP-DISCOUNTS-2023','H2 2023 AARP DISCOUNTS - Right Time Targeting','H2 2023 AARP DISCOUNTS V2']
df_dcm = df_dcm.withColumn('Campaign', when(f.col('Campaign Name').isin(health_list), 'Health').when(f.col('Campaign Name').isin(discount_list), 'Discount').otherwise('NULL'))

df_dcm = df_dcm.withColumn('SubChannel', f.lit('Display'))

## Filter for Campaign Name '--'##
df_dcm = df_dcm.filter(~f.col('Campaign Name').isin('---'))

df_dcm = df_dcm.filter(~f.col('Publisher (CM360)').isin(['teads.tv', 'yelp.com'])) ## Getting complete data from MSIGHTS for these two campiagn sources, hence filtering for them

## Changing Column Names ##
df_dcm = df_dcm.withColumnRenamed('Impressions','Imps')
df_dcm = df_dcm.withColumnRenamed( 'Designated Market Area (DMA)','DmaName')
df_dcm = df_dcm.withColumn('Cost', f.col('Media Cost')+f.col('DV360 Cost USD'))

df_dcm = df_dcm.select('Date', 'DmaName', 'SubChannel', 'Campaign', 'Cost', 'Imps', 'Clicks')
# df_dcm.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DCM : Adding Missing Data 1 - Discount & Health

# COMMAND ----------

# Display: DCM #
file_path = 'dbfs:/blend360/sandbox/mmm/asi_phase2/other_supplementry_files/Other Files MSIGHTS_Google Campaigns by Week - 2022 2023_Cross LoB.xlsx'
df_discount_dcm = read_excel(file_path)
df_discount_dcm = df_discount_dcm.withColumn("Date", f.to_date(f.col('Week Of'), 'yyyy-MM-dd'))

## Adding Column for Health_GoogleSearch ##
df_discount_dcm = df_discount_dcm.withColumn("SubChannel",f.lit('Display'))
df_discount_dcm = df_discount_dcm.withColumn('Campaign', when( f.col('Campaign Category').isin(['Healthcare & Insurance']) ,f.lit('Health')).otherwise(f.lit('Discount')))
df_discount_dcm = df_discount_dcm.withColumn('DmaName', f.lit('No Metro'))

## Renaming Column ##
df_discount_dcm = df_discount_dcm.withColumnRenamed('Impressions Delivered','Imps')
df_discount_dcm = df_discount_dcm.withColumnRenamed('Media Spend','Cost')
df_discount_dcm = df_discount_dcm.withColumnRenamed('Clicks Delivered','Clicks')

df_discount_dcm = df_discount_dcm.select('Date', 'DmaName', 'SubChannel', 'Campaign', 'Cost', 'Imps', 'Clicks')
# df_discount_dcm.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DCM : Adding Missing Data 2 - Discount & Health

# COMMAND ----------

# Display: DCM #
file_path = 'dbfs:/blend360/sandbox/mmm/asi_phase2/other_supplementry_files/MSIGHTS Cross LoB Various Display Publishers Non Google of 17-05-2024.xlsx'
df_discount_dcm2 = read_excel(file_path)
df_discount_dcm2 = df_discount_dcm2.withColumn("Date", f.to_date(f.col('Week Of'), 'yyyy-MM-dd'))

## Adding Column for Health_GoogleSearch ##
df_discount_dcm2 = df_discount_dcm2.withColumn("SubChannel",f.lit('Display'))
df_discount_dcm2 = df_discount_dcm2.withColumn('Campaign', when( f.col('Campaign Category').isin(['Healthcare & Insurance']) ,f.lit('Health')).otherwise(f.lit('Discount')))
df_discount_dcm2 = df_discount_dcm2.withColumn('DmaName', f.lit('No Metro'))

## Renaming Column ##
df_discount_dcm2 = df_discount_dcm2.withColumnRenamed('Impressions Delivered','Imps')
df_discount_dcm2 = df_discount_dcm2.withColumnRenamed('Media Spend','Cost')
df_discount_dcm2 = df_discount_dcm2.withColumnRenamed('Clicks Delivered','Clicks')

df_discount_dcm2 = df_discount_dcm2.select('Date', 'DmaName', 'SubChannel', 'Campaign', 'Cost', 'Imps', 'Clicks')
# df_discount_dcm2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union Data

# COMMAND ----------

df_dcm = df_dcm.union(df_discount_dcm).union(df_discount_dcm2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DMA Map : DCM

# COMMAND ----------

dma_mapping = {
    "No Metro": 1000,
    "Fairbanks": 745,
    "Bowling Green": 736,
    "Phoenix": 753,
    "West Palm Beach/Fort Pierce": 548,
    "Savannah": 507,
    "Lima": 558,
    "Miami/Fort Lauderdale": 528,
    "Omaha": 652,
    "Anchorage": 743,
    "Lafayette-In": 642,
    "Raleigh/Durham": 560,
    "Fresno/Visalia": 866,
    "Johnstown/Altoona": 574,
    "Peoria/Bloomington": 675,
    "Norfolk/Portsmouth/Newport News": 544,
    "Casper/Riverton": 767,
    "Chattanooga": 575,
    "Harlingen/Weslaco/Brownsville/Mcallen": 636,
    "Tallahassee/Thomasville": 530,
    "Great Falls": 755,
    "Lincoln/Hastings/Kearney": 722,
    "Laredo": 749,
    "Washington Dc": 511,
    "Grand Junction/Montrose": 773,
    "Tucson/Sierra Vista": 789,
    "Zanesville": 596,
    "Bakersfield": 800,
    "San Antonio": 641,
    "Mobile/Pensacola/Fort Walton Beach": 686,
    "Tyler/Longview/Lufkin/Nacogdoches": 709,
    "Jacksonville-Brunswick": 561,
    "Louisville": 529,
    "Philadelphia": 504,
    "Dayton": 542,
    "Seattle/Tacoma": 819,
    "Dallas/Fort Worth": 623,
    "Los Angeles": 803,
    "Paducah/Cape Girardeau/Harrisburg/Mt Vernon": 632,
    "Utica": 526,
    "Springfield-Mo": 619,
    "Columbus/Tupelo/West Point": 673,
    "Presque Isle": 552,
    "Dothan": 606,
    "Indianapolis": 527,
    "Cleveland": 510,
    "Des Moines/Ames": 679,
    "Topeka": 605,
    "Victoria": 626,
    "Nashville": 659,
    "Oklahoma City": 650,
    "Rockford": 610,
    "San Diego": 825,
    "Beaumont/Port Arthur": 692,
    "Florence/Myrtle Beach": 570,
    "Wichita Falls/Lawton": 627,
    "Detroit": 505,
    "Mankato": 737,
    "San Francisco/Oakland/San Jose": 807,
    "Sherman-Tx/Ada-Ok": 657,
    "Richmond/Petersburg": 556,
    "Austin-Tx": 635,
    "Boise": 757,
    "Baton Rouge": 716,
    "Flint/Saginaw/Bay City": 513,
    "Yuma/El Centro": 771,
    "Birmingham": 630,
    "Parkersburg": 597,
    "Abilene/Sweetwater": 662,
    "Evansville": 649,
    "Youngstown": 536,
    "Cedar Rapids/Waterloo/Dubuque": 637,
    "Memphis": 640,
    "Monterey/Salinas": 828,
    "Columbus-Oh": 535,
    "Madison": 669,
    "Rochester-Ny": 611,
    "El Paso": 765,
    "Alexandria-La": 644,
    "Medford/Klamath Falls": 813,
    "Milwaukee": 617,
    "Huntsville/Decatur/Florence": 691,
    "Pittsburgh": 508,
    "Little Rock/Pine Bluff": 693,
    "Clarksburg/Weston": 598,
    "Springfield/Holyoke": 543,
    "Odessa/Midland": 633,
    "Rochester/Mason City/Austin": 611,
    "Yakima/Pasco/Richland/Kennewick": 810,
    "Lansing": 551,
    "Columbia/Jefferson City": 604,
    "Chicago": 602,
    "Orlando/Daytona Beach/Melbourne": 534,
    "Wilkes Barre/Scranton": 577,
    "Lubbock": 651,
    "Davenport/Rock Island/Moline": 682,
    "Champaign/Springfield/Decatur": 648,
    "Greenville/New Bern/Washington": 567,
    "Wheeling/Steubenville": 554,
    "Fargo/Valley City": 724,
    "Joplin/Pittsburg": 603,
    "Toledo": 538,
    "Erie": 516,
    "Corpus Christi": 600,
    "Shreveport": 612,
    "Wilmington": 550,
    "Amarillo": 634,
    "Wausau/Rhinelander": 705,
    "Grand Rapids/Kalamazoo/Battle Creek": 563,
    "No Metro": 1000,
    "Macon": 503,
    "Glendive": 798,
    "Atlanta": 524,
    "Reno": 811,
    "New Orleans": 622,
    "Albany/Schenectady/Troy": 532,
    "Las Vegas": 839,
    "Lexington": 541,
    "Charlottesville": 584,
    "Honolulu": 744,
    "Jackson-Tn": 718,
    "Binghamton": 502,
    "Monroe/El Dorado": 628,
    "San Angelo": 661,
    "Gainesville": 592,
    "Portland/Auburn": 500,
    "Fort Myers/Naples": 571,
    "Palm Springs": 804,
    "Portland-Or": 820,
    "Harrisonburg": 569,
    "Lafayette-La": 582,
    "Lake Charles": 643,
    "Roanoke/Lynchburg": 573,
    "Augusta": 520,
    "Albuquerque/Santa Fe": 790,
    "Duluth/Superior": 676,
    "Kansas City": 616,
    "Eureka": 802,
    "Spokane": 881,
    "Santa Barbara/Santa Maria/San Luis Obispo": 855,
    "Salisbury": 576,
    "Missoula": 762,
    "Saint Louis": 609,
    "Juneau": 747,
    "Marquette": 553,
    "Hattiesburg/Laurel": 710,
    "Billings": 756,
    "Montgomery/Selma": 698,
    "Bangor": 537,
    "Helena": 766,
    "Fort Smith/Fayetteville/Springdale/Rogers": 670,
    "Charleston/Huntington": 564,
    "Columbia-Sc": 746,
    "Sacramento/Stockton/Modesto": 862,
    "Houston": 618,
    "Terre Haute": 581,
    "Greenwood/Greenville": 647,
    "Ottumwa/Kirksville": 631,
    "South Bend/Elkhart": 588,
    "Twin Falls": 760,
    "Elmira": 565,
    "Jonesboro": 734,
    "Chico/Redding": 868,
    "Tulsa": 671,
    "Green Bay/Appleton": 658,
    "Harrisburg/Lancaster/Lebanon/York": 566,
    "Biloxi/Gulfport": 746,
    "Alpena": 583,
    "Buffalo": 514,
    "Burlington/Plattsburgh": 523,
    "Charleston-Sc": 546,
    "Colorado Springs/Pueblo": 752,
    "Knoxville": 557,
    "La Crosse/Eau Claire": 702,
    "Saint Joseph": 638,
    "Bend-Or": 821,
    "Bluefield/Beckley/Oak Hill": 559,
    "Cincinnati": 515,
    "Traverse City/Cadillac": 540,
    "Waco/Temple/Bryan": 625,
    "Providence/New Bedford": 521,
    "Tri-Cities-Tn-Va": 531,
    "Greensboro/High Point/Winston-Salem": 518,
    "Watertown": 549,
    "Panama City": 656,
    "Syracuse": 555,
    "Greenville/Spartanburg/Asheville/Anderson": 567,
    "Minneapolis/Saint Paul": 613,
    "Salt Lake City": 770,
    "Baltimore": 512,
    "Wichita/Hutchinson": 678,
    "Columbus-Ga": 522,
    "Tampa/Saint Petersburg": 539,
    "New York": 501,
    "Charlotte": 517,
    "Cheyenne/Scottsbluff": 759,
    "Quincy/Hannibal/Keokuk": 717,
    "Sioux City": 624,
    "North Platte": 740,
    "Eugene": 801,
    "Jackson-Ms": 639,
    "Meridian": 711,
    "Idaho Falls/Pocatello": 758,
    "Denver": 751,
    "Boston": 506,
    "Rapid City": 764,
    "Albany-Ga": 525,
    "Butte/Bozeman": 754,
    "Fort Wayne": 509,
    "Minot/Bismarck/Dickinson": 687,
    "Sioux Falls/Mitchell": 725,
    "Hartford/New Haven": 533,
}

print(dma_mapping)


# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  

df_dcm= df_dcm.withColumn("DMA_Code", map_dma_name_to_code("DmaName"))
df_dcm = df_dcm.groupBy('Date', 'DMA_Code', 'SubChannel', 'Campaign').agg(f.sum(f.col('Cost')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks'))
# df_dcm.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discount: Google Search

# COMMAND ----------

# Search: Health and Discount #
folder_path = 'dbfs:/blend360/sandbox/mmm/asi_phase2/' 

##  Discount Google Search ## 
file_path = "Discounts/Paid Search 22-23/Paid Search Google/02.24_AARP_Discounts_DMA_Google.xlsx"
df_discount_google_search = read_excel(folder_path+file_path)
df_discount_google_search = df_discount_google_search.filter(f.col('Week').isNotNull())

## Adding Column for Discount_GoogleSearch ##
df_discount_google_search = df_discount_google_search.withColumn('SubChannel',f.lit('Google'))
df_discount_google_search = df_discount_google_search.withColumn('Campaign', when( f.lower(f.col('Campaign Name')).like("%nonbrand%"), 'NonBrandDiscount' ).\
                                                                             when( f.lower(f.col('Campaign Name')).like("%brand%"), 'BrandDiscount' ).\
                                                                             otherwise("OtherDiscount"))

## Renaming the column ##
df_discount_google_search = df_discount_google_search.withColumnRenamed('Impr.', 'Imps')
df_discount_google_search = df_discount_google_search.withColumnRenamed('DMA Region (Matched)', 'DmaName')
df_discount_google_search = df_discount_google_search.withColumnRenamed('Week', 'Date')

df_discount_google_search = df_discount_google_search.select('Date', 'DmaName', 'SubChannel', 'Campaign', 'Cost', 'Imps', 'Clicks')
# df_discount_google_search.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discount: Microsoft Search

# COMMAND ----------

# Search: Health and Discount #
folder_path = 'dbfs:/blend360/sandbox/mmm/asi_phase2/' 

## Discount Microsoft Search ##
file_path = "Discounts/Paid Search 22-23/Paid Search Microsoft/02.24_AARP_Discounts_DMA_Microsoft.xlsx"
df_discount_msoft_search = read_excel(folder_path+file_path)
df_discount_msoft_search = df_discount_msoft_search.withColumnRenamed('Week (Mon - Sun)','Date').filter(f.col('Date').isNotNull())

## Adding Column for Discount_MsoftSearch ##
df_discount_msoft_search= df_discount_msoft_search.withColumn('SubChannel',f.lit('Bing'))
df_discount_msoft_search = df_discount_msoft_search.withColumn('Campaign', when( f.lower(f.col('Campaign')).like("%nonbrand%"), 'NonBrandDiscount' ).\
                                                                             when( f.lower(f.col('Campaign')).like("%brand%"), 'BrandDiscount' ).\
                                                                             otherwise("OtherDiscount"))

## Renaming Column ##
df_discount_msoft_search= df_discount_msoft_search.withColumnRenamed('Impr.', 'Imps')
df_discount_msoft_search= df_discount_msoft_search.withColumnRenamed('Metro area (User location)', 'DmaName')

df_discount_msoft_search = df_discount_msoft_search.select('Date', 'DmaName', 'SubChannel', 'Campaign', 'Cost', 'Imps', 'Clicks')
# df_discount_msoft_search.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Health: Google Search

# COMMAND ----------

# Search: Health and Discount #
folder_path = 'dbfs:/blend360/sandbox/mmm/asi_phase2/' 

## Health Google Search ##
file_path = 'Health/Paid Search 22-23/Google/AARP_Health_Google_DMA.xlsx'
df_health_google_search = read_excel(folder_path+file_path)
df_health_google_search = df_health_google_search.withColumn("Date", f.to_date(f.col('Week  of (Mon - Sun)'), 'yyyy-MM-dd'))

## Adding Column for Health_GoogleSearch ##
df_health_google_search = df_health_google_search.withColumn("SubChannel",f.lit('Google'))
df_health_google_search = df_health_google_search.withColumn('Campaign', when( f.lower(f.col('Campaign_name')).like("%nonbrand%"), 'NonBrandHealth' ).\
                                                                             when( f.lower(f.col('Campaign_name')).like("%brand%"), 'BrandHealth' ).\
                                                                             otherwise("OtherHealth"))

## Renaming Column ##
df_health_google_search = df_health_google_search.withColumnRenamed('Impr.','Imps')
df_health_google_search = df_health_google_search.withColumnRenamed('DMA Region (Matched)','DmaName')

df_health_google_search = df_health_google_search.select('Date', 'DmaName', 'SubChannel', 'Campaign', 'Cost', 'Imps', 'Clicks')
# df_health_google_search.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Finance: Search

# COMMAND ----------

## Health Google Search ##
file_path = 'Finance/2022/Paid Search/02.24_AARP_Finance_DMA.xlsx'
df_fin_search = read_excel(folder_path+file_path)
df_fin_search = df_fin_search.withColumn("Date", f.to_date(f.col('Week'), 'yyyy-MM-dd'))

## Adding Column for Health_GoogleSearch ##
df_fin_search = df_fin_search.withColumn("SubChannel",f.lit('Unknown'))
df_fin_search = df_fin_search.withColumn('Campaign', when( f.lower(f.col('Campaign')).like("%nonbrand%"), 'NonBrandFinance' ).\
                                                                             when( f.lower(f.col('Campaign')).like("%brand%"), 'BrandFinance' ).\
                                                                             otherwise("OtherFinance"))

## Renaming Column ##
df_fin_search = df_fin_search.withColumnRenamed('Impr.','Imps')
df_fin_search = df_fin_search.withColumnRenamed('DMA Region (Matched)','DmaName')

df_fin_search = df_fin_search.select('Date', 'DmaName', 'SubChannel', 'Campaign', 'Cost', 'Imps', 'Clicks')
# df_fin_search.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Union Data 

# COMMAND ----------

## Since below df have same dma name patterns, making one common DF ##
df_search = df_discount_google_search.union(df_discount_msoft_search).union(df_health_google_search).union(df_fin_search)
# df_search.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DMA Map: Search

# COMMAND ----------

dma_mapping = {
    "No Metro": 1000,
    "Odessa-Midland TX": 633,
    "North Platte NE": 740,
    "Flint-Saginaw-Bay City MI": 513,
    "Hattiesburg-Laurel MS": 710,
    "Toledo OH": 538,
    "Austin TX": 635,
    "Portland-Auburn ME": 500,
    "Rochester-Mason City-Austin,IA": 611,
    "Buffalo NY": 514,
    "Chicago IL": 602,
    "Washington DC (Hagerstown MD)": 511,
    "Raleigh-Durham (Fayetteville) NC": 560,
    "Portland OR": 820,
    "Columbus-Tupelo-West Point MS": 673,
    "Harrisonburg VA": 569,
    "Springfield MO": 619,
    "Wichita-Hutchinson KS": 678,
    "Zanesville OH": 596,
    "Jackson MS": 639,
    "Topeka KS": 605,
    "Bowling Green KY": 736,
    "Lexington KY": 541,
    "Boston MA-Manchester NH": 506,
    "La Crosse-Eau Claire WI": 702,
    "Baton Rouge LA": 716,
    "Lafayette LA": 582,
    "Albany GA": 525,
    "Kansas City MO": 616,
    "Montgomery-Selma, AL": 698,
    "Paducah KY-Cape Girardeau MO-Harrisburg-Mount Vernon IL": 632,
    "Quincy IL-Hannibal MO-Keokuk IA": 717,
    "Louisville KY": 529,
    "Santa Barbara-Santa Maria-San Luis Obispo CA": 855,
    "Victoria TX": 626,
    "Charleston SC": 519,
    "Cheyenne WY-Scottsbluff NE": 759,
    "Eugene OR": 801,
    "Macon GA": 503,
    "Anchorage AK": 743,
    "Dayton OH": 542,
    "Providence-New Bedford,MA": 521,
    "Lake Charles LA": 643,
    "Madison WI": 669,
    "Waco-Temple-Bryan TX": 625,
    "Youngstown OH": 536,
    "West Palm Beach-Ft. Pierce FL": 548,
    "Fresno-Visalia CA": 866,
    "Medford-Klamath Falls OR": 813,
    "Tulsa OK": 671,
    "Johnstown-Altoona-State College PA": 574,
    "Roanoke-Lynchburg VA": 573,
    "St. Louis MO": 609,
    "Twin Falls ID": 760,
    "Erie PA": 516,
    "Alexandria LA": 644,
    "Sioux City IA": 624,
    "Traverse City-Cadillac MI": 540,
    "Lima OH": 558,
    "Fairbanks AK": 745,
    "Miami-Ft. Lauderdale FL": 528,
    "Tallahassee FL-Thomasville GA": 530,
    "Richmond-Petersburg VA": 556,
    "New Orleans LA": 622,
    "Savannah GA": 507,
    "Panama City FL": 656,
    "Rochester NY": 611,
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
    "Los Angeles CA": 803,
    "Wilkes Barre-Scranton PA": 577,
    "Bakersfield CA": 800,
    "Little Rock-Pine Bluff AR": 693,
    "Marquette MI": 553,
    "Norfolk-Portsmouth-Newport News VA": 544,
    "Albany-Schenectady-Troy NY": 532,
    "Indianapolis IN": 527,
    "Lansing MI": 551,
    "Palm Springs CA": 804,
    "Sherman-Ada, OK": 657,
    "South Bend-Elkhart IN": 588,
    "Missoula MT": 762,
    "Rockford IL": 610,
    "San Diego CA": 825,
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
    "Chattanooga TN": 575,
    "Wausau-Rhinelander WI": 705,
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
    "Elmira (Corning) NY": 565,
    "Minneapolis-St. Paul MN": 613,
    "New York, NY": 501,
    "Denver CO": 751,
    "Honolulu HI": 744,
    "Ft. Myers-Naples FL": 571,
    "San Angelo TX": 661,
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
    "Lincoln & Hastings-Kearney NE": 722,
    "Yuma AZ-El Centro CA": 771,
    "Baltimore MD": 512,
    "Mankato MN": 737,
    "Florence-Myrtle Beach SC": 570,
    "Hartford & New Haven CT": 533,
    "Greenwood-Greenville MS": 647,
    "Harrisburg-Lancaster-Lebanon-York PA": 566,
    "Boise ID": 757,
    "Oklahoma City OK": 650,
    "Dallas-Ft. Worth TX": 623,
    "Seattle-Tacoma WA": 819,
    "Minot-Bismarck-Dickinson(Williston) ND": 687,
    "Rapid City SD": 764,
    "Utica NY": 526,
    "Colorado Springs-Pueblo CO": 752,
    "Sioux Falls(Mitchell) SD": 725,
    "Grand Junction-Montrose CO": 773,
    "Detroit MI": 505,
    "Syracuse NY": 555,
    "Wilmington NC": 550,
    "Charleston-Huntington WV": 564,
    "Orlando-Daytona Beach-Melbourne FL": 534,
    "Champaign & Springfield-Decatur IL": 648,
    "Duluth MN-Superior WI": 676,
    "Columbus OH": 535,
    "El Paso TX": 765,
    "Alpena MI": 583,
    "Abilene-Sweetwater TX": 662,
    "Tucson (Sierra Vista) AZ": 789,
    "Billings, MT": 756,
    "Jacksonville FL": 561,
    "Cedar Rapids-Waterloo-Iowa City & Dubuque IA": 637,
    "Greenville-New Bern-Washington NC": 567,
    "San Antonio TX": 641,
    "Beaumont-Port Arthur TX": 692,
    "Great Falls MT": 755,
    "Columbus GA": 522,
    "Omaha NE": 652,
    "Mobile AL-Pensacola (Ft. Walton Beach) FL": 686,
    "Bangor ME": 537,
    "Tri-Cities TN-VA": 531,
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
    "Casper-Riverton WY": 767,
    "Presque Isle ME": 552,
    "Bend OR": 821,
    "Jackson TN": 718,
    "Davenport IA-Rock Island-Moline IL": 682,
    "Milwaukee WI": 617,
}


# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  

df_search = df_search.withColumn("DMA_Code", map_dma_name_to_code("DmaName"))
df_search = df_search.groupBy('Date', 'DMA_Code', 'SubChannel', 'Campaign').agg(f.sum(f.col('Cost')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks'))
# df_search.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union Search and DCM 

# COMMAND ----------

final_model_df = df_dcm.union(df_search)
# final_model_df.display()

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

@udf
def get_pop_prop(dma_code):
  return norm_ch_dma_code_prop.get(dma_code)


def new_df_func(col1, col2, col3, col4, col5, col6, col7):

  original_row = Row(Date=col1, DMA_Code=col2, SubChannel=col3, Campaign=col4, Spend=col5, Imps=col6, Clicks=col7)
  new_df = spark.sparkContext.parallelize([original_row]*210).toDF()

  ## National to DMA ##
  labels_udf = f.udf(lambda indx: dma_code[indx-1], StringType()) 
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
# MAGIC # Some More Updates

# COMMAND ----------

## Adding Channel Name ##
cols = ['Date', 'DMA_Code', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df = final_model_df.withColumn('Channel', when(f.col('SubChannel').isin(['Display']), f.col('SubChannel')).otherwise(f.lit('Search'))).select(cols) ## Re-arranging Columns 
# save_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_asi_SearchDcm" 
save_df_func(final_model_df, table_name)

# COMMAND ----------



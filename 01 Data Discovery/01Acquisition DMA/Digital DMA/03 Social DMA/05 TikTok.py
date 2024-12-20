# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

## Reading Campaign Data ##
df = read_table("temp.ja_blend_mmm2_DigitalDMA").filter( (f.col('media_type')=='Social') & (f.col('Traffic_Source')=='TikTok') ).filter(f.year(f.col('week_start')).isin([2022, 2023]))
df.display()

# COMMAND ----------

df.groupBy(f.year(f.col('week_start')).alias('Year')).agg(f.sum(f.col('Spend')).alias('Spend')).display()

# COMMAND ----------

df.groupBy(f.year(f.col('week_start')).alias('Year'), 'campaign').agg(f.sum(f.col('Spend')).alias('Spend')).display()

# COMMAND ----------

## Reading Campaign Data ##
df = read_table("temp.ja_blend_mmm2_DigitalDMA").filter( (f.col('media_type')=='Social') & (f.col('Traffic_Source')=='TikTok') ).filter(f.year(f.col('week_start')).isin([2022, 2023]))

## Rename Columns ##
rename_dict = {'week_start':'Date',  'campaign':'Campaign', 'Impressions':'Imps', 'GEO':'DmaName'}
df = rename_cols_func(df, rename_dict)

## Adding New Columns ##
df = df.withColumn( 'Channel', f.lit('Social'))
df = df.withColumn( 'SubChannel', f.lit('TikTok'))
df = df.withColumn( 'Campaign', f.lit('PlaceHolderForNow'))

## Imputing for NULL column Value ##
df = df.fillna('National', subset=['DmaName'])

## Selecting Cols ##
cols = ['Date', 'DmaName', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
df.select(cols).display()

# COMMAND ----------

## Aggregating Column Value ##
final_model_df = df.groupBy('Date', 'DmaName', 'Channel', 'SubChannel', 'Campaign').agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks') )
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

dma_mapping = {'Fairbanks': 745,
 'Bowling Green': 736,
 'Butte-Bozeman': 754,
 'Champaign-Springfield-Decatur': 648,
 'Sherman-Ada': 657,
 'Savannah': 507,
 'Lima': 558,
 'Omaha': 652,
 'Anchorage': 743,
 'Ottumwa-Kirksville': 631,
 'Montgomery-Selma': 698,
 'Columbus, GA (Opelika, AL)': 522,
 'El Paso (Las Cruces)': 765,
 'Chico-Redding': 868,
 'Little Rock-Pine Bluff': 693,
 'Monroe-El Dorado': 628,
 'National': 1000,
 'Chattanooga': 575,
 'Rochestr-Mason City-Austin': 611,
 'Great Falls': 755,
 'Cedar Rapids-Waterloo-Iowa City-Dubuque': 637,
 'Laredo': 749,
 'Columbus, OH': 535,
 'Zanesville': 596,
 'Traverse City-Cadillac': 540,
 'Bakersfield': 800,
 'San Antonio': 641,
 'Mobile-Pensacola (Navarre)': 686,
 'Philadelphia': 504,
 'Louisville': 529,
 'Dayton': 542,
 'Los Angeles': 803,
 'Greenville-New Bern-Washington': 545,
 'Johnstown-Altoona-State College': 574,
 'Utica': 526,
 'Presque Isle': 552,
 'Washington (Hagerstown)': 511,
 'Dothan': 606,
 'Grand Junction-Montrose': 773,
 'Sacramento-Stockton-Modesto': 862,
 'Indianapolis': 527,
 'Charleston, SC': 519,
 'Topeka': 605,
 'Victoria': 626,
 'San Diego': 825,
 'Nashville': 659,
 'South Bend-Elkhart': 588,
 'Rockford': 610,
 'Oklahoma City': 650,
 'Wichita-Hutchinson': 678,
 'Davenport-Rock Island-Moline': 682,
 'Albany, GA': 525,
 'Mankato': 737,
 'Detroit': 505,
 'Miami-Fort Lauderdale': 528,
 'Bluefield-Beckley-Oak Hill': 559,
 'Wilkes-Barre–Scranton–Hazleton': 577,
 'Columbia-Jefferson City': 604,
 'Springfield-Holyoke': 543,
 'Tri-Cities, TN-VA': 531,
 'Boise': 757,
 'Minneapolis-St. Paul': 613,
 'Baton Rouge': 716,
 'St. Louis': 609,
 'Beaumont-Port Arthur': 692,
 'Clarksburg-Weston': 598,
 'Colorado Springs-Pueblo': 752,
 'Parkersburg': 597,
 'Myrtle Beach-Florence': 570,
 'Jackson, MS': 718,
 'Youngstown': 536,
 'Evansville': 649,
 'Abilene-Sweetwater': 662,
 'Memphis': 640,
 'Austin': 635,
 'Norfolk-Portsmouth-Newport News': 544,
 'Madison': 669,
 'Milwaukee': 617,
 'Casper-Riverton': 767,
 'Pittsburgh': 508,
 'St. Joseph': 638,
 'Richmond-Petersburg': 556,
 'Green Bay-Appleton': 658,
 'Waco-Temple-Bryan': 625,
 'Lincoln-Hastings-Kearney': 722,
 'Lansing': 551,
 'Biloxi-Gulfport': 746,
 'Chicago': 602,
 'Sioux Falls(Mitchell)': 725,
 'Lubbock': 651,
 'Greensboro–High Point–Winston-Salem': 518,
 'Minot-Bismarck-Dickinson (Williston)': 687,
 'Providence-New Bedford': 521,
 'Seattle-Tacoma': 819,
 'Albuquerque-Santa Fe': 790,
 'Wausau-Rhinelander': 705,
 'Toledo': 547,
 'Quincy-Hannibal-Keokuk': 717,
 'Boston (Manchester)': 506,
 'Erie': 516,
 'Albany-Schenectady-Troy': 532,
 'Greenville-Spartanburg-Asheville-Anderson': 567,
 'Corpus Christi': 600,
 'Wilmington': 550,
 'Fort Smith-Fayetteville-Springdale-Rogers': 670,
 'Shreveport': 612,
 'Amarillo': 634,
 'Yuma-El Centro': 771,
 'Des Moines-Ames': 679,
 'Tyler-Longview (Lufkin-Nacogdoches)': 709,
 'Macon': 503,
 'Glendive': 798,
 'Atlanta': 524,
 'Tampa-St. Petersburg (Sarasota)': 539,
 'Reno': 811,
 'New Orleans': 622,
 'Lexington': 541,
 'Las Vegas': 839,
 'Santa Barbara-Santa Maria-San Luis Obispo': 855,
 'Bend, OR': 821,
 'Charlottesville': 584,
 'Honolulu': 744,
 'Cheyenne-Scottsbluff': 759,
 'Binghamton': 502,
 'Peoria-Bloomington': 675,
 'Yakima-Pasco-Richland-Kennewick': 810,
 'San Angelo': 661,
 'Hartford-New Haven': 533,
 'Joplin-Pittsburg': 603,
 'San Francisco-Oakland-San Jose': 807,
 'Roanoke-Lynchburg': 573,
 'Gainesville': 592,
 'Cleveland-Akron (Canton)': 510,
 'Odessa-Midland': 633,
 'Palm Springs': 804,
 'Harlingen-Weslaco-Brownsville-McAllen': 636,
 'Harrisonburg': 569,
 'Lake Charles': 643,
 'Birmingham (Anniston-Tuscaloosa)': 630,
 'Charleston-Huntington': 564,
 'Hattiesburg-Laurel': 710,
 'Paducah-Cape Girardeau-Harrisburg': 632,
 'Harrisburg-Lancaster-Lebanon-York': 566,
 'Kansas City': 616,
 'Eureka': 802,
 'Spokane': 881,
 'Lafayette, LA': 642,
 'Salisbury': 576,
 'Missoula': 762,
 'Burlington-Plattsburgh': 523,
 'Marquette': 553,
 'Juneau': 747,
 'Billings': 756,
 'Dallas-Fort Worth': 623,
 'Helena': 766,
 'Bangor': 537,
 'Greenwood-Greenville': 647,
 'Columbus-Tupelo-West Point-Houston': 673,
 'Columbia, SC': 546,
 'Houston': 618,
 'Terre Haute': 581,
 'Medford-Klamath Falls': 813,
 'La Crosse-Eau Claire': 702,
 'Jacksonville': 561,
 'Portland-Auburn': 500,
 'Twin Falls': 760,
 'Jonesboro': 734,
 'Tulsa': 671,
 'Rochester, NY': 538,
 'Flint-Saginaw-Bay City': 513,
 'Fresno-Visalia': 866,
 'Jackson, TN': 639,
 'Alpena': 583,
 'Knoxville': 557,
 'Grand Rapids-Kalamazoo-Battle Creek': 563,
 'Buffalo': 514,
 'Tucson (Sierra Vista)': 789,
 'Lafayette, IN': 582,
 'Wichita Falls & Lawton': 627,
 'Portland, OR': 820,
 'Cincinnati': 515,
 'Fort Myers-Naples': 571,
 'Raleigh-Durham (Fayetteville)': 560,
 'Alexandria, LA': 644,
 'Watertown': 549,
 'Panama City': 656,
 'Syracuse': 555,
 'Orlando-Daytona Beach-Melbourne': 534,
 'Huntsville-Decatur (Florence)': 691,
 'Salt Lake City': 770,
 'Baltimore': 512,
 'Elmira (Corning)': 565,
 'Duluth-Superior, WI': 676,
 'Fargo-Moorhead-Grand Forks': 724,
 'New York': 501,
 'Idaho Falls-Pocatello (Jackson)': 758,
 'Monterey-Salinas': 828,
 'Sioux City': 624,
 'Charlotte': 517,
 'North Platte': 740,
 'Eugene': 801,
 'Meridian': 711,
 'Augusta-Aiken': 520,
 'West Palm Beach-Fort Pierce': 548,
 'Denver': 751,
 'Rapid City': 764,
 'Tallahassee-Thomasville': 530,
 'Fort Wayne': 509,
 'Phoenix (Prescott)': 753,
 'Wheeling-Steubenville, OH': 554,
 'Springfield, MO': 619}

# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  
dma_col = "DmaName"
cols = ["Date", "DmaCode", "Channel", "SubChannel", "Campaign"]

final_model_df = final_model_df.withColumn("DmaCode", map_dma_name_to_code(dma_col))
final_model_df = final_model_df.groupBy(cols).agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks'))
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

## Seperating National and DMA level information ##
final_model_df_national = final_model_df.filter(f.col('DmaCode')==1000).drop('DmaCode')
final_model_df_dma = final_model_df.filter(f.col('DmaCode')!=1000)




## Creating Key-Value Dataframe ##
data_tuples = [(key,value) for key, value in norm_ch_dma_code_prop.items()]
norm_pop_prop_df = spark.createDataFrame(data_tuples, schema = ['DmaCode', 'Prop'])

## Joining above df with final_model_df ##
final_model_df_national = final_model_df_national.crossJoin(norm_pop_prop_df)

## Dividing Metrics by 210 to distribute data at national level ##
cols = ["Spend", "Imps", "Clicks"]
for col in cols:
  final_model_df_national = final_model_df_national.withColumn(col,f.col(col)*f.col('Prop'))




## Re-arranging COlumns ##
cols = ['Date', 'DmaCode', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
final_model_df_national = final_model_df_national.select(cols)


## Combining back with DMA level Data ##
save_df = final_model_df_national.union(final_model_df_dma)
save_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # QC Block

# COMMAND ----------

# QC_df = unit_test( df, save_df, ['Spend','Spend'], ['Date','Date'])
# QC_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_SocialTikTok" 
save_df_func(save_df, table_name)

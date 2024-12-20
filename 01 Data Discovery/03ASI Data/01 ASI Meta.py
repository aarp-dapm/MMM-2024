# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Getting Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discount 2022

# COMMAND ----------

### Appending all files ##
cols = ['Week', 'Publisher', 'Campaign name', 'Account name', 'Ad name', 'Impressions', 'Amount spent (USD)', 'Link clicks', 'Video plays at 100%', 'DMA region', 'Reporting starts', 'Reporting ends']
excel_file_path = "dbfs:/blend360/sandbox/mmm/asi_phase2/Discounts/2022/Meta/"
df_disc_2022 = read_excel(dbutils.fs.ls(excel_file_path)[0].path).select(cols) 

for file in dbutils.fs.ls(excel_file_path)[1:]:
  temp = read_excel(file.path).select(cols)
  df_disc_2022 = df_disc_2022.union(temp)

## Renaming Files ##
df_disc_2022 = df_disc_2022.withColumn('SubChannel', f.lit('Meta'))
df_disc_2022 = df_disc_2022.withColumn('Campaign', when( f.lower(f.col('Campaign name')).like("%onboarding%"), f.lit("OnboardingDiscount") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%prospecting%"), f.lit("ProspectingDiscount") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%retargeting%"), f.lit("RetargetingDiscount") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%thematic%"), f.lit("ThematicDiscount") ).\
                                                 otherwise('OthersDiscount'))

# df_disc_2022_dma = get_traget_list(df_disc_2022, 'DMA region')
# df_disc_2022.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discount 2023

# COMMAND ----------

## Appending all files ##
cols = ['Week', 'Publisher', 'Campaign name', 'Account name', 'Ad name', 'Impressions', 'Amount spent (USD)', 'Link clicks', 'Video plays at 100%', 'DMA region', 'Reporting starts', 'Reporting ends']
excel_file_path = "dbfs:/blend360/sandbox/mmm/asi_phase2/Discounts/2023/Meta/"
df_disc_2023 = read_excel(dbutils.fs.ls(excel_file_path)[0].path).select(cols) 

for file in dbutils.fs.ls(excel_file_path)[1:]:
  temp = read_excel(file.path).select(cols)
  df_disc_2023 = df_disc_2023.union(temp)

## Renaming Files ##
df_disc_2023 = df_disc_2023.withColumn('SubChannel', f.lit('Meta'))
df_disc_2023 = df_disc_2023.withColumn('Campaign', when( f.lower(f.col('Campaign name')).like("%onboarding%"), f.lit("OnboardingDiscount") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%prospecting%"), f.lit("ProspectingDiscount") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%retargeting%"), f.lit("RetargetingDiscount") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%thematic%"), f.lit("ThematicDiscount") ).\
                                                 otherwise('OthersDiscount'))

# df_disc_2023_dma = get_traget_list(df_disc_2023, 'DMA region')
# df_disc_2023.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Health 2022

# COMMAND ----------

## Appending all files ##
cols = ['Week', 'Publisher', 'Campaign name', 'Account Name', 'Ad name', 'Impressions', 'Amount spent (USD)', 'Link clicks', 'Video plays at 100%', 'DMA region', 'Reporting starts', 'Reporting ends']
excel_file_path = "dbfs:/blend360/sandbox/mmm/asi_phase2/Health/2022/Meta/"
df_health_2022 = read_excel(dbutils.fs.ls(excel_file_path)[0].path).select(cols) 

for file in dbutils.fs.ls(excel_file_path)[1:]:
  temp = read_excel(file.path).select(cols)
  df_health_2022 = df_health_2022.union(temp)

## Renaming Files ##
df_health_2022 = df_health_2022.withColumn('SubChannel', f.lit('Meta'))
df_health_2022 = df_health_2022.withColumn('Campaign', when( f.lower(f.col('Campaign name')).like("%onboarding%"), f.lit("OnboardingHealth") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%prospecting%"), f.lit("ProspectingHealth") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%retargeting%"), f.lit("RetargetingHealth") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%thematic%"), f.lit("ThematicHealth") ).\
                                                 otherwise('OthersHealth'))
# df_health_2022_dma = get_traget_list(df_health_2022, 'DMA region')
# df_health_2022.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Health 2023

# COMMAND ----------

## Appending all files ##
cols = ['Week', 'Publisher', 'Campaign name', 'Account Name', 'Ad name', 'Impressions', 'Amount spent (USD)', 'Link clicks', 'Video plays at 100%', 'DMA region', 'Reporting starts', 'Reporting ends']
excel_file_path = "dbfs:/blend360/sandbox/mmm/asi_phase2/Health/2023/Meta/"
df_health_2023 = read_excel(dbutils.fs.ls(excel_file_path)[0].path).select(cols) 

for file in dbutils.fs.ls(excel_file_path)[0:]:
  temp = read_excel(file.path).select(cols)
  df_health_2023 = df_health_2023.union(temp)

## Renaming Files ##
df_health_2023 = df_health_2023.withColumn('SubChannel',f.lit('Meta'))
df_health_2023 = df_health_2023.withColumn('Campaign', when( f.lower(f.col('Campaign name')).like("%onboarding%"), f.lit("OnboardingHealth") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%prospecting%"), f.lit("ProspectingHealth") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%retargeting%"), f.lit("RetargetingHealth") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%thematic%"), f.lit("ThematicHealth") ).\
                                                 otherwise('OthersHealth'))
# df_health_2023_dma = get_traget_list(df_health_2023, 'DMA region')
# df_health_2023.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finance 2022

# COMMAND ----------

## Appending all files ##
cols = ['Week', 'Publisher', 'Campaign name', 'Account Name', 'Ad name', 'Impressions', 'Amount spent (USD)', 'Link clicks', 'Video plays at 100%', 'DMA region', 'Reporting starts', 'Reporting ends']
excel_file_path = "dbfs:/blend360/sandbox/mmm/asi_phase2/Finance/2022/Meta/"
df_fin_2022 = read_excel(dbutils.fs.ls(excel_file_path)[0].path).select(cols) 

for file in dbutils.fs.ls(excel_file_path)[0:]:
  temp = read_excel(file.path).select(cols)
  df_fin_2022 = df_fin_2022.union(temp)

## Renaming Files ##
df_fin_2022 = df_fin_2022.withColumn('SubChannel',f.lit('Meta'))
df_fin_2022 = df_fin_2022.withColumn('Campaign', when( f.lower(f.col('Campaign name')).like("%onboarding%"), f.lit("OnboardingFinance") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%prospecting%"), f.lit("ProspectingFinance") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%retargeting%"), f.lit("RetargetingFinance") ).\
                                                 when( f.lower(f.col('Campaign name')).like("%thematic%"), f.lit("ThematicFinance") ).\
                                                 otherwise('OthersFinance'))
# df_health_2023_dma = get_traget_list(df_health_2023, 'DMA region')
# df_health_2023.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Union Data

# COMMAND ----------

cols = ['Reporting starts', 'Reporting ends', 'DMA region', 'SubChannel', 'Campaign', 'Amount spent (USD)', 'Impressions', 'Link clicks']

df = reduce(DataFrame.unionAll, [df_health_2023, df_health_2022, df_disc_2023, df_disc_2022, df_fin_2022])
df = df.groupBy('Reporting starts', 'Reporting ends', 'DMA region', 'SubChannel', 'Campaign').agg(f.sum(f.col('Amount spent (USD)')).alias('Cost'), f.sum(f.col('Impressions')).alias('Imps'), f.sum(f.col('Link clicks')).alias('Clicks'))

## Renaming Columns ##
df = df.withColumnRenamed('Reporting starts','ReportingStarts')
df = df.withColumnRenamed('Reporting ends','ReportingEnds')
df = df.withColumnRenamed('DMA region','DmaRegion')

# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converting Data from week level to daily level to relevant week level

# COMMAND ----------

## Creating Cross Join Df ##
new_df = df.select('ReportingStarts', 'ReportingEnds').dropDuplicates()
new_df = new_df.withColumn('start_date', f.col('ReportingStarts').cast('date')).withColumn('end_date', f.col('ReportingEnds').cast('date'))
new_df = new_df.withColumn('Date', f.explode(f.expr('sequence(start_date, end_date, interval 1 day)')))
new_df = new_df.withColumn('Factor', f.datediff(f.col('end_date'),f.col('start_date')) + 1)

## Joining Both DFs ##
df = df.join(new_df, on=['ReportingStarts', 'ReportingEnds'], how='inner')

## Dividing all metric by 7 ##
df = df.withColumn('Cost', f.col('Cost')/f.col('Factor'))
df = df.withColumn('Imps', f.col('Imps')/f.col('Factor'))
df = df.withColumn('Clicks', f.col('Clicks')/f.col('Factor'))

final_model_df = df.groupBy('Date', 'DmaRegion', 'SubChannel', 'Campaign').agg(f.sum(f.col('Cost')).alias('Cost'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks')  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## DMA Map

# COMMAND ----------

dma_mapping = {'Greensboro-H.Point-W.Salem': 518,
 'Chattanooga': 575,
 'Minneapolis-St. Paul': 613,
 'Minot-Bsmrck-Dcknsn(Wlstn)': 687,
 'Las Vegas': 839,
 'Wichita Falls & Lawton': 627,
 'Milwaukee': 617,
 'Dallas-Ft. Worth': 623,
 'Oklahoma City': 650,
 'Wichita-Hutchinson Plus': 678,
 'Baltimore': 512,
 'Kansas City': 616,
 'Augusta-Aiken': 520,
 'Burlington-Plattsburgh': 523,
 'Rapid City': 764,
 'Evansville': 649,
 'Charleston, SC': 519,
 'Ft. Myers-Naples': 571,
 'Terre Haute': 581,
 'Orlando-Daytona Bch-Melbrn': 534,
 'Alexandria, LA': 644,
 'Davenport-R.Island-Moline': 682,
 'Great Falls': 755,
 'Grand Rapids-Kalmzoo-B.Crk': 563,
 'Columbus-Tupelo-W Pnt-Hstn': 673,
 'Nashville': 659,
 'Phoenix (Prescott)': 753,
 'Bluefield-Beckley-Oak Hill': 559,
 'Joplin-Pittsburg': 603,
 'Juneau': 747,
 'Tulsa': 671,
 'Norfolk-Portsmth-Newpt Nws': 544,
 'Harrisburg-Lncstr-Leb-York': 566,
 'Glendive': 798,
 'Parkersburg': 597,
 'Sacramnto-Stkton-Modesto': 862,
 'Lafayette, IN': 582,
 'El Paso (Las Cruces)': 765,
 'Tri-Cities, TN-VA': 531,
 'Butte-Bozeman': 754,
 'Laredo': 749,
 'Seattle-Tacoma': 819,
 'Ottumwa-Kirksville': 631,
 'Sherman-Ada': 657,
 'Lafayette, LA': 642,
 'Columbia-Jefferson City': 604,
 'Macon': 503,
 'Miami-Ft. Lauderdale': 528,
 'Springfield, MO': 619,
 'Detroit': 505,
 'Reno': 811,
 'Tucson (Sierra Vista)': 789,
 'Birmingham (Ann And Tusc)': 630,
 'North Platte': 740,
 'Harrisonburg': 569,
 'Alpena': 583,
 'Dothan': 606,
 'Cleveland-Akron (Canton)': 510,
 'Cheyenne-Scottsbluff': 759,
 'Waco-Temple-Bryan': 625,
 'Elmira (Corning)': 565,
 'Abilene-Sweetwater': 662,
 'Odessa-Midland': 633,
 'Raleigh-Durham (Fayetvlle)': 560,
 'Richmond-Petersburg': 556,
 'Wilkes Barre-Scranton-Hztn': 577,
 'Greenville-N.Bern-Washngtn': 545,
 'Buffalo': 514,
 'Atlanta': 524,
 'Bend, OR': 821,
 'Bakersfield': 800,
 'Beaumont-Port Arthur': 692,
 'Medford-Klamath Falls': 813,
 'Springfield-Holyoke': 543,
 'Rochester-Mason City-Austin': 611,
 'Twin Falls': 760,
 'Hartford & New Haven': 533,
 'Fargo-Valley City': 724,
 'Charlotte': 517,
 'Portland-Auburn': 500,
 'Des Moines-Ames': 679,
 'Presque Isle': 552,
 'Columbia, SC': 546,
 'Utica': 526,
 'Albuquerque-Santa Fe': 790,
 'Unknown': 1000,
 'Toledo': 547,
 'Boston (Manchester)': 506,
 'Johnstown-Altoona-St Colge': 574,
 'Shreveport': 612,
 'Colorado Springs-Pueblo': 752,
 'Hattiesburg-Laurel': 710,
 'Lubbock': 651,
 'Eureka': 802,
 'Knoxville': 557,
 'Chico-Redding': 868,
 'Ft. Smith-Fay-Sprngdl-Rgrs': 670,
 'Duluth-Superior': 676,
 'Idaho Fals-Pocatllo(Jcksn)': 758,
 'Billings': 756,
 'Houston': 618,
 'Montgomery-Selma': 698,
 'Columbus, OH': 535,
 'La Crosse-Eau Claire': 702,
 'Green Bay-Appleton': 658,
 'Topeka': 605,
 'Lansing': 551,
 'Roanoke-Lynchburg': 573,
 'San Diego': 825,
 'Champaign&Sprngfld-Decatur': 648,
 'Albany, GA': 525,
 'Salisbury': 576,
 'Louisville': 529,
 'St. Louis': 609,
 'Spokane': 881,
 'Honolulu': 744,
 'Jackson, TN': 639,
 'Rochester, NY': 538,
 'Syracuse': 555,
 'Wheeling-Steubenville': 554,
 'Cedar Rapids-Wtrlo-Iwc&Dub': 637,
 'Mobile-Pensacola (Ft Walt)': 686,
 'Wausau-Rhinelander': 705,
 'Corpus Christi': 600,
 'Lake Charles': 643,
 'Portland, OR': 820,
 'Dayton': 542,
 'Watertown': 549,
 'Austin': 635,
 'Little Rock-Pine Bluff': 693,
 'Monroe-El Dorado': 628,
 'Helena': 766,
 'Peoria-Bloomington': 675,
 'Fresno-Visalia': 866,
 'Youngstown': 536,
 'Charleston-Huntington': 564,
 'Tyler-Longview(Lfkn&Ncgd)': 709,
 'Los Angeles': 803,
 'Santabarbra-Sanmar-Sanluob': 855,
 'Denver': 751,
 'Jonesboro': 734,
 'New York': 501,
 'Clarksburg-Weston': 598,
 'Bowling Green': 736,
 'San Antonio': 641,
 'Harlingen-Wslco-Brnsvl-Mca': 636,
 'Casper-Riverton': 767,
 'St. Joseph': 638,
 'Providence-New Bedford': 521,
 'Myrtle Beach-Florence': 570,
 'Palm Springs': 804,
 'Panama City': 656,
 'Meridian': 711,
 'Omaha': 652,
 'Greenwood-Greenville': 647,
 'Mankato': 737,
 'Huntsville-Decatur (Flor)': 691,
 'Binghamton': 502,
 'San Angelo': 661,
 'Salt Lake City': 770,
 'Columbus, GA (Opelika, Al)': 522,
 'Zanesville': 596,
 'Biloxi-Gulfport': 746,
 'Charlottesville': 584,
 'Greenvll-Spart-Ashevll-And': 567,
 'Grand Junction-Montrose': 773,
 'Philadelphia': 504,
 'Savannah': 507,
 'Traverse City-Cadillac': 540,
 'Fairbanks': 745,
 'Tampa-St. Pete (Sarasota)': 539,
 'Monterey-Salinas': 828,
 'Missoula': 762,
 'San Francisco-Oak-San Jose': 807,
 'Paducah-Cape Girard-Harsbg': 632,
 'West Palm Beach-Ft. Pierce': 548,
 'Indianapolis': 527,
 'Pittsburgh': 508,
 'Jackson, MS': 718,
 'Albany-Schenectady-Troy': 532,
 'Lima': 558,
 'Lincoln & Hastings-Krny': 722,
 'Bangor': 537,
 'Memphis': 640,
 'Yakima-Pasco-Rchlnd-Knnwck': 810,
 'Cincinnati': 515,
 'Chicago': 602,
 'Jacksonville': 561,
 'Yuma-El Centro': 771,
 'Flint-Saginaw-Bay City': 513,
 'Lexington': 541,
 'Tallahassee-Thomasville': 530,
 'South Bend-Elkhart': 588,
 'Marquette': 553,
 'Quincy-Hannibal-Keokuk': 717,
 'Madison': 669,
 'Sioux Falls(Mitchell)': 725,
 'Sioux City': 624,
 'Rockford': 610,
 'Boise': 757,
 'Wilmington': 550,
 'Ft. Wayne': 509,
 'Erie': 516,
 'New Orleans': 622,
 'Baton Rouge': 716,
 'Anchorage': 743,
 'Eugene': 801,
 'Gainesville': 592,
 'Victoria': 626,
 'Washington, DC (Hagrstwn)': 511,
 'Amarillo': 634}

# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  

final_model_df = final_model_df.withColumn("DMA_Code", map_dma_name_to_code("DmaRegion"))
final_model_df = final_model_df.groupBy('Date', 'DMA_Code', 'SubChannel', 'Campaign').agg(f.sum(f.col('Cost')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks'))
# final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving Updates 

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_ASI_Meta_progress" 
# save_df_func(final_model_df, table_name, False)
final_model_df = read_table(table_name)

# COMMAND ----------

## Aggregating Data to Week ## 
final_model_df = final_model_df.withColumn('Date', f.date_format( f.date_sub(f.col('Date'), f.dayofweek(f.col('Date'))-2 ) ,'yyyy-MM-dd') )
final_model_df = final_model_df.groupBy('Date', 'DMA_Code', 'SubChannel', 'Campaign').agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks'))

final_model_df = final_model_df.filter(f.year(f.col('Date')).isin([2022, 2023]))
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distributing National Data to DMA level

# COMMAND ----------

## Creating Key-Value Dataframe ##
data_tuples = [(key,value) for key, value in norm_ch_dma_code_prop.items()]
norm_pop_prop_df = spark.createDataFrame(data_tuples, schema = ['DMA_Code', 'Prop'])


## Seperating National Level DF from DMA Level DF ##
final_model_df_dma = final_model_df.filter(f.col('DMA_Code')!=1000)
final_model_df_national = final_model_df.filter(f.col('DMA_Code')==1000)

## Joining above national level df with dma prop df ##
final_model_df_national = final_model_df_national.drop('DMA_Code').crossJoin(norm_pop_prop_df)

## Updating Columns Value ##
cols = ['Spend', 'Imps', 'Clicks']
for col in cols:
  final_model_df_national = final_model_df_national.withColumn(col, f.col(col)*f.col('Prop'))
final_model_df_national = final_model_df_national.select(final_model_df_dma.columns)

## Union Both DF ##
save_df = final_model_df_dma.union(final_model_df_national.select(final_model_df_dma.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC # Some More Updates

# COMMAND ----------

## Adding Channel Name ##
cols = ['Date', 'DMA_Code', 'Channel', 'SubChannel', 'Campaign', 'Spend', 'Imps', 'Clicks']
save_df = save_df.withColumn('Channel', f.lit('Social')).select(cols) ## Re-arranging Columns 
# save_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # QC Block

# COMMAND ----------

# temp = unit_test(final_model_df, save_df, ['Spend', 'Spend'], ['Date', 'Date'])
# temp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
# table_name = "temp.ja_blend_mmm2_ASI_Meta" 
table_name = "temp.ja_blend_mmm2_ASI_MetaVo2" 
save_df_func(save_df, table_name)

# COMMAND ----------



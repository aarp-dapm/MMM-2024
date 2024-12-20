# Databricks notebook source
# MAGIC %md
# MAGIC # Utilities Function

# COMMAND ----------

# MAGIC %run "Users/arajput@aarp.org/Unified Measurement/01MMM/Utilities/utilities_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Local Files

# COMMAND ----------

## Filter Out GEO LOcations ##
locations = [
    "London",
    "Frontenac",
    "Wellington",
    "Indre-et-Loire",
    "Middlesex",
    "Stormont (Dundas & Glengarry)",
    "Stade",
    "Nuremberg",
    "Merseyside",
    "Squamish-Lillooet",
    "Greater Manchester",
    "Montreal",
    "Saint John",
    "Carleton",
    "Le Haut-Richelieu Regional County Municipality",
    "Pisa",
    "Division 16",
    "Genoa",
    "Simcoe",
    "Milan",
    "Windsor and Maidenhead",
    "Hauts-de-Seine",
    "Bergen",
    "West Sussex",
    "Cariboo",
    "Madawaska",
    "Waterloo",
    "Karlsruhe",
    "Glasgow City",
    "Gatineau",
    "Dufferin",
    "Quebec",
    "Jönköping",
    "Nagpur",
    "Durham",
    "Eindhoven",
    "Hamilton",
    "Sherbrooke",
    "Peel",
    "Westmorland",
    "Fraser Valley",
    "Halton",
    "Brant",
    "Haldimand-Norfolk",
    "Greater Vancouver",
    "Capital",
    "Toronto",
    "Niagara",
    "York",
    "Halifax",
    "Ottawa",
    "Sunshine Coast",
    "Central Okanagan",
    "Thompson-Nicola",
    "Buckinghamshire",
    "Essex"
]
len(locations)

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocessing Data Source

# COMMAND ----------

## Reading Campaign Data ##
df = read_table("temp.ja_blend_mmm2_DigitalDMA").filter((f.col('media_type')=='Search Brand Bing')).filter(f.year(f.col('week_start')).isin([2022, 2023]))


## Filter out GEO locations ##
df = df.filter(~f.col('GEO').isin(locations))

## Renaming Channels ##
rename_dict = {'week_start':'Date', 'GEO':'DmaName', 'campaign':'Campaign', 'Impressions':'Imps'}
df = rename_cols_func(df, rename_dict)

## Adding New Columns ##
df = df.withColumn( 'Channel', f.lit('Search'))
df = df.withColumn( 'SubChannel', f.lit('Bing'))



## Agg. Metrics ##
final_model_df = df.groupBy('Date', 'DmaName', 'Channel', 'SubChannel', 'Campaign').agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DMA Map

# COMMAND ----------

dma_mapping = {'Jonesboro, AR': 734,
 'Gainesville, FL': 592,
 'Ottumwa, IA-Kirksville, MO': 631,
 'Flint-Saginaw-Bay City, MI': 513,
 'Palm Springs, CA': 804,
 'WILKES BARRE-SCRANTON-HAZLETON, PA': 577,
 'Paducah, KY-Cape Girardeau, MO-Harrisburg, IL': 632,
 'Charleston-Huntington, WV': 564,
 'Eureka, CA': 802,
 'Ft. Smith-Fayetteville-Springdale-Rogers, AR': 670,
 'Wichita Falls, TX & Lawton, OK': 627,
 'Minneapolis-St. Paul, MN': 613,
 'Corpus Christi, TX': 600,
 'Greenville-New Bern-Washington, NC': 545,
 'El Paso, TX (Las Cruces, NM)': 765,
 'Harrisonburg, VA': 569,
 'Harrisburg-Lancaster-Lebanon-York, PA': 566,
 'Sacramento-Stockton-Modesto, CA': 862,
 'Greenwood-Greenville, MS': 647,
 'Reno, NV': 811,
 'Columbus, OH': 535,
 'Honolulu, HI': 744,
 'Ft. Wayne, IN': 509,
 'Bakersfield, CA': 800,
 'Montgomery-Selma, AL': 698,
 'Lima, OH': 558,
 'Laredo, TX': 749,
 'Baton Rouge, LA': 716,
 'San Francisco-Oakland-San Jose, CA': 807,
 'Topeka, KS': 605,
 'Dayton, OH': 542,
 'Fresno-Visalia, CA': 866,
 'Elmira (Corning), NY': 565,
 'Wausau-Rhinelander, WI': 705,
 'Clarksburg-Weston, WV': 598,
 'Louisville, KY': 529,
 'Kansas City, MO': 616,
 'Missoula, MT': 762,
 'Cincinnati, OH': 515,
 'Biloxi-Gulfport, MS': 746,
 'Las Vegas, NV': 839,
 'Colorado Springs-Pueblo, CO': 752,
 'Charleston, SC': 519,
 'Columbus-Tupelo-West Point-Houston, MS': 673,
 'Atlanta, GA': 524,
 'Charlottesville, VA': 584,
 'Lexington, KY': 541,
 'Juneau, AK': 747,
 'Presque Isle, ME': 552,
 'La Crosse-Eau Claire, WI': 702,
 'Green Bay-Appleton, WI': 658,
 'Phoenix (Prescott), AZ': 753,
 'Albany, GA': 525,
 'San Angelo, TX': 661,
 'Beaumont-Port Arthur, TX': 692,
 'Anchorage, AK': 743,
 'Tri-Cities, TN-VA': 531,
 'Columbia-Jefferson City, MO': 604,
 'FARGO': 724,
 'Tulsa, OK': 671,
 'Monroe, LA-El Dorado, AR': 628,
 'Milwaukee, WI': 617,
 'Greensboro-High Point-Winston Salem, NC': 518,
 'Medford-Klamath Falls, OR': 813,
 'Yuma, AZ-El Centro, CA': 771,
 'Houston, TX': 618,
 'Hartford & New Haven, CT': 533,
 'Helena, MT': 766,
 'Huntsville-Decatur (Florence), AL': 691,
 'Jackson, MS': 718,
 'Traverse City-Cadillac, MI': 540,
 'Raleigh-Durham (Fayetteville), NC': 560,
 'Twin Falls, ID': 760,
 'Burlington, VT-Plattsburgh, NY': 523,
 'Cedar Rapids-Waterloo-Iowa City & Dubuque, IA': 637,
 'Alpena, MI': 583,
 'Miami-Ft. Lauderdale, FL': 528,
 'Harlingen-Weslaco-Brownsville-McAllen, TX': 636,
 'Waco-Temple-Bryan, TX': 625,
 'Great Falls, MT': 755,
 'Champaign & Springfield-Decatur, IL': 648,
 'Watertown, NY': 549,
 'Nashville, TN': 659,
 'Butte-Bozeman, MT': 754,
 'Minot-Bismarck-Dickinson(Williston), ND': 687,
 'Bluefield-Beckley-Oak Hill, WV': 559,
 'Los Angeles, CA': 803,
 'Washington, DC (Hagerstown, MD)': 511,
 'Lubbock, TX': 651,
 'Glendive, MT': 798,
 'Grand Junction-Montrose, CO': 773,
 'Marquette, MI': 553,
 'Greenville-Spartanburg, SC-Asheville, NC-Anderson, SC': 567,
 'Knoxville, TN': 557,
 'Bangor, ME': 537,
 'Spokane, WA': 881,
 'Syracuse, NY': 555,
 'Little Rock-Pine Bluff, AR': 693,
 'Wheeling, WV-Steubenville, OH': 554,
 'Rochester, MN-Mason City, IA-Austin, MN': 611,
 'Evansville, IN': 649,
 'Lincoln & Hastings-Kearney, NE': 722,
 'Joplin, MO-Pittsburg, KS': 603,
 'San Diego, CA': 825,
 'Dothan, AL': 606,
 'Chicago, IL': 602,
 'Detroit, MI': 505,
 'Grand Rapids-Kalamazoo-Battle Creek, MI': 563,
 'Providence, RI-New Bedford, MA': 521,
 'Seattle-Tacoma, WA': 819,
 'Toledo, OH': 547,
 'Birmingham (Anniston and Tuscaloosa), AL': 630,
 'Charlotte, NC': 517,
 'Roanoke-Lynchburg, VA': 573,
 'Cleveland-Akron (Canton), OH': 510,
 'Sherman, TX-Ada, OK': 657,
 'Peoria-Bloomington, IL': 675,
 'Wichita-Hutchinson, KS Plus': 678,
 'St. Louis, MO': 609,
 'North Platte, NE': 740,
 'Albuquerque-Santa Fe, NM': 790,
 'St. Joseph, MO': 638,
 'Rockford, IL': 610,
 'Chico-Redding, CA': 868,
 'Amarillo, TX': 634,
 'Bend, OR': 821,
 'JOHNSTOWN-ALTOONA-STATE COLLEGE, PA': 574,
 'Myrtle Beach-Florence, SC': 570,
 'Tyler-Longview(Lufkin & Nacogdoches), TX': 709,
 'Abilene-Sweetwater, TX': 662,
 'Parkersburg, WV': 597,
 'Pittsburgh, PA': 508,
 'Cheyenne, WY-Scottsbluff, NE': 759,
 'Norfolk-Portsmouth-Newport News, VA': 544,
 'Memphis, TN': 640,
 'Bowling Green, KY': 736,
 'Baltimore, MD': 512,
 'New York, NY': 501,
 'Binghamton, NY': 502,
 'Santa Barbara-Santa Maria-San Luis Obispo, CA': 855,
 'Oklahoma City, OK': 650,
 'Monterey-Salinas, CA': 828,
 'South Bend-Elkhart, IN': 588,
 'Omaha, NE': 652,
 'Meridian, MS': 711,
 'Des Moines-Ames, IA': 679,
 'Orlando-Daytona Beach-Melbourne, FL': 534,
 'Lansing, MI': 551,
 'Albany-Schenectady-Troy, NY': 532,
 'Boston, MA (Manchester, NH)': 506,
 'Chattanooga, TN': 575,
 'Eugene, OR': 801,
 'Springfield-Holyoke, MA': 543,
 'Sioux Falls(Mitchell), SD': 725,
 'Salisbury, MD': 576,
 'Jacksonville, FL': 561,
 'Davenport, IA-Rock Island-Moline, IL': 682,
 'Savannah, GA': 507,
 'Mankato, MN': 737,
 'Fairbanks, AK': 745,
 'Ft. Myers-Naples, FL': 571,
 'Buffalo, NY': 514,
 'Tampa-St. Petersburg (Sarasota), FL': 539,
 'Lafayette, LA': 642,
 'Mobile, AL-Pensacola (Ft. Walton Beach), FL': 686,
 'IDAHO FALLS-POCATELLO, ID (JACKSON, WY)': 758,
 'Lake Charles, LA': 643,
 'Indianapolis, IN': 527,
 'Portland-Auburn, ME': 500,
 'Wilmington, NC': 550,
 'Utica, NY': 526,
 'Richmond-Petersburg, VA': 556,
 'Victoria, TX': 626,
 'Madison, WI': 669,
 'Duluth, MN-Superior, WI': 676,
 'Columbia, SC': 546,
 'Hattiesburg-Laurel, MS': 710,
 'COLUMBUS, GA (OPELIKA, AL)': 522,
 'West Palm Beach-Ft. Pierce, FL': 548,
 'Tucson (Sierra Vista), AZ': 789,
 'Erie, PA': 516,
 'Macon, GA': 503,
 'Rochester, NY': 538,
 'Terre Haute, IN': 581,
 'Youngstown, OH': 536,
 'Jackson, TN': 639,
 'Dallas-Ft. Worth, TX': 623,
 'Shreveport, LA': 612,
 'Lafayette, IN': 582,
 'Portland, OR': 820,
 'Austin, TX': 635,
 'Billings, MT': 756,
 'San Antonio, TX': 641,
 'Casper-Riverton, WY': 767,
 'Tallahassee, FL-Thomasville, GA': 530,
 'Alexandria, LA': 644,
 'Rapid City, SD': 764,
 'Quincy, IL-Hannibal, MO-Keokuk, IA': 717,
 'New Orleans, LA': 622,
 'Sioux City, IA': 624,
 'Salt Lake City, UT': 770,
 'Panama City, FL': 656,
 'Zanesville, OH': 596,
 'Boise, ID': 757,
 'Denver, CO': 751,
 'Philadelphia, PA': 504,
 'Odessa-Midland, TX': 633,
 'Augusta, GA-Aiken, SC': 520,
 'Yakima-Pasco-Richland-Kennewick, WA': 810,
 'Springfield, MO': 619}

# COMMAND ----------

# Define a User Defined Function (UDF) to map DMA names to codes
@udf
def map_dma_name_to_code(name):
    return dma_mapping.get(name)
  
dma_col = "DmaName"
cols = ["Date", "DMA_Code", 'Channel', "SubChannel", "Campaign"]

final_model_df = final_model_df.withColumn("DMA_Code", map_dma_name_to_code(dma_col))
final_model_df = final_model_df.groupBy(cols).agg(f.sum(f.col('Spend')).alias('Spend'), f.sum(f.col('Imps')).alias('Imps'), f.sum(f.col('Clicks')).alias('Clicks'))
final_model_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # QC Block

# COMMAND ----------

# QC_df = unit_test( df, final_model_df, ['Spend','Spend'], ['Date','Date'])
# QC_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Saving File

# COMMAND ----------

## Saving Dataframe ##
table_name = "temp.ja_blend_mmm2_SearchBing" 
save_df_func(final_model_df, table_name)

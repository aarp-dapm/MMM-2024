# MMM-2024

## 01 Data Discovery
This folder processes the raw data obtained either from S3 or .CSV files into standard format. Processing of data includes: 

1) Aggreagtion of Data \
2) Imputation of missing data \ 
3) Standardizing all metrics of data \


Folder structure is as follows:

- Buying Group Level Data (like Membership, Brand)
  - Media Level Data (like Social, Search)
    - Platform Level Data (like Facebook, Google, Bing)
      - 00 Media Data
      - 01 Platform1 Data
      - 02 Platform2 Data
      - 03 Platform3 Data
      - NOTE: Files 01 Platform1 Data, 02 Platform2 Data .... 0N PlatformN Data recieve data from each data source and process them. All these processed files are merged in 00 Media Data file for one single media.
     
## 02 Model Development
This folder has two major sub-folders:
1) 01 Data Processing: Concatenates all data sources. Aggregates data at required level and pivot data in the form to be ingested by modeling library.
   
                       Following Scripts are present in the folder:
   
                       1) 01 Data Processing MediaData.py: Joins all Media Data\
                       2) 02 Data Processing TargetVar.py: Joins all Target Variables (Joins, registrations, Ben Ref Clicks), Promotions, Seasonality, Brand metrics.\
                       3) 101 DataCreation WeekAgeDma ChannelLevelModel.py & 101 DataCreation WeekDma ChannelLevelModel.py: This script and below converts data to be ingested by modelling script. Here data is created at different granularity for different cases. Point to be noted there is not just one data source created in these modeling scripts, multiple data sources are created which will be ingested by different sub-modeliing solutions. Check back on these two scripts to know how data source is created,a lthough there are some "00 Data Creation" folders as well in some sub-modelling scenario.\
   
3) 02 Model Dev: Here we are developing the models and submodels for Joins.
                 Following Scripts are present in the folder:
                 1) 01 Initial Model: This folder has script for channel level modelling. It has script for both main model and sub model. Look for script "--------main.py" for main model.
                 2) 02 Age Group Randomization: It has script for each age group model. Look for script "--------main.py" for main model.
                 3) 03 Sub Model: In this folder, channel level output from "01 Initial Model" are broken into sub-channel level contirbutions.Look for script "--------main.py" for main model.
                 4) 04 Sub Model (Campaign): In this folder, sub channel level output are broken into campaign level contributons. Look for script "--------main.py" for main model.

   NOTE: In each folder, main modelling is done in "-------------Main.py" and contribution calculation is done in "----------Evaluation.py"


## 03 Optimization
This folder created media response curve for each media (not at platform level) and, in a seperate script, optimizes media spend for maximum joins while fixing total spend or minimizes spend for fized joins. 



## 04 Brand Model
Scripts for Brand models


## 05 DMA Analysis
Scripts for DMA analysis 


## 06 DM&P Analysis
Scripts for DM&P analysis. Investigating how DM&P media is driving registrations 


## 07 Benefit Referral analysis
Scripts for Referral analysis. Investigating how ASI media is driving Benefit Referral clicks

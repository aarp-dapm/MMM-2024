# MMM-2024

## 01 Data Discovery
This folder processes the raw data obtained either from S3 or .CSV files into standard format. Processing of data includes: 

1) Aggreagtion of Data 
2) Imputation of missing data  
3) Standardizing all metrics of data 


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
   
                 1) 01 Initial Model: This folder has script for channel level modelling. It has script for both main model and sub model. Look for script "--------main.py" for main model.\
                 2) 02 Age Group Randomization: It has script for each age group model. Look for script "--------main.py" for main model.\
                 3) 03 Sub Model: In this folder, channel level output from "01 Initial Model" are broken into sub-channel level contirbutions.Look for script "--------main.py" for main model.\
                 4) 04 Sub Model (Campaign): In this folder, sub channel level output are broken into campaign level contributons. Look for script "--------main.py" for main model.\

   NOTE: In each folder, main modelling is done in "-------------Main.py" and contribution calculation is done in "----------Evaluation.py"


## 03 Optimization
This folder created media response curve for each media (not at platform level) and, in a seperate script, optimizes media spend for maximum joins while fixing total spend or minimizes spend for fixed joins. 

                Following Scripts/Folder are present in the folder:

                1) Folders like 01.02 Membership Curves, 01.03 ICM Curves, 01.04 Brand Curves, 01.05 ASI Curves, 01.06 RemainingEM curves: These folders contain calculations of curves parameters for each channel - Buying group level curve.
                2) 01.01 Optimization curves: Creating Input: Preprocess data to be consumed by above folder and remaining scripts.
                3) 01.02.01 Optimization Curves: Budget Allocation: Script to maximize joins for fixed total spend by reallocating budget to channels
                4) 01.02.02 Optimization Curves: Budget Allocation (Fixed Joins Minimize Budget): Script to minimize total spend for fixed spend by reallocating budget to channels



## 04 Brand Model
This folder contains scripts for brand model.

                Following Scripts/Folder are present in the folder:

                1) 01 Brand Model: Base Joins on Media: Examines the effect of Brand Media on Base joins obtained from MMMM
                2) 02 Brand Model: Base Joins and Purcahse Intent Vo1 (and Vo2): Examines the effect of Purchase Intent KPI on Base Joins 

## 05 DMA Analysis
This folder contains script for DMA Analysis.

                Following Scripts/Folder are present in the folder:
                1) Scripts from 01.01 to 01.08 randomize coefficient for each channel and calculate coeff. per DMA for that channel. Calculation is done for total of 8 channels.
                2) 01.20 Compile Reports: Compiles coeff. for all DMAs and all channel in one single report 
## 06 DM&P Analysis
This folder contains Data creation and Model development scripts for Registration analysis.

                Following Scripts/Folder are present in the folder:
                1) 01 Data Processing: Creates variable transformation for variables at buying group level. Adds other releavnt data needed for analysis.
                2) 02 Initial Model: 
                        1) 01.01 AARP Reg MMM - Main: Adds additional data source of Priroty Engagment, and models DM&P media (and other media) on Registration variable (and PE Variable)
                        2) 01.02.01 AARP Reg MMM - Model Evaluation - Reg (and 01.02.02 AARP Reg MMM - Model Evaluation - PE): Calculates model contribution for different models and for different target KPIs 


## 07 Benefit Referral analysis
Scripts for Referral analysis. Investigating how ASI media is driving Benefit Referral clicks


                Following Scripts/Folder are present in the folder:
                1) 01 Data Processing: Creates variable transformation for variables at buying group level. Adds other relevant data needed for analysis.
                2) 02 Initial Model: 
                        1) 01.01 AARP Reg MMM - Main: Models how ASI media is driving Benefit referral Clicks.
                        2) 01.02 AARP Reg MMM - Model Evaluation: Calculates model contribution for different Benefit Referral Clicks Models. 


                NOTE: Notebooks name in Benefit Referral Analysis are same to the one in DM&P analysis but analysis are different in each folder.

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
1) 01 Data Processing: Concatenates all data sources. Aggregates data at required level and pivot data in the form to be ingested by modeling libraray
2) 02 Model Dev: Here we are developing the models and submodels for Joins


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

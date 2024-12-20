# MMM-2024

## 01 Data Discovery
This folder processes the raw data obtained either from S3 or .CSV files into standard format. Processing of data includes: 

1) Aggreagtion of Data \
2) Imputation of missing data \ 
3) Standardizing all metrics of data \


Folder structure is as follows:

Buying Group Level Data (like Membership, Brand):\
               |\
               | -----> Media Level Data (like Social, Search) :\
                                   |\
                                   | -----> Platform Level Data (like Facebook, Google, Bing):\
                                                   |\
                                                   | ----->  00 Media Data\
                                                             01 Platform1 Data\
                                                             02 Platform2 Data\
                                                             03 Platform3 Data
                                                   

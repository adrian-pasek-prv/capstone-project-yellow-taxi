# New York's Yellow Taxi Trip Records Data Engineering Project

## Project Summary
The project is made for the purpose of completing Udacity Data Engineering Nanodegree program. The main idea was to create an ELT pipeline that will provide an analytic database for popular New York's yellow taxi trips. The source of data comes from nyc.gov website and is stored as monthly parquet files. Depending on the month, each parquet file is about 100MB and contains nearly 10M records. The date range of the dataset spans from January 2019 to December 2022. The dataset is made in normalized form where most of the dimensions are indicated by numerical ids - a data dictionary in the form of CSV files is provided and it will serve as a foundation for dimension tables. More information about the dataset can be found [here](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and also in [explore.ipynb](https://github.com/adrian-pasek-prv/capstone-project-yellow-taxi/blob/main/explore.ipynb) Jupyter notebook file.

## Data model

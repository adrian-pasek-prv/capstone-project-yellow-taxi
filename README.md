# New York's Yellow Taxi Trip Records Data Engineering Project

## Project Summary
The project is made for the purpose of completing Udacity Data Engineering Nanodegree program. The main idea was to create an ELT pipeline that will provide an analytic database for popular New York's yellow taxi trips. The source of data comes from nyc.gov website and is stored as monthly parquet files. Depending on the month, each parquet file is about 100MB and contains nearly 10M records. The date range of the dataset spans from January 2019 to December 2022. The dataset is made in normalized form where most of the dimensions are indicated by numerical ids - a data dictionary in the form of CSV files is provided and it will serve as a foundation for dimension tables. More information about the dataset can be found [here](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and also in [explore.ipynb](https://github.com/adrian-pasek-prv/capstone-project-yellow-taxi/blob/main/explore.ipynb) Jupyter notebook file.

## Data Model
This project uses the data modeling concept of [star schema](https://www.databricks.com/glossary/star-schema) where a fact table that represents records of yellow taxi trips is surrounded by dimension tables that contain descriptive information. Dimension tables where constructed in accordance with [data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) that was provided by nyc.gov

**mart_trips_hourly** table is the final product of this project and it serves as a single source of truth. In this data mart, decriptive information was decoded by joining dimension tables and data was aggregated to the hourly level to provide a complete picture.

### Data Model Entity Relationship Diagram
![image](https://github.com/adrian-pasek-prv/capstone-project-yellow-taxi/assets/99073144/248e3413-dc2e-497f-84a9-238da159ac9a)


## Choice of Technologies
* Amazon S3 - used as a storage for provided parquet files. Storing data source in S3 is not only safe due such features like bucket versioning and replication of object, it's also easily accessed by other AWS services
* Amazon Redshift - analytical data warehouse that will ingest the data sources from S3 and do the heavy-lifting of processing and transforming the data. Amazon Redshift is the perfect choice when working with huge volume of data as it enables to scale processing power to suit our needs.
* Apache Airflow - orchestration tool that will help us to define and execute a workflow for ELT processes. It provides many plug-and-play operators that can be used with AWS services. It also includes scheduling and monitoring features that will enable us to ensure satisfactory data delivery to our stakeholders

# Data Pipeline
Data pipeline for this project is defined and maintained in Apache Airflow. It includes the main DAG that orchestrates a set of tasks that perform ELT actions on a monthly basis:
* create_tables - group of tasks that create tables used in the downstream actions
* stage_to_redshift - group of tasks that stage source data stored on S3 to Redshift cluster using a custom StageToRedshiftOperator that performs COPY operations
* load_dim_tables - group of tasks that populate dimension tables
* load_fact_trips - inserts trip records into a fact table
* load_mart_tables - joins fact and dimension tables to provide descriptive information and aggregates the data to hourly level
* run_data_quality_checks - 

  # Covid-19 Delta Lake Project - Udacity README

## Purpose
The purpose of this project is to prototype [Delta Lake](https://delta.io/), as a means of developing a lightweight, next-generation data lake for publically available Covid-19 data. 

As the Covid-19 situation continues to progress throughout the world, creating a centralized data repository for timely analysis can be crucial to detecting new outbreaks or at-risk areas. This projects aims to accomplish that by aggregating several datasources into a single, easily accessible, lightweight format. 

The current data sources for this data lake include:

- [Rearc Coronavirus (COVID-19) Cases by US County](https://aws.amazon.com/marketplace/pp/Coronavirus-COVID-19-Data-in-the-United-States-The/prodview-jmb464qw2yg74#overview) dataset, made available in the NIH public Covid datalake
- [Rearc Coronavirus (COVID-19) Testing by US State](https://aws.amazon.com/marketplace/pp/Coronavirus-COVID-19-Data-in-the-United-States-The/prodview-jmb464qw2yg74#overview) dataset, also made available in NIH public Covid datalake
- [NIH County Population 2018 Static Dataset](https://dj2taa9i652rf.cloudfront.net/)

These sources are current as of 9/21/2020, but will expand as time goes on.

## Tools and Technologies
For this project, Delta Lake 0.7.0, Spark 3.0, AWS s3, and Amazon Redshift were selected in order handle that large and diverse dataset. Rationale for the tool selection can be found below:
- [Delta Lake](https://delta.io/) is an open source, next-generation data lake tool that brings ACID transactions to Apache Spark. It was selected due to its ability to handle a wide array of incoming data types, and its efficient handling of meta-data, data compression to parquet, as well as ability to handle schema evolution. These 3 factors will be critical as the dataset grows in both volume and variety of data, as schemas for the respective Delta Tables will almost definitely change over time.
- [Spark 3.0](https://spark.apache.org/docs/latest/) was implemented as a requirement of Delta Lake, but also because of its ability to wrangle, transform, and process a wide variety of data sources in a big data context
- [AWS s3](https://aws.amazon.com/s3/) was used as a storage layer due to its interoperability with Spark, as well as its easy to access interface. Additionally, configuring read/write permissions to the Delta Lake is made easier using AWS access controls for s3
- [Amazon RedShift](https://aws.amazon.com/redshift/) is an optional  component to this project, that allows users to quickly stand up a data warehouse to quickly query and analyze the dataset on an ad-hoc basis. Its easy and cost-effective implementation and interoperability with s3 make it a good choice for effective querying or analysis of Delta Lake data 

## Data Model
Data model was designed for quick read/writes, and allows the users to enrich the <code>covid_cases</code> and <code>covid_tests</code> fact tables, using the <code>population</code> and <code>time</code> dim tables. 

![schema](images/warehouse_schema.png)

With this data model, users will be able to analyze trends in tests, positive cases, and deaths at both a county and state level over time. Additionally, including county populations data will allow users to normalize trends as a proportion of county/regional/state populations, allowing for more contextualized data.

## Additional Considerations
- **Data Growth:** As the dataset grows in size, we may consider moving this project to an [**AWS EMR cluster**](https://aws.amazon.com/emr/), which allows for horizontal scaling of pre-configured Hadoop/Spark Environments, to accomodate a data volumes up to 100x the current size. Currently, the dataset is small enough that Spark scripts can be run locally, but that is subject to change as additional datasets are added or as the dataset grows in size. Use of AWS EMR will be reassessed during that time.
- **Pipeline Scheduling:** Updates are currently run manually, by submitting the <code>load_delta_lake.py</code> spark script and running the <code>etl.py</code> on an ad-hoc basis. However, as the need for automated updates grow, the team may consider implementing [Apache Airflow](https://airflow.apache.org/), and refactoring the <code>load_delta_lake</code> functions into Spark operators, as well as refactoring the <code>etl</code> and <code>read_s3_boto</code> functions into s3, Redshift/Postgres, and Python operators.
- **Database/Datalake Access**: This script is developed to allow each user to stand up his/her own Delta Lake for analysis. However, in the case that a single Delta Lake requires multiple users to access it, the s3 bucket access can be configured to allow different users read access to the data on a relatively low-cost basis. As for the Redshift warehouse, access can be granted to multiple users by creating an [AWS Redshift Security Group](https://docs.aws.amazon.com/redshift/latest/mgmt/working-with-security-groups.html). Please note that Redshift cluster charges may increase as more users make calls to Redshift, or as compute requirements increase as more users access the data warehouse. 

## Scripts and Process
For this project, Spark and Boto3 are leveraged to extract the data from its source location and write the data to S3 in parquet/delta format. From there, a python ETL script is also made available to load the Delta Lake data to Amazon Redshift for analysis.

The following scripts and helper scripts facilitate this process:

### Main Scripts:
- **load_delta_lake.py**: PySpark Script that reads data from the various sources and writes to a Delta Lake s3 bucket by leveraging helper functions in the <code>read_s3_boto.py</code>.
- **etl.py**: Copies data from Delta Lake s3 Bucket, by invoking helper queries in the <code>sql_queries.py</code> file, and then writes parsed data to an Amazon Redshift Data Warehouse with the following schema:

### Helper Scripts
- **sql_queries.py**: Defines drop, create, copy, and load table queries to extract data from Delta Lake s3 bucket into redshift
- **read_s3_boto**: Leverages Boto3 to interact with s3 buckets that contain source datasets from the NIH and Rearc/NYT. This tool was implemented due to iteroperability issues between the datasources and the new Spark 3.0, but may be depreciated as the bugs are resolved.
- **dwh.cfg**: Config file that contains connection parameters to the Redshift data warehouse

## Prequisites
To implement this datalake, you must have the following tools set up in your development environment:
- Spark 3.0
- Delta Lake 0.7.0
- AWS Account with Console Access
- The aws-java-sdk jar, which can be downloaded from [mvn repository](https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk). The path to this jar file must be copied to the path into the specified location in <code>load_delta_lake.py</code>
- The delta-core jar, which can be downloaded from [mvn repository](https://mvnrepository.com/artifact/io.delta/delta-core). The path to this jar file must be copied to the path into the specified location in <code>load_delta_lake.py</code>
- An S3 bucket to write the Delta files to

To create and populate the Redshift data warehouse, the following steps must completed:
1. Create AWS Redshift cluster 
2. Create IAM role with S3 read access
3. Populate the dwh.cfg file with the correct database credentials and IAM ARN

## Instructions

1. Once the environment set up, <code>spark-submit</code> the <code>load_delta_lake.py</code> file to populate the Delta Lake
2. Once the spark script is complete, run the <code>etl.py</code> file as standard python file to copy the data into Redshift  

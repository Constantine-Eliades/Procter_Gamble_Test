# Forecasting Solution

This repository contains a forecasting solution with the following components:

- **config.yml**: Configuration file.
- **common.py**: Module containing all common libraries.
- **run_forecast.py**: Main module that calls the forecasting script.
- **load_data.py**: Script module that loads CSV data and saves them to a Parquet file. The Parquet file is partitioned by product ID, but comments suggest an alternative partitioning by order date and saving as a single file.
- **test_forecast.py**: File that uses pytest to test the existence of CSV files' data.
- **test.yml**: Test file for GitHub CI/CD.
- **requirements.txt**: Contains the necessary libraries to be installed.
- **parquet_file_schema.txt**: Schema of Parquet files.
- **readme.md**: This file.

## Parquet File Schema
   - product_id : string,
   - product_category_name : string,
   - price_sum : float64,
   - week_start_date : datetime

## Features Extraction and Usage

Regarding the questions and answers provided in the readme.txt:

## Questions and Answers

### 1. Features Extraction

- **Which features would you extract and how from the tables? How would you use the remaining tables?**
   - Extract relevant features such as product price, customer information, etc.
   - Consider aggregating features like total sales, average sales, etc.

### 2. Application in Production

- **How would you turn it into an application in production?**
   - Wrap the forecasting code into a function or class for modularity.
   - Use a task scheduler (e.g., Airflow) for regular updates.
   - Deploy the solution as a microservice or serverless function for scalability.

### 3. Design for Multiple Countries

- **How would you design an application if you knew that you would have to build a similar solution for a couple of other countries, and the data schema might be different for them, however, you can get the same underlying data?**
   - Create a modular and flexible ETL process to handle different data schemas.
   - Use configuration files or metadata to define data mappings for each country.
   - Implement a country-specific preprocessing step to handle schema variations.
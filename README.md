![Screenshot (487)](https://github.com/user-attachments/assets/6b8006a0-f44c-4045-9c33-3a4edb232e0e)



# SnowPark-DataPipeline-Project

This project demonstrates the creation of an ETL (Extract, Transform, Load) data pipeline using Snowflake's Snowpark API and Snowpipe. The pipeline is designed to process and transform data collected from Kaggle and then load the transformed data into a Snowflake table.

## Project Overview

- **Data Source :** Kaggle datasets
- **Cloud Platform :** Snowflake
- **Technologies :Used**
  - **SnowPark API :** For in-Snowflake data processing and transformations.
  - **SnowPipe :** For automated data ingestion from stage to table.
  - **SQL and Python :** Used within Snowflake for scripting and managing the data pipeline.
  
## Key Features

- **Data Extraction :** Imported the Bengaluru House Dataset from Kaggle directly into a Snowflake stage.
- **Data Transformation :** Used the Snowpark API to clean, transform, and prepare the data within Snowflake.
- **Data Loading :** Loaded the transformed data into a Snowflake table.
- **ETL Pipeline :** The entire ETL process is executed within the Snowflake environment, ensuring the data is ready for analysis.

## How to Run

1. Ensure you have access to a Snowflake account and environment.
2. Clone this repository and review the provided scripts.
3. Execute the ETL pipeline within the Snowflake environment using the Snowpark API and Snowpipe for data processing and loading.

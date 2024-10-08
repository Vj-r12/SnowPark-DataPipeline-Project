-- CREATEING DATABASE.

CREATE DATABASE SNOWPARK_DATAPIPELINE_PROJECT;


-- CREATEING INTERNAL STAGE.

CREATE STAGE IF NOT EXISTS SNOWPARK_DATAPIPELINE_PROJECT.PUBLIC.DATAPIPELINE_STAGE
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"')
ENCRYPTION = (TYPE='SNOWFLAKE_SSE');


-- CREATING FILE FORMAT.

CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"';


-- CREATING MAIN TABLE.

CREATE TABLE IF NOT EXISTS SNOWPARK_DATAPIPELINE_PROJECT.PUBLIC.HOUSE_DATA (
                                        AREA_TYPE VARCHAR,
                                        AVAILABILITY VARCHAR,
                                        LOCATION VARCHAR,
                                        SIZE VARCHAR,
                                        SOCIETY VARCHAR,
                                        TOTAL_SQFt VARCHAR,
                                        BATH INT,
                                        BALCONY VARCHAR,
                                        PRICE FLOAT
                                      );

-- CREATING SNOW PIPE 
-- SNOW PIPE -->> WHICH TAKES DATA FROM STAGE TO TABLE AUTOMATTICALLY WHEN THE NEW DATA IS AVAILABLE AT STAGEING AREA.

CREATE PIPE IF NOT EXISTS SNOWPARK_DATAPIPELINE_PROJECT.PUBLIC.DATAPIPELINE_PROJECT_PIPE AS
COPY INTO HOUSE_DATA -- TABLE
FROM @DATAPIPELINE_STAGE
FILE_FORMAT = (TYPE='CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"');


-- REFRESHING SNOW PIPE BY USING TASK MANGER IT WILL AUTOMATICALLY REFRESH.

CREATE OR REPLACE TASK SNOWPARK_DATAPIPELINE_PROJECT.PUBLIC.REFRESH_PIPE
WAREHOUSE = PIPELINE_PROJECT
SCHEDULE = 'USING CRON 0 * * * * UTC' -- Runs every hour
AS
ALTER PIPE DATAPIPELINE_PROJECT_PIPE REFRESH;


-- REFRESHING SNOW PIPE BY MANUALLY.

ALTER PIPE DATAPIPELINE_PROJECT_PIPE REFRESH;
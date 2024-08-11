# IMPORTING LIBRARIES.

# NOTE -->> WE ARE WORKING WITH SNOWFLAKE ENVIRONMENT WITHIN CLOUD.

import snowflake.snowpark as sp
from snowflake.snowpark import functions as f

# EXTRACTING DATA.

def Extract_Data(session: sp.Session):

    # Extracting data from table.
    
    df = session.table('House_Data')
    return df
    

# TRANSFORMING DATA.

def Transform_Data(df):
    
    # Transforming column data & renaming columns.
    
    transform_1 = df.select(f.trim(f.col('area_type')).alias('AREA_TYPES'),
                  f.trim(f.col('availability')).alias(('AVAILABILITY')),
                  f.trim(f.col('location')).alias(('LOCATION')),
                  f.trim(f.col('society')).alias(('SOCIETY')),
                  f.ltrim(f.col('size')).alias('SIZE'),
                  f.col('TOTAL_SQFT'),
                  f.ltrim(f.col('Bath')).alias('BATHROOM'),
                  f.col('BALCONY'),
                  f.col('Price').alias('PRICE_PER_SQFT'))

    # Transforming column data.

    transform_2 = transform_1.select(f.initcap(f.col('AREA_TYPES')).alias('AREA_TYPES'),
                                     f.initcap(f.col('LOCATION')).alias('LOCATION'),
                                     f.initcap(f.col('SOCIETY')).alias('SOCIETY'),
                                     f.col('AVAILABILITY'),
                                     f.col('SIZE'),
                                     f.col('TOTAL_SQFT'),
                                     f.col('BATHROOM'),
                                     f.col('BALCONY'),
                                     f.col('PRICE_PER_SQFT'))

    # Handling null values. 

    transform_3 = transform_2.fillna({'SOCIETY' : 'Not Specified', 'BATHROOM' : 'Not Available',
                                      'BALCONY' : 'Not Available','AREA_TYPES': 'Unknown', 
                                      'LOCATION': 'Unknown','SIZE': 'Not Specified', 
                                      'TOTAL_SQFT': 0, 'PRICE_PER_SQFT': 0})
 
    # Replaceing balcony columns values.
    
    transform_4 = transform_3.replace({'0' : 'Not Available', 'null' : 'Not Specified', 'BathRoom' : 'Not Available'}, subset=['BALCONY', 'SIZE', 'BATHROOM'])
    
    # Returning Dataframe.
    
    return transform_4


# LOADING DATA.

def Load_Data(transform_4):
    try:

        # loading data into table.

        transform_4.write.save_as_table("Final_House_Data", mode="append")
        
    except Exception as error:
        print('Oops there is an error occured', error)
    else:
        print('Data Loaded Into Table!!')


# MAIN FUNCTION -->> ALL FUNCTIONS ARE EXECUTING UNDER MAIN FUNCTION.

def main(session: sp.Session):
    df = Extract_Data(session)
    transformed_df = Transform_Data(df)
    Load_Data(transformed_df)
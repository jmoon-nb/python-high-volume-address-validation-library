import os
import sys
import argparse
import subprocess
import configparser
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine


def get_db_connection_string(database_name):
    db_config = configparser.ConfigParser()
    db_config.read(db_config_file)

    import_db_connection_string = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Database={database_name};"
        f"Server={db_config['DATABASE']['SERVER']};"
        f"UID={db_config['DATABASE']['USERNAME']};"
        f"PWD={db_config['DATABASE']['PASSWORD']};"
        "TrustServerCertificate=YES;"
    )
    
    import_db_connection_string = f"mssql+pyodbc:///?odbc_connect={import_db_connection_string}"

    return import_db_connection_string 

def get_source_locations(connection_engine):

    query = """
        SELECT TOP 10
            *
        FROM Staging.Location l
    """
    connection = connection_engine.connect()
    
    unfiltered_locations_df = pd.read_sql_query(query, connection)
    locations_df = unfiltered_locations_df[['Address1', 'Address2', 'City', 'State', 'Zip', 'LocationID']]

    return locations_df, unfiltered_locations_df

def append_results(results_df, unfiltered_locations_df):
    result_col_mapping = {
        'location_id': 'LocationID',
        'street_number': 'StreetNumberValidation',
        'route': 'StreetValidation',
        'locality': "LocalityValidation",
        'administrative_area_level_1': 'StateCodeValidation',
        'postal_code': 'PostalCodeValidation',
        'postal_code_suffix': 'PostalCodeSuffixValidation',
        'error_msg': 'ValidationError'
    }

    # Select subset of columns from results_df
    columns_to_merge = ['location_id', 'street_number', 'route', 'locality', 'administrative_area_level_1', 'postal_code', 'postal_code_suffix', 'error_msg']
    results_df_subset = results_df[columns_to_merge].rename(columns=result_col_mapping)

    # Update 'LocationID' data type in results dataframe 
    results_df_subset['LocationID'] = pd.to_numeric(results_df_subset['LocationID'], errors='coerce').astype('Int64') 

    final_df = pd.merge(unfiltered_locations_df, results_df_subset, on='LocationID', how='inner') 

    final_df = final_df[
        [
            'LocationID',
            'ImportedLocationID',
            'JRSLocationID',
            'LocationName',
            'Address1',
            'Address2',
            'City',
            'State',
            'Zip',
            'County',
            'FIPS',
            'Country',
            'StreetNumberValidation',
            'StreetValidation',
            'LocalityValidation',
            'StateCodeValidation',
            'PostalCodeValidation',
            'PostalCodeSuffixValidation',
            'ValidationError'
        ]
    ]

    final_df.to_csv('final-validation-results.csv', index=False)
    
    return final_df

def insert_into_ids(connection_engine):
    sql_file = os.path.join(local_dir, 'InsertResults.sql')
    with open(sql_file, 'r') as file:
        query = file.read()

    connection = connection_engine.connect()

    try:
        connection.execute(query)
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(f"Query execution failed: {e}")


    return None 

def main():

    locations_df, unfiltered_locations_df = get_source_locations(engine)

    locations_df.to_csv('./src/sample-data/only_addresses.csv', index=False)

    print("CSV Generated")

    script_path = Path("./src/main.py")

    result = subprocess.run(["python", script_path.name], cwd=script_path.parent, capture_output=True, text=True)

    # Access output and status of subprocess
    # print("Output:", result.stdout)
    # print("Errors:", result.stderr)
    # print("Exit Code:", result.returncode)

    if result.returncode == 0:
        print("Google Bulk Address Validation Script executed successfully!")
    else:
        print("Google Bulk Address Validation Script encountered an error.")


    results_df = pd.read_csv('./src/output.csv')
    
    append_results(results_df, unfiltered_locations_df)

    return None

if __name__ == "__main__":
    local_dir = os.path.dirname(os.path.abspath(__file__))
    db_config_file = os.path.join(os.getcwd(), 'config.ini')
    config_exists = os.path.exists(db_config_file)

    parser = argparse.ArgumentParser(description='Generates a CSV file with a list of locations for a specified jurisdiction')

    args = parser.parse_args()

    if not config_exists:
        print("This script is designed to be run locally and requires a config.ini file to be present.")
        sys.exit(1)

    database_name = 'ImportDataStaging'
    import_db_connection_string = get_db_connection_string(database_name)

    engine = create_engine(import_db_connection_string)
    
    main()
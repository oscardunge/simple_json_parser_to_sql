##############################################################################
# README
##############################################################################
# This script reads JSON data into PostgreSQL using common Python datatypes
# such as dict, list, pandas dataframe, and str.
##############################################################################
# Input: JSON filename without file extension, ID for unique constraint
##############################################################################
# Output: Not applicable
##############################################################################
# End state: Data imported to PostgreSQL
##############################################################################

import json
import pandas as pd
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
import re
import sys
from typing import List, Dict, Union, Any
from config.connection_details import conn_details



def json_file_name_to_dataframe(table_name: str) -> pd.DataFrame:
    """
    Reads a JSON file and converts it to a pandas DataFrame.

    Args:
        table_name (str): The name of the JSON file (without the .json extension).

    Returns:
        pd.DataFrame: A DataFrame containing the data from the JSON file.
    """
    with open(f"{table_name}.json", 'r') as f:
        data = json.load(f)

    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        df = pd.DataFrame([data])
    else:
        df = pd.DataFrame()

    return df

def pandas_dataframe_to_sql_return_tablename(table_name: str) -> str:
    """
    Converts a pandas DataFrame to a SQL table and returns the table name with a timestamp.

    Args:
        table_name (str): The base name of the table.

    Returns:
        str: The table name with a timestamp appended.
    """
    df = json_file_name_to_dataframe(table_name)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    table_name_with_timestamp = f"{table_name}_{timestamp}"

    engine = create_engine('postgresql+psycopg2://oscar:@localhost:5432/postgres')
    df.to_sql(table_name_with_timestamp, engine, if_exists='replace', index=False, method='multi', schema='sql_dwh')

    return table_name_with_timestamp

def get_column_names(table_name: str,  cursor: psycopg2.extensions.cursor) -> list:
    """
    Retrieves the column names of a table.

    Args:
        table_name (str): The name of the table.
        cursor (psycopg2.extensions.cursor): The database cursor.

    Returns:
        list: A list of column names.
    """
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    columns = [f'"{row[0]}"' for row in cursor.fetchall()]

    return columns

def create_table_with_constraints_return_new_tablename(table_name: str, id: str, conn_details: dict) -> str:
    """
    Creates a new table with a unique constraint and returns the table name with a timestamp.

    Args:
        table_name (str): The base name of the table.
        id (str): The column name to apply the unique constraint.
        conn_details (dict): The connection details for the PostgreSQL database.

    Returns:
        str: The table name with a timestamp appended.
    """
    local_connection = psycopg2.connect(**conn_details)
    local_cursor = local_connection.cursor()

    try:
        table_name_with_timestamp = pandas_dataframe_to_sql_return_tablename(table_name)
        print(table_name_with_timestamp)
        local_connection.commit()

        create_statement = f"""
            create table if not exists {table_name} as
            select *
            from {table_name_with_timestamp}
            limit 0;
            """
        print(create_statement)

        local_cursor.execute(create_statement)
        local_connection.commit()
    except Exception as e:
        print(f"Error___: {e} error reported from create try in function create_table_with_constraints_return_new_tablename")

    try:
        constraint_statement = f"""
            alter table {table_name}
            add constraint {id}_{table_name}
            unique ({id});"""

        print(constraint_statement)
        local_cursor = local_connection.cursor()
        local_cursor.execute(constraint_statement)
        local_cursor.connection.commit()
        return table_name_with_timestamp

    except Exception as e:
        print(f"Log: {e} from constraint try block in function create_table_with_constraints_return_new_tablename")
        return table_name_with_timestamp
    finally:
        local_cursor.close()

def main(table_name: str, id: str):
    """
    Main function to read JSON data into PostgreSQL with a unique constraint.

    Args:
        table_name (str): The base name of the table.
        id (str): The column name to apply the unique constraint.
    """
    error_occurred = False

    conn = psycopg2.connect(**conn_details)
    cursor = conn.cursor()

    table_name_with_timestamp = create_table_with_constraints_return_new_tablename(table_name, id, conn_details)

    try:
        columns = get_column_names(table_name, cursor)
        columns_str = ', '.join(columns)

        insert_statement = f"""
        insert into {table_name} ({columns_str})
        select {columns_str}
        from {table_name_with_timestamp}
        on conflict ({id}) do nothing
        ;
        """
        print(insert_statement)

        cursor.execute(insert_statement)

    except psycopg2.Error as e:
        print(f"Error__: {e} , will not drop {table_name_with_timestamp} or commit")
        error_occurred = True
    finally:
        if not error_occurred:
            cursor.execute(f"DROP TABLE {table_name_with_timestamp}")
            print(f"attemt: DROP TABLE {table_name_with_timestamp}")
            conn.commit()
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    import sys
    main(sys.argv[1], sys.argv[2])
    # example main("bronze_filename_2024", "natural_key")

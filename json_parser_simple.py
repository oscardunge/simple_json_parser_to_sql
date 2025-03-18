##############################################################################
##readme######################################################################
####read json into postgres sql use common  python datatypes##################
####ie dict list pandas dataframe str#########################################
##############################################################################
####input: jsonfilename withouth file ending, id for unique constraint########
##############################################################################
####output: Not applicable####################################################
##############################################################################
####end state is data import to postgres #####################################
##############################################################################
##############################################################################
##############################################################################


import json
import pandas as pd
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
import re
import sys
from typing import List, Dict, Union, Any


conn_details = {
        "dbname":'postgres',
        "user":'oscar',
        "password":'',
        "host":'localhost',
        "port":'5432',
        "options":"-c search_path=sql_dwh"
    }

class CustomExceptionWithVariable(Exception):
    def __init__(self, message, variable):
        super().__init__(message)
        self.variable = variable
    def __str__(self):
        return f"{super().__str__()} (Variable: {self.variable})"



def json_file_name_to_dataframe(table_name: str) -> pd.DataFrame:
    with open(f"{table_name}.json", 'r') as f:
        data = json.load(f)
    
    if isinstance(data, list):
        df = pd.DataFrame(data)
    elif isinstance(data, dict):
        df = pd.DataFrame([data])
    else:
        df = pd.DataFrame()
    
    return df


def pandas_dataframe_to_sql_return_tablename(table_name: str) -> str :
    df = json_file_name_to_dataframe(table_name)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    table_name_with_timestamp = f"{table_name}_{timestamp}"
    
    engine = create_engine('postgresql+psycopg2://oscar:@localhost:5432/postgres')
    
    df.to_sql(table_name_with_timestamp, engine, if_exists='replace', index=False, method='multi', schema='sql_dwh')
    
    return table_name_with_timestamp




def get_column_names(table_name: str,  cursor: psycopg2.extensions.cursor) -> list:
    
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
    columns = [f'"{row[0]}"' for row in cursor.fetchall()]
    
    return columns





def create_table_with_constraints_return_new_tablename(table_name: str, id: str, conn_details: dict) -> str:
    
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
        print(f"Error___: {e} create_table_with_constraints_return_new_tablename")
    
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
        print(f"Error___: {e} create_table_with_constraints_return_new_tablename")
        return table_name_with_timestamp
    finally:
        local_cursor.close()


def main(table_name: str, id: str):
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





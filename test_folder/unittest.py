try:
    import json
    from json_parser_simple import *
    import pandas as pd
    import psycopg2
    from sqlalchemy import create_engine
    from datetime import datetime


    test_table_name = 'test_table_name'
    
    test_data = [
        {"col1": 1, "col2": "a"},
        {"col1": 2, "col2": "b"}
    ]


    filename = 'test_table.json'


    with open(filename, 'w') as f:
        json.dump(test_data, f)


    conn = psycopg2.connect(**conn_details)
    cursor = conn.cursor()

    table_name_with_timestamp = create_table_with_constraints_return_new_tablename(test_table_name, 'col1', conn_details)
    assert table_name_with_timestamp.startswith('test_table_name'), "Table name with timestamp is incorrect"

    conn.commit()

    print(f"{test_table_name} created successfully.")



    # Test json_file_name_to_dataframe
    df = json_file_name_to_dataframe(test_table_name)
    print(df)
    assert isinstance(df, pd.DataFrame), "The output is not a DataFrame"
    assert list(df.columns) == ['col1', 'col2'], "Column names do not match"
    assert df.shape == (2, 2), "DataFrame shape is incorrect"

    # Test pandas_dataframe_to_sql_return_tablename
    # df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    table_name_with_timestamp = pandas_dataframe_to_sql_return_tablename(test_table_name)
    print(table_name_with_timestamp)
    cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{test_table_name}'")
    sql_table_name_return = [row[0] for row in cursor.fetchall()]
    assert sql_table_name_return[0] in table_name_with_timestamp, "Table name with timestamp is incorrect"


    # Test get_column_names
    # conn = psycopg2.connect(**conn_details)
    # cursor = conn.cursor()
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{test_table_name}'")
    columns = [f'{row[0]}' for row in cursor.fetchall()]
    print(columns)
    assert columns == ['col1', 'col2'], "Column names do not match"


    print("All tests passed!")

except AssertionError as e:
    print(f"Assertion Test failed: {e}")
except Exception as e:
    print(f"Exception {e}")

finally:
    # Drop the test table
    drop_table_query_new_rows = f"DROP TABLE IF EXISTS {table_name_with_timestamp} CASCADE;"
    drop_table_query_main = f"DROP TABLE IF EXISTS {test_table_name} CASCADE;"


    cursor.execute(drop_table_query_new_rows)
    
    conn.commit()

    cursor.execute(drop_table_query_main)
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"{test_table_name} and constraints deleted successfully.")
    print(f"{table_name_with_timestamp} and constraints deleted successfully.")

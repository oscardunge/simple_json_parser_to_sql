
# Simple JSON Parser to SQL

## Overview

This project provides a simple Python script (`json_parser_simple.py`) that parses JSON data and inserts it into a PostgreSQL database. It is designed to be straightforward and easy to use, making it ideal for small projects or learning purposes.
It solves wanting a sql database from a no-sql fileformat, for acid transactions, easy transformability and reproducable analytics, ML and visualizations. 

## Features

- Parses JSON data from a file.
- Inserts parsed data into a PostgreSQL database.
- Supports basic error handling.
- Has a unit-test in test_folder/unittest.py

## Requirements

- Python 3.x
- Required Python libraries (install via `pip`):
    ```bash
    pip install pandas psycopg2 sqlalchemy
    ```
- from config.connection_details import conn_details
    holds some simple localhost info, but its nice to 
    have separate, to mask from git. 

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/oscardunge/simple_json_parser_to_sql.git
    ```
2. Navigate to the project directory:
    ```bash
    cd simple_json_parser_to_sql
    ```

## Usage

1. Ensure you have a JSON file ready for parsing.
2. Run the script with the JSON file as an argument:
    ```bash
    python json_parser_simple.py your_data.json
    ```
3. The script will create a PostgreSQL database and insert the parsed data into it.

## Example

```
    #this input method used in terminal 
    #example python json_parser_simple.py bronze_filename_2024 natural_key
    #will use main(sys.argv[1], sys.argv[2]) 
```

## Input

- JSON file containing data to be parsed. Example format:
    ```json
    [
        {"name": "John Doe", "age": 30},
        {"name": "Jane Smith", "age": 25}
    ]
    ```

## Output

- In mentioned example Data inserted into a PostgreSQL database table in schema sql_dwh if it exists. 
Otherwise run "CREATE SCHEMA sql_dwh;"  named `data`.

## Tests

The `test_folder` contains tests to ensure the functionality of the `json_parser_simple.py` script. These tests cover various scenarios, including:

- Valid JSON input
- Invalid JSON input
- Database connection errors
- Data insertion errors

### Running Tests

To run the tests, use the following command:
```bash
python -m test_folder.unittest
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## Contact

For any questions or inquiries, please contact Oscar Dunge.



# Simple JSON Parser to SQL

## Overview

This project provides a simple Python script (`json_parser_simple.py`) that parses JSON data and inserts it into a PostgreSQL database. It is designed to be straightforward and easy to use, making it ideal for small projects or learning purposes.

## Features

- Parses JSON data from a file.
- Inserts parsed data into a PostgreSQL database.
- Supports basic error handling.

## Requirements

- Python 3.x
- Required Python libraries (install via `pip`):
    ```bash
    pip install pandas psycopg2 sqlalchemy
    ```

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

```python
import json
import pandas as pd
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
import re
import sys
from typing import List, Dict, Union, Any

def parse_json_to_sql(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)
    
    # Database connection
    engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')
    conn = engine.connect()
    
    # Create table if not exists
    conn.execute('''CREATE TABLE IF NOT EXISTS data (
                        id SERIAL PRIMARY KEY,
                        name TEXT,
                        age INTEGER
                    )''')
    
    # Insert data
    for item in data:
        conn.execute('INSERT INTO data (name, age) VALUES (%s, %s)', (item['name'], item['age']))
    
    conn.close()

if __name__ == "__main__":
    parse_json_to_sql(sys.argv[1])
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

- Data inserted into a PostgreSQL database table named `data`.

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

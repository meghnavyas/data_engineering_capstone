# Python code to read data from various file formats
import requests
import pandas as pd
import json
import sqlite3


# TODO: Fetch Data from API and Save Locally. 
# Pull Pokemon data from the PokeAPI and save it to a local file. The local JSON file should contain Pokemon data for Bulbasaur (ID: 1)
# 1. Make a GET request to data_api; 
# Ref docs: https://docs.python-requests.org/en/latest/user/quickstart/#response-content

# 2. Extract the JSON response
# Ref docs: https://docs.python-requests.org/en/latest/user/quickstart/#json-response-content

# 3. Write the JSON data to local_file

# Open a file writer
# Ref docs: https://docs.python.org/3/tutorial/inputoutput.html#methods-of-file-objects and how to 

# Save json into the open file writer
# Ref docs https://docs.python.org/3/tutorial/inputoutput.html#saving-structured-data-with-json


# TODO:
# 1. Use Python standard libraries to open local_file
# Ref docs:  https://docs.python.org/3/tutorial/inputoutput.html#methods-of-file-objects 

# 2. Use json.load to convert it into a json 
# Ref docs: https://docs.python.org/3/library/json.html#json.load

# 3. Read the name and id from the json, as you would from a dictionary


'''
Parse Data and Insert into SQLite Database. Extract specific Pokemon attributes and store them in a SQLite database.
Table Schema:

CREATE TABLE pokemon (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    base_experience INTEGER
);
'''


def read_from_api(url, headers=None, params=None):
    """Fetch data from a REST API endpoint."""
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()


def write_json_to_file(data, file_path):
    """Write JSON data to a local file."""
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)


def read_from_csv(file_path):
    """Read data from a CSV file."""
    df = pd.read_csv(file_path)
    return df.head()


def read_from_json(file_path):
    """Read data from a JSON file."""
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data


def connect_to_sqlite(db_path):
    """Connect to a SQLite database."""
    conn = sqlite3.connect(db_path)
    return conn


def create_pokemon_table(conn):
    """Create the pokemon table in the SQLite database."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS POKEMON (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        base_experience INTEGER);
    """
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    conn.commit()


def insert_pokemon(conn, pokemon):
    """Insert a new pokemon into the pokemon table."""
    insert_sql = """
    INSERT INTO POKEMON (id, name, base_experience)
    VALUES (?, ?, ?);
    """
    cursor = conn.cursor()
    cursor.execute(insert_sql, (pokemon['id'], pokemon['name'], pokemon['base_experience']))
    conn.commit()


def drop_pokemon_table(conn):
    """Drop the pokemon table from the SQLite database."""
    drop_table_sql = "DROP TABLE IF EXISTS POKEMON;"
    cursor = conn.cursor()
    cursor.execute(drop_table_sql)
    conn.commit()

def main():
    #api_url = "https://pokeapi.co/api/v2/pokemon/1"
    #data = read_from_api(api_url)
    #print(data)

    #csv_file_path = "/Users/meghnavyas/Documents/data_engineering_capstone/notebooks/data/customer.csv"
    #print(read_from_csv(csv_file_path))

    data_api = "https://pokeapi.co/api/v2/pokemon/1/"
    local_file = "pokemon_data.json"
    database_file = "pokemon.db"

    api_data = read_from_api(data_api)
    write_json_to_file(api_data, local_file)
    print(f"Pokemon data saved to {local_file}")
    json_data = read_from_json(local_file)
    print(f"Pokemon Name: {json_data['name']}, ID: {json_data['id']}")


    conn = connect_to_sqlite(database_file)
    print(f"Connected to SQLite database at {database_file}")

    drop_pokemon_table(conn)

    create_pokemon_table(conn)
    insert_pokemon(conn, json_data)

    print(f"Inserted Pokemon {json_data['name']} into the database.")

    select_sql = 'SELECT * FROM POKEMON;'

    cursor = conn.cursor()
    cursor.execute(select_sql)
    rows = cursor.fetchall()


    print("Current records in POKEMON table:")
    for row in rows:
        print(row)

    conn.close()

if __name__ == "__main__":
    main()
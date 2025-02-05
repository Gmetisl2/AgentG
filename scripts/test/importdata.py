from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, text
import csv

# Connection details
server = 'sqldbserverms.database.windows.net'
database = 'sqldbms'
username = 'mszaplon'
password = ''

# Create the connection string
connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server'
engine = create_engine(connection_string)

def drop_table_if_exists(table_name):
    with engine.connect() as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        connection.commit()

def create_table_from_csv(table_name, csv_file, columns):
    metadata = MetaData()
    
    # Create the table
    table = Table(
        table_name, metadata,
        *(Column(col_name, col_type) for col_name, col_type in columns)
    )
    
    # Drop and recreate the table
    metadata.drop_all(engine, tables=[table])
    metadata.create_all(engine)
    
    # Print table schema for debugging
    print(f"\nTable {table_name} schema:")
    for column in table.columns:
        print(f"Column: {column.name}, Type: {column.type}")
    
    with engine.connect() as connection:
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)  # Store headers for debugging
            print(f"\nCSV headers for {csv_file}:")
            print(headers)
            
            for data in reader:
                data_dict = {col_name: val for (col_name, _), val in zip(columns, data)}
                print(f"\nInserting data: {data_dict}")  # Debug print
                try:
                    connection.execute(table.insert().values(**data_dict))
                    connection.commit()
                except Exception as e:
                    print(f"Error inserting row: {e}")
                    continue

# Define columns for each table
rewards_columns = [
    ('ID', Integer),
    ('userID', String),  # Changed from 'userid' to 'userID' to match case
    ('wa', String),
    ('balance', Integer),
    ('reward', Integer),
    ('tx', String),
    ('date', Date),
    ('reward_round', Integer)
]

waMap_columns = [
    ('ID', Integer),
    ('platform', String),
    ('userID', String),
    ('username', String),
    ('wa', String),
    ('date', Date)
]

# Drop existing tables
drop_table_if_exists('rewards')
drop_table_if_exists('waMap')

# Create new tables and import data
create_table_from_csv('rewards', 'rewards.csv', rewards_columns)
create_table_from_csv('waMap', 'waMap.csv', waMap_columns)
from sqlalchemy import create_engine, text
import os

# Connection details from environment variables
server = os.getenv('AZURE_SQL_SERVER')
database = os.getenv('AZURE_SQL_DATABASE')
username = os.getenv('AZURE_SQL_USERNAME')
password = os.getenv('AZURE_SQL_PASSWORD')

# Create the connection string
connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server'
engine = create_engine(connection_string)

# Insert a new row into the TEST table
with engine.connect() as connection:
    insert_statement = text("INSERT INTO TEST (ID, name) VALUES (3, 'Alice Johnson')")
    connection.execute(insert_statement)
    print("Inserted new row into TEST table")
import csv
from datetime import date
from sqlalchemy import create_engine, text
import os

def get_engine():
    # Connection details from environment variables
    server = os.getenv('AZURE_SQL_SERVER')
    database = os.getenv('AZURE_SQL_DATABASE')
    username = os.getenv('AZURE_SQL_USERNAME')
    password = os.getenv('AZURE_SQL_PASSWORD')
    
    # Create the connection string
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server'
    return create_engine(connection_string)

def insert_row(engine, platform, userID, username, wa, date_value):
    # Insert a new row into the waMap table
    insert_query = text("""
        INSERT INTO waMap (platform, userID, username, wa, date)
        VALUES (:platform, :userID, :username, :wa, :date)
    """)
    
    try:
        with engine.connect() as connection:
            connection.execute(insert_query, {
                'platform': platform,
                'userID': userID,
                'username': username,
                'wa': wa,
                'date': date_value
            })
            connection.commit()
    except Exception as e:
        print(f"Error inserting row: {e}")
        print(f"Data: platform={platform}, userID={userID}, username={username}, wa={wa}, date={date_value}")

def read_csv_and_insert(csv_filename, platform):
    engine = get_engine()
    rows = []
    
    # Read data from CSV
    try:
        with open(csv_filename, newline='') as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)  # Skip header row
            for row in csv_reader:
                if len(row) >= 3:  # Ensure row has required columns
                    userID, username, wa = row
                    rows.append((platform, userID, username, wa, date.today()))
                else:
                    print(f"Skipping invalid row: {row}")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    if rows:
        # Insert all rows into the Azure SQL database
        for row in rows:
            insert_row(engine, *row)
        
        # Clear the CSV file and write header
        try:
            with open(csv_filename, 'w', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow(['userID', 'username', 'wa'])  # Write header
            print(f"Successfully processed {len(rows)} rows and cleared the CSV file.")
        except Exception as e:
            print(f"Error clearing CSV file: {e}")
    else:
        print("No data found in the CSV file.")

if __name__ == "__main__":
    # Example usage
    read_csv_and_insert('newusers.csv', 'Telegram')
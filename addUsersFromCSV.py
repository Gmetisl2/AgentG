import csv
from datetime import date
from sqlalchemy import create_engine, text
import os
from tenacity import retry, stop_after_attempt, wait_exponential
from sqlalchemy.orm import Session
import logging

# Set up logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def get_engine():
    # Connection details from environment variables
    server = os.getenv('AZURE_SQL_SERVER')
    database = os.getenv('AZURE_SQL_DATABASE')
    username = os.getenv('AZURE_SQL_USERNAME')
    password = os.getenv('AZURE_SQL_PASSWORD')
    
    # Create the connection string with extended timeout
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server'
    
    # Create engine with connection pooling and extended timeout
    return create_engine(
        connection_string,
        connect_args={
            'timeout': 300,  # 5 minute timeout
            'retry_with_backoff': True,
            'backoff_factor': 2
        },
        pool_size=5,
        max_overflow=10
    )

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def insert_row(engine, platform, userID, username, wa, date_value):
    # Insert a new row into the waMap table
    insert_query = text("""
        INSERT INTO waMap (platform, userID, username, wa, date)
        VALUES (:platform, :userID, :username, :wa, :date)
    """)
    
    with Session(engine) as session:
        try:
            session.execute(insert_query, {
                'platform': platform,
                'userID': userID,
                'username': username,
                'wa': wa,
                'date': date_value
            })
            session.commit()
            logger.info(f"Successfully inserted row for user {username}")
        except Exception as e:
            session.rollback()
            logger.error(f"Error inserting row: {e}")
            logger.error(f"Data: platform={platform}, userID={userID}, username={username}, wa={wa}, date={date_value}")
            raise

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
                    logger.warning(f"Skipping invalid row: {row}")
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return

    if rows:
        successful_inserts = 0
        # Insert all rows into the Azure SQL database
        for row in rows:
            try:
                insert_row(engine, *row)
                successful_inserts += 1
            except Exception as e:
                logger.error(f"Failed to insert row: {e}")
                continue
        
        if successful_inserts > 0:
            # Clear the CSV file and write header only if we had successful inserts
            try:
                with open(csv_filename, 'w', newline='') as csvfile:
                    csv_writer = csv.writer(csvfile)
                    csv_writer.writerow(['userID', 'username', 'wa'])  # Write header
                logger.info(f"Successfully processed {successful_inserts} rows and cleared the CSV file.")
            except Exception as e:
                logger.error(f"Error clearing CSV file: {e}")
        else:
            logger.warning("No rows were successfully inserted. CSV file was not cleared.")
    else:
        logger.info("No data found in the CSV file.")

if __name__ == "__main__":
    # Example usage
    read_csv_and_insert('newusers.csv', 'Telegram')

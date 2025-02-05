import csv
import sqlite3
from datetime import date

def insert_row(platform, userID, username, wa, date):
    # Connect to the SQLite database
    conn = sqlite3.connect('gmetis.db')
    cursor = conn.cursor()
    
    # Insert a new row into the gmetis table
    cursor.execute('''
    INSERT INTO waMap (platform, userID, username, wa, date)
    VALUES (?, ?, ?, ?, ?)
    ''', (platform, userID, username, wa, date))
    
    # Commit the changes and close the connection
    conn.commit()
    conn.close()

def read_csv_and_insert(csv_filename, platform):
    rows = []
    with open(csv_filename, newline='') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            userID, username, wa = row
            rows.append((platform, userID, username, wa, date.today()))
    
    if rows:
        # Insert all rows into the SQLite database
        for row in rows:
            insert_row(*row)
        
        # Wipe all rows from the CSV file
        with open(csv_filename, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['userID', 'username', 'wa'])  # Write header
    else:
        print("No data found in the CSV file.")

# Example usage
read_csv_and_insert('newusers.csv', 'Telegram')

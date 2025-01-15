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

# Example usage
insert_row('Telegram', '343', 'tes', '0x23', date.today())



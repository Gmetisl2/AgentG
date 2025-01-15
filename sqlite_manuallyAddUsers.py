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
insert_row('Telegram', '1032939925', 'ekofinasir', '0x48fD5e19e829A8D7B0B8D9c4E5F0FAfFF5bd35f0', date.today())



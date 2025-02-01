import sqlite3
from datetime import date

def insert_row(platform, username, wa, date):
    # Connect to the SQLite database
    conn = sqlite3.connect('gmetisx.db')
    cursor = conn.cursor()
    
    # Insert a new row into the gmetis table
    cursor.execute('''
    INSERT INTO waMap (platform, username, wa, date)
    VALUES (?, ?, ?, ?)
    ''', (platform, username, wa, date))
    
    # Commit the changes and close the connection
    conn.commit()
    conn.close()

# Example usage
insert_row('twitter', 'testelizax2', '0x7e832478125d53542Ba3c9A41C05C4288C79Cd6B', date.today())



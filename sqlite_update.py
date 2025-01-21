import sqlite3

def update_wa(userID, new_wa):
    # Connect to the SQLite database
    conn = sqlite3.connect('gmetis.db')
    cursor = conn.cursor()
    
    # Update the wa field for the given userID
    cursor.execute('''
    UPDATE waMap
    SET wa = ?
    WHERE userID = ?
    ''', (new_wa, userID))
    
    # Commit the changes and close the connection
    conn.commit()
    conn.close()

# Example usage
update_wa('123', '0x6c')


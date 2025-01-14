import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('gmetis.db')
cursor = conn.cursor()

# Update the table to set wa, balance, reward, and tx to NULL
update_query = '''
UPDATE rewards
SET wa = NULL,
    balance = NULL,
    reward = NULL,
    tx = NULL
'''

try:
    cursor.execute(update_query)
    conn.commit()
    print("Columns wa, balance, reward, and tx have been set to NULL for all rows.")
except sqlite3.OperationalError as e:
    print(f"An error occurred: {e}")

# Close the connection
conn.close()
import sqlite3
from datetime import datetime

def setup_database(db_name='gmetisx.db'):
    # Connect to SQLite database (creates it if it doesn't exist)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create rewards table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS rewards (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT,
        post_id TEXT,
        wa TEXT,
        balance INTEGER,
        reward INTEGER,
        tx TEXT,
        date DATE,
        reward_round INTEGER
    )
    ''')
    
    # Create waMap table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS waMap (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        platform TEXT,
        username TEXT,
        wa TEXT,
        date DATE
    )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rewards_username ON rewards(username)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rewards_date ON rewards(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rewards_round ON rewards(reward_round)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_wamap_username ON waMap(username)')
    
    # Optional: Insert some sample data for testing
    cursor.execute('''
    INSERT INTO waMap (platform, username, wa, date)
    VALUES 
        (?, ?, ?, ?),
        (?, ?, ?, ?)
    ''', (
        'twitter', 'testuser1', '0x1234567890abcdef1234567890abcdef12345678', datetime.now().date(),
        'twitter', 'testuser2', '0xabcdef1234567890abcdef1234567890abcdef12', datetime.now().date()
    ))
    
    cursor.execute('''
    INSERT INTO rewards (username, post_id, wa, balance, reward, tx, date, reward_round)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        'testuser1', '123456789', '0x1234567890abcdef1234567890abcdef12345678', 
        100, 10, '0xabc...', datetime.now().date(), 1
    ))
    
    # Commit changes and close connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    setup_database()
    print("Database setup completed successfully!")
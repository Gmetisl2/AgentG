import sqlite3
import csv

def export_table_to_csv(db_name, table_name, csv_file):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    column_names = [description[0] for description in cursor.description]

    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(column_names)
        writer.writerows(rows)

    conn.close()

# Export tables
export_table_to_csv('gmetis.db', 'rewards', 'rewards.csv')
export_table_to_csv('gmetis.db', 'waMap', 'waMap.csv')
import sqlite3
import csv

def generate_csv(database, destination):
    # Connect to SQLite databasae
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    try:
        table_label = 'Label'
        columns_label = ['id', 'sessionId', 'affect', 'affect_binary', 'context', 'timestamp', 'time_of_engagement', 'time_of_completion']

        table_heart = 'HeartRateMeasurement'
        columns_heart = ['id', 'sessionId', 'timestamp', 'hr', 'hrIbi', 'status', 'hrIbi_quality', 'hrv']

        table_ppg = 'PpgGreenMeasurement'
        columns_ppg = ['id', 'sessionId', 'timestamp', 'value']

        table_accel = 'AccelerometerMeasurement'
        columns_accel = ['id', 'sessionId', 'timestamp', 'x', 'y', 'z']

        table_session = 'SessionData'
        columns_session = ['id', 'startTimeMillis', 'endTimeMillis']

        # Fetch data from database
        cursor.execute(f"SELECT * FROM {table_label}")
        data = cursor.fetchall()

        # Write data to CSV
        with open(destination, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            # Write header row
            csv_writer.writerow(columns_label)
            # Write data rows
            csv_writer.writerows(data)

        print("Data crawled and saved.")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close database connection
        if conn:
            conn.close()



database = '../Study_Data/My_DB.db'
destination = '../CSV_Data/label_csv'
generate_csv(database, destination)
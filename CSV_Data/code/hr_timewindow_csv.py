import csv
import sqlite3


def generate_timewindow_csv(database, destination, window=-3):
    try:
        conn = sqlite3.connect(database)
        cursor = conn.cursor()

        # Execute SQL query to retrieve relevant data
        hr_query = f"SELECT id, hr, timestamp FROM HeartRateMeasurement WHERE hr > 0;"
        cursor.execute(hr_query)
        hr_data = cursor.fetchall()

        label_query = f"SELECT id, affect, timestamp FROM Label WHERE timestamp > 0;"
        cursor.execute(label_query)
        label_data = cursor.fetchall()

    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        conn.close()

    # Write data to a CSV file
    with open(destination, 'w', newline='') as csv_file:
        columns = ["id", "hr-3", "hr-2", "hr-1", "affect"]
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(columns)

        for item in hr_data:
            csv_writer.writerow([item[0], item[1]])

        for item in label_data:
            csv_writer.writerow([item[1]])




database = '../../My_DB.db'
destination = '../../CSV_Data/hr_timewindow.csv'
generate_timewindow_csv(database, destination)
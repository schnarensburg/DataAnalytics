from datetime import datetime
import sqlite3
import os
import statistics
from itertools import groupby


def get_id(item):
    return item[0]


def match_notificationId(data_lists):
    '''
    Making sure to match LableData based on the id of each table
    '''
    if not data_lists:
        return []

        # Find the maximum length among all tuples
    max_len = max(len(data_list) for data_list in data_lists)

    # Initialize the result list with empty tuples
    merged_list = [tuple() for _ in range(max_len)]
    # Iterate through the remaining lists and extend the first tuple
    for data_list in data_lists:
        for i, item in enumerate(data_list):
            merged_list[i] += item

    return merged_list


def generate_label(db_folder):
    '''
    Generate table LabelData in each file
    '''
    db_files = [f for f in os.listdir(db_folder) if f.endswith('.db')]

    for file in db_files:
        # print(f"current db: {file}")
        # Connect to source database
        db_file_path = os.path.join(db_folder, file)
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()

        # Change time format to timestamp (InteractionMeasurement and NotificationData)
        cursor.execute("SELECT time_of_engagement, time_of_completion FROM InteractionMeasurement")
        interaction_data = cursor.fetchall()
        interaction_data_processed = []
        for tuple in interaction_data:
            time_of_engagement = datetime.fromisoformat(tuple[0]).timestamp()
            time_of_completion = datetime.fromisoformat(tuple[1]).timestamp()
            interaction_data_processed.append((time_of_engagement, time_of_completion))

        # Crawl label data from AffectData
        cursor.execute(f"SELECT notificationId, sessionId, affect FROM AffectData")
        affect_data = cursor.fetchall()

        # Add affect_binary (assigns 1 to positive and 0 to negative emotions)
        affect_data_processed = []
        for item in affect_data:
            if item[2] in {'Zufrieden', 'Glücklich', 'Begeistert', 'Aufgeregt', 'Erfreut', 'Erfüllt', 'Ruhig'}:
                affect_data_processed.append(item + (1,))
            else:
                affect_data_processed.append(item + (0,))

        # Crawl label data from ContextData
        cursor.execute(f"SELECT context FROM ContextData")
        context_data = cursor.fetchall()

        data = match_notificationId([affect_data_processed, context_data,
                                    interaction_data_processed])

        label_columns = {
            'LabelData': '"id" INTEGER, "SessionId" INTEGER, "affect" STRING, "affect_binary" INTEGER, "context" '
                         'STRING, "time_of_engagement" INTEGER, "time_of_completion" INTEGER'}
        # Add table to database
        cursor.execute(f"CREATE TABLE IF NOT EXISTS Label ({label_columns['LabelData']})")
        # Define SQL query for inserting data
        insert_query = "INSERT INTO Label (id, SessionId, affect, affect_binary, context, time_of_engagement, " \
                       "time_of_completion) VALUES (?, ?, ?, ?, ?, ?, ?)"

        # Iterate though data and insert each tuple into db
        for item in data:
            try:
                cursor.execute(insert_query, item)
            except sqlite3.Error as e:
                print(e)
        conn.commit()
        conn.close()
    print("label generated")


def add_sessionId(db_folder):
    '''
    Adding a sessionId in the relevant tables for the study data based on the time interval in SessionData
    '''
    # Get list of databases
    db_files = [f for f in os.listdir(db_folder) if f.endswith('.db')]

    sessionId_hilf = 0

    for file in db_files:
        # Connect to source database
        db_file_path = os.path.join(db_folder, file)
        conn = sqlite3.connect(db_file_path)
        cursor = conn.cursor()

        cursor.execute("SELECT id, startTimeMillis, endTimeMillis FROM SessionData")
        session_data = cursor.fetchall()

        print(f"{file}: {session_data}")
        for table_name in ['HeartRateMeasurement', 'Label', 'AccelerometerMeasurement', 'PpgGreenMeasurement']:
            if table_name == 'Label':
                cursor.execute(f"SELECT time_of_engagement FROM {table_name}")
                table_timestamp = cursor.fetchall()
            else:
                cursor.execute(f"SELECT timestamp FROM {table_name}")
                table_timestamp = cursor.fetchall()

            for timestamp in table_timestamp:
                for session in session_data:
                    session_id = session[0] + sessionId_hilf
                    start_time = session[1]
                    end_time = session[2]
                    if timestamp[0] is not None and start_time <= timestamp[0] <= end_time:
                        try:
                            cursor.execute(f"UPDATE {table_name} SET sessionId = ?", (session_id,))
                        except sqlite3.Error as e:
                            print(e)
                        conn.commit()

        sessionId_hilf += len(session_data)
        conn.commit()
        conn.close()

    print("sessionId generated")


def copy_to_new_db(db_folder, destination_db):
    '''
    Copy the processed data into one database
    '''
    conn_dest = sqlite3.connect(destination_db)
    cursor_dest = conn_dest.cursor()

    tables_to_copy = ['HeartRateMeasurement', 'AccelerometerMeasurement', 'PpgGreenMeasurement', 'Label', 'SessionData']

    # Initialize tables in destination
    cursor_dest.execute(f"CREATE TABLE IF NOT EXISTS HeartRateMeasurement ("
                        f"'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, "
                        f"'sessionId' INTEGER,"
                        f"'timestamp' INTEGER,"
                        f"'hr' INTEGER NOT NULL,"
                        f"'hrIbi' INTEGER,"
                        f"'hrIbi_quality' INTEGER,"
                        f"'status' INTEGER)"
                        );

    cursor_dest.execute(f"CREATE TABLE IF NOT EXISTS AccelerometerMeasurement ("
                        f"'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, "
                        f"'sessionId' INTEGER,"
                        f"'timestamp' INTEGER,"
                        f"'x' INTEGER NOT NULL,"
                        f"'y' INTEGER,"
                        f"'z' INTEGER)"
                        );

    cursor_dest.execute(f"CREATE TABLE IF NOT EXISTS PpgGreenMeasurement ("
                        f"'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, "
                        f"'sessionId' INTEGER,"
                        f"'timestamp' INTEGER,"
                        f"'value' INTEGER)"
                        );

    cursor_dest.execute(f"CREATE TABLE IF NOT EXISTS Label ("
                        f"'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, "
                        f"'sessionId' INTEGER,"
                        f"'affect' VARCHAR(30),"
                        f"'affect_binary' INTEGER,"
                        f"'context' VARCHAR(30),"
                        f"'time_of_engagement' INTEGER,"
                        f"'time_of_completion' INTEGER)"
                        );

    cursor_dest.execute(f"CREATE TABLE IF NOT EXISTS SessionData ("
                        f"'id' INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, "
                        f"'startTimeMillis' INTEGER,"
                        f"'endTimeMillis' INTEGER)"
                        );

    # Get list of databases
    db_files = [f for f in os.listdir(db_folder) if f.endswith('.db')]

    for file in db_files:
        print(file)
        # Connect to source database
        db_file_path = os.path.join(db_folder, file)
        conn_source = sqlite3.connect(db_file_path)
        cursor_source = conn_source.cursor()

        for table in tables_to_copy:
            # Fetch and insert data into the destination table
            if table == 'HeartRateMeasurement':
                cursor_source.execute(f"SELECT sessionId, timestamp, hr, hrIbi, status FROM {table};")
                table_data = cursor_source.fetchall()

                cursor_dest.executemany(f"INSERT INTO {table} (sessionId, timestamp, hr, hrIbi, status) VALUES (?, ?, "
                                        f"?, ?, ?)", table_data)

            elif table == 'AccelerometerMeasurement':
                cursor_source.execute(f"SELECT sessionId, timestamp, x, y, z FROM {table};")
                table_data = cursor_source.fetchall()

                cursor_dest.executemany(f"INSERT INTO {table} (sessionId, timestamp, x, y, z) VALUES (?, ?, "
                                        f"?, ?, ?)", table_data)

            elif table == 'PpgGreenMeasurement':
                cursor_source.execute(f"SELECT sessionId, timestamp, value FROM {table};")
                table_data = cursor_source.fetchall()

                cursor_dest.executemany(f"INSERT INTO {table} (sessionId, timestamp, value) VALUES (?, ?, ?)",
                                        table_data)

            elif table == 'Label':
                cursor_source.execute(f"SELECT sessionId, affect, affect_binary, context, "
                                      f"time_of_engagement, time_of_completion FROM {table};")
                table_data = cursor_source.fetchall()

                cursor_dest.executemany(f"INSERT INTO {table} (sessionId, affect, affect_binary, context, "
                                        f"time_of_engagement, time_of_completion) VALUES (?, ?, ?, ?, ?, ?)",
                                        table_data)

            elif table == 'SessionData':
                cursor_source.execute(f"SELECT startTimeMillis, endTimeMillis FROM {table};")
                table_data = cursor_source.fetchall()

                cursor_dest.executemany(f"INSERT INTO {table} (startTimeMillis, endTimeMillis) VALUES (?, ?)", table_data)

        conn_source.close()

    conn_dest.commit()
    conn_dest.close()

    print('Tables copied successfully.')


def process_hr(destination_db):
    '''
    Adjust hrIbi and generate hrIbi_quality for more data
    '''
    conn = sqlite3.connect(destination_db)
    cursor = conn.cursor()

    # Fetch hrIbi data
    cursor.execute("SELECT hrIbi FROM HeartRateMeasurement")
    data = cursor.fetchall()

    # Adjust hrIbi
    adjusted_data = [((hrIbi & 0x7FFF), ((hrIbi >> 15) & 0x21)) for (hrIbi,) in data]

    # Add transformed data
    cursor.executemany("UPDATE HeartRateMeasurement SET hrIbi = ?, hrIbi_quality = ?", adjusted_data)

    conn.commit()
    cursor.close()

    print("hr processed")


db_folder = '../databases'
destination_db = '../Merged_DB.db'

generate_label(db_folder)
'''
add_sessionId takes a lot of computing time. Looping through all the databases and tables to execute SQL commands 
is quite time intensive.
'''
add_sessionId(db_folder)
copy_to_new_db(db_folder, destination_db)
'''
process_hr takes a lot of computing time. Two SQL commands, especially executemany seems to be the bottleneck.
'''
process_hr(destination_db)

from datetime import datetime
import sqlite3
import os
import statistics
from itertools import groupby


def get_id(item):
    return item[0]


def match_notificationId(affect, context, notification, interactionMeasurement):
    '''
    Making sure to match LableData based on the id of each table
    '''
    matched_data = []
    if not all([affect, context, notification, interactionMeasurement]):
        return []

    for id_val, group_affect in groupby(affect, key=get_id):
        group_context = next(groupby(context, key=get_id))[1]
        group_notification = next(groupby(notification, key=get_id))[1]
        group_interactionMeasurement = next(groupby(interactionMeasurement, key=get_id))[1]

        combined_data = [(id_val, *item1[1:], *item2[1:], *item3[1:], *item4[1:]) for item1, item2, item3, item4 in
                         zip(group_affect, group_context, group_notification, group_interactionMeasurement)]
        matched_data.extend(combined_data)

    return matched_data


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
        cursor.execute("SELECT id, time_of_engagement, time_of_completion FROM InteractionMeasurement")
        interaction_data = cursor.fetchall()
        interaction_data_processed = []
        for tuple in interaction_data:
            time_of_engagement = datetime.fromisoformat(tuple[1]).timestamp()
            time_of_completion = datetime.fromisoformat(tuple[2]).timestamp()
            interaction_data_processed.append((tuple[0], time_of_engagement, time_of_completion))

        cursor.execute("SELECT id, time FROM NotificationData")
        notification_data = cursor.fetchall()
        notification_data_processed = []
        for tuple in notification_data:
            time = datetime.fromisoformat(tuple[1]).timestamp()
            notification_data_processed.append((tuple[0], time))

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
        cursor.execute(f"SELECT notificationId, context FROM ContextData")
        context_data = cursor.fetchall()

        data = match_notificationId(affect_data_processed, context_data, notification_data_processed,
                                    interaction_data_processed)

        label_columns = {
            'LabelData': '"id" INTEGER, "SessionId" INTEGER, "affect" STRING, "affect_binary" INTEGER, "context" '
                         'STRING, "timestamp" INTEGER, "time_of_engagement" INTEGER, "time_of_completion" INTEGER'}
        # Add table to database
        cursor.execute(f"CREATE TABLE IF NOT EXISTS Label ({label_columns['LabelData']})")
        # Define SQL query for inserting data
        insert_query = "INSERT INTO Label (id, SessionId, affect, affect_binary, context, timestamp, time_of_engagement, time_of_completion) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

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
        for table_name in ['HeartRateMeasurement', 'Label']:
            # , 'AccelerometerMeasurement', 'PpgGreenMeasurement'
            cursor.execute(f"SELECT timestamp FROM {table_name}")
            table_timestamp = cursor.fetchall()

            for timestamp in table_timestamp:
                timestamp = timestamp[0]
                for session in session_data:
                    session_id = session[0] + sessionId_hilf
                    start_time = session[1]
                    end_time = session[2]

                    # AKTUELLER STAND
                    if start_time <= timestamp <= end_time:
                        try:
                            cursor.execute(f"UPDATE {table_name} SET sessionId = ?", (session_id,))
                        except sqlite3.Error as e:
                            print(e)
                        conn.commit()

        sessionId_hilf += len(session_data)
        conn.commit()
        conn.close()


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
                        f"'timestamp' INTEGER,"
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
                cursor_source.execute(f"SELECT sessionId, affect, affect_binary, context, timestamp, "
                                      f"time_of_engagement, time_of_completion FROM {table};")
                table_data = cursor_source.fetchall()

                cursor_dest.executemany(f"INSERT INTO {table} (sessionId, affect, affect_binary, context, timestamp, "
                                        f"time_of_engagement, time_of_completion) VALUES (?, ?, ?, ?, ?, ?, ?)",
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
    Adjust hrIbi and generate hrIbi_quality and hrv for more data
    '''
    conn = sqlite3.connect(destination_db)
    cursor = conn.cursor()

    # Fetch hrIbi data
    cursor.execute("SELECT hrIbi FROM HeartRateMeasurement")
    data = cursor.fetchall()

    # Adjust hrIbi
    adjusted_data = [((hrIbi & 0x7FFF), ((hrIbi >> 15) & 0x1)) for (hrIbi,) in data]

    # Add transformed data
    cursor.executemany("UPDATE HeartRateMeasurement SET hrIbi = ?, hrIbi_quality = ?", adjusted_data)

    hrv_values = []
    for row in data:
        interbeat_intervals = row[1]
        nn_intervals = (float(interval) for interval in interbeat_intervals)
        hrv_value = statistics.stdev(nn_intervals)
        hrv_values.append(hrv_value)
        cursor.execute("UPDATE HeartRateMeasurement SET hrv = ? WHERE id = ?", (hrv_value, row[0]))

    conn.commit()
    cursor.close()


db_folder = '../Study_Data/databases'
destination_db = '../Study_Data/My_DB.db'

generate_label(db_folder)
add_sessionId(db_folder)
copy_to_new_db(db_folder, destination_db)
process_hr(destination_db)

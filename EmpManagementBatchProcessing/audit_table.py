import mysql.connector
import boto3
from datetime import datetime, timedelta


def connect_to_mysql(host, port, user, password, database_name):
    # Connect to MySQL database
    connection = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password
    )
    cursor = connection.cursor()
    try:
        # Execute SQL query to create database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        # Switch to the newly created database
        cursor.execute(f"USE {database_name}")
        return mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database_name
        )  # Return cursor for the new database
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        return None


def create_audit_table(mysql_connection, table_name):
    mysql_cursor = mysql_connection.cursor()

    # Define the audit table creation SQL
    create_audit_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        script_name VARCHAR(255),
        execution_timestamp DATETIME,
        status VARCHAR(20),
        script_type VARCHAR(20),
        file_processed VARCHAR(255),
        details TEXT
    )
    """

    # Create the audit table if it doesn't exist
    mysql_cursor.execute(create_audit_table_sql)


def write_audit_record(mysql_connection, table_name, script_name, status, script_type, file_processed, details):
    mysql_cursor = mysql_connection.cursor()

    # Insert audit record into MySQL
    execution_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    insert_audit_record_sql = f"""
    INSERT INTO {table_name} (script_name, execution_timestamp, status, script_type, file_processed, details)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    # adding all values in a tuple to passing to the currsor
    audit_record = (script_name, execution_timestamp, status, script_type, file_processed, details)
    mysql_cursor.execute(insert_audit_record_sql, audit_record)
    mysql_connection.commit()


def get_last_audit_record(mysql_cursor, table_name):
    # Define the SQL query to retrieve the last audit record
    last_audit_record_sql = f"""
    SELECT status, script_type 
    FROM {table_name}
    ORDER BY execution_timestamp DESC 
    LIMIT 1
    """

    # Execute the SQL query
    mysql_cursor.execute(last_audit_record_sql)

    # Fetch the last audit record
    return mysql_cursor.fetchone()


def get_next_script_to_run(mysql_connection, table_name):
    mysql_cursor = mysql_connection.cursor()

    # Fetch the last audit record
    last_audit_record = get_last_audit_record(mysql_cursor, table_name)

    # Check if a record exists
    if last_audit_record:
        # Extract information from the audit record
        status, script_type = last_audit_record

        # Check if the last run was successful if it is successfull means then next
        #script to run will be surely incremental
        if status == "Success":
            return "Incremental"
        # if it is failed then we have to check the script type also if it is firstload
        # then our script type is first load defaut return type if it is incremental
        # then script type is incremenatal
        elif status == "Failed" and script_type == "Incremental":
            return "Incremental"

    # If no record found or last run failed and it is first load then this return
    # statement is executes
    return "First Load"


# Calculate total no of working days
def calculate_working_days(starting_date, ending_date, weekend_count):
    start_date = starting_date
    end_date = ending_date

    # Initialize counters for Saturdays and Sundays
    saturday_count = 0
    sunday_count = 0

    # Iterate through each date in the time period
    while start_date <= end_date:
        # Check if the current date is Saturday or Sunday
        if start_date.weekday() == 5:  # Saturday
            saturday_count += 1
        elif start_date.weekday() == 6:  # Sunday
            sunday_count += 1
        # Move to the next date
        start_date += timedelta(days=1)

    # Calculate the total count of Saturdays and Sundays
    sat_sun_count = saturday_count + sunday_count
    total_days = (ending_date - starting_date).days + 1
    working_days = total_days - sat_sun_count - weekend_count

    return working_days


def rawToProcessed(source_bucket, source_prefix, destination_bucket, destination_prefix):
    s3  = boto3.client('s3', region_name='us-east-1')
    # List objects in the source folder
    response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)

    # Check if there are any objects to move
    if 'Contents' not in response:
        print("No files found in the source folder.")
        return []

    # Move each file to the destination folder
    moved_files = []
    for obj in response['Contents']:
        # Extract the file name as it has full prefix like source_prefix/myfile.txt
        #so we are splitng '/' we got a list and the element should be the file name
        file_name = obj['Key'].split('/')[-1]
        
        # copy the file we have to provide which bucket we have to copy the file
        # and what the object prefix which we have to key we got it in obj['Key']
        copy_source = {'Bucket': source_bucket, 'Key': obj['Key']}

        # destination key where we have to copy the file which includes the destination
        # prefix and file_name 
        destination_key = destination_prefix + "/" + file_name
        
        # copying the object
        s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_key)

        # Delete the original file
        s3.delete_object(Bucket=source_bucket, Key=obj['Key'])

        # append the file name which is moved to list moved_files
        moved_files.append(file_name)

    # at time of deletion of all file from s3 folder then there is no file in folder due to which
    # s3 removed folder  itself from bucket so recreating it at the end
    # s3.put_object(Bucket=source_bucket, Key=source_prefix)

    return ",".join(moved_files)
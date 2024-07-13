import mysql.connector
from Emp_Com_Sql_helper import *
import time

try:
    # Define MySQL connection parameters
    host = 'bootcamp.cpimkgacy4iw.us-east-1.rds.amazonaws.com'
    port = "3306"
    user = 'vishal'
    password = 'Vishal1997'
    database = 'Emp_Stream'

    # Establish MySQL connection
    connection = connect_to_database(host,port,user,password,database)

    # Enable auto-commit
    connection.autocommit = True
    # Create a cursor object to execute SQL queries
    cursor = connection.cursor(buffered=True)




    while True:
        cursor.execute("SELECT min(id) FROM chat_records")
        last_fetched_id = cursor.fetchone()[0]
    
        if last_fetched_id is not None:

            sql_query = f"SELECT sender,timestamp FROM chat_records WHERE id = {last_fetched_id}"

            # Execute the SQL query
            cursor.execute(sql_query)

            # Fetch the result
            row = cursor.fetchone()
            sender, strike_timestamp = row
            print(row)

            # Update strike count and emp_salary status
            strike_counts = update_strike(cursor, sender, strike_timestamp)
            upd_emp_slry_status(cursor, sender, strike_counts)
            # Run cooldown period if it's one hour
            if is_midnight():
                cooldown_period(cursor)

            # Delete the record of last_fetched_id from chat_records
            delete_query = f"DELETE FROM chat_records WHERE id = {last_fetched_id}"
            cursor.execute(delete_query)

            # Commit changes for each iteration
            connection.commit()

            

        else:
            if is_midnight():
                cooldown_period(cursor)
            print("sleep for 2 minutes")
            time.sleep(120)
            # Reinitialize connection and cursor after sleep
            cursor.close()
            connection.close()
            connection = connect_to_database(host,port,user,password,database)
            cursor = connection.cursor(buffered=True)

except mysql.connector.Error as error:
    print("Error occurred:", error)

finally:
    # Close cursor and connection when done or in case of error
    if 'connection' in locals():
        cursor.close()
        connection.close()

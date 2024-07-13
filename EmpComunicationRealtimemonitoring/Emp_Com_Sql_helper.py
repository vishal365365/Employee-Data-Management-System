from datetime import datetime   
import mysql.connector


def connect_to_database(host,port,user,password,database):
    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )

def update_strike(cursor, emp_id, last_strike_time):
    # Increment the strike count or insert a new record if it doesn't exist,
    # and return the current strike count
    cursor.execute("""
    INSERT INTO emp_slry_strike_cnt (emp_id, strike_cnt)
    VALUES (%s, 1)
    ON DUPLICATE KEY UPDATE
    strike_cnt = strike_cnt + 1
    """, (emp_id,))


    # maintaing history for strike time for an emp for cooldown period
    cursor.execute(""" 
    INSERT INTO emp_strk_time_history (emp_id, strike_time) 
    VALUES (%s, %s)
    """,(emp_id,last_strike_time))


    cursor.execute("SELECT strike_cnt FROM emp_slry_strike_cnt WHERE emp_id = %s", (emp_id,))
    strike_count = cursor.fetchone()[0]
    return strike_count



def upd_emp_slry_status(cursor, emp_id, strike_count):

    # Update the salary if the strike count is less than 10 and updating the strk_slry also
    if strike_count < 10:
        cursor.execute("""
            UPDATE emp_slry_strike_cnt
            SET active_salary = salary * POWER(0.9, %s),
            strike_%s_salary = salary * POWER(0.9, %s)
            WHERE emp_id = %s
        """, (strike_count,strike_count,strike_count,emp_id))
    # Update the status to 'INACTIVE' if the strike count is 10 or more
        
    else:
        
        cursor.execute("""
            UPDATE emp_slry_strike_cnt
            SET status = 'INACTIVE'
            WHERE emp_id = %s
        """, (emp_id))
        


def is_midnight():
    now = datetime.now()
    if now.hour == 0 and ( now.minute >= 0 and now.minute <= 15) :
        return True
    return False
def cooldown_period(cursor):

    # removing those emp who has completed 9 strikes as they are not part of 
    # cooldown process
    

    create_temp_table_query = """
        CREATE TEMPORARY TABLE temp_emp_ids AS
        SELECT emp_id
        FROM emp_strk_time_history
        GROUP BY emp_id
        HAVING COUNT(*) = 10
        """
    cursor.execute(create_temp_table_query)

     # Delete records from emp_strk_time_history where emp_id matches those in temp table
    delete_query = """
    DELETE FROM emp_strk_time_history
    WHERE emp_id IN (
    SELECT emp_id
    FROM temp_emp_ids
        )
        """
    cursor.execute(delete_query)
    

    select_query = """
    SELECT emp_id, strike_time 
    from emp_strk_time_history
    WHERE DATE(last_strike_time) <= NOW() - INTERVAL '30 days'
    """

    cursor.execute(select_query)
    results = cursor.fetchall()

    # Update the records based on retrieved data
    for emp_id,last_strike_time in results:

        # fetching latest_slry_strike_cnt from table emp_slry_strike_cnt

        cursor.execute("""
                    select strike_cnt
                    from emp_slry_strike_cnt
                    where emp_id = %s
                    """,(emp_id))
        
        strike_cnt = cursor.fetchone()
        strike_count_column = f"strike_{strike_cnt}_salary"
        update_query = f"""
        UPDATE emp_slry_strike_cnt
        SET 
        strike_cnt = strike_cnt - 1,
        latest_salary = latest_salary * (1/0.9),
        {strike_count_column} = 0
        WHERE emp_id = %s;
        """
        cursor.execute(update_query, (emp_id,))


        # delete processed record from emp_strk_time_history which is 
        # coming under cool down 

        delete_query = """
        DELETE FROM emp_strk_time_history
        WHERE emp_id = %s AND strike_time = %s
        """
        cursor.execute(delete_query, (emp_id,last_strike_time))
        

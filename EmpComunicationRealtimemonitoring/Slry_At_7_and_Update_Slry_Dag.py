import mysql.connector
from mysql.connector import Error
from airflow import DAG
from init import db
from time import sleep
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['vishaljoshi753@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# --------------------------------------------------------------------
# Function to drop, create, and insert data into the table
def salary_strike_count_at_7():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host=db['host'],
            user='vishal',
            password='Vishal1997',
            database='Emp_Stream'
        )

        
        cursor = connection.cursor()
        # Drop table if exists
        drop_table_query = "DROP TABLE IF EXISTS salary_and_strike_cnt_at_7;"
        cursor.execute(drop_table_query)
        # Create table
        create_table_query = """
        CREATE TABLE salary_and_strike_cnt_at_7 (
            emp_id BIGINT,
            active_salary BIGINT,
            strike_cnt INT
        );
        """
        cursor.execute(create_table_query)
        # Insert data from emp_slry_strike_cnt
        insert_data_query = """
        INSERT INTO salary_and_strike_cnt_at_7 (emp_id,active_salary,strike_cnt)
        SELECT emp_id, active_salary,strike_cnt
        FROM emp_slry_strike_cnt;
        """
        cursor.execute(insert_data_query)
        # Commit the transaction
        connection.commit()
    except Error as e:
        print(f"Error: {e}")
        raise e
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

            
# --------------------------------------------------------------------------------------

def update_salary_helper(cursor,total_records):
    for records in total_records:
        emp_id,designation,salary = records
        print(records)

        #fetching result from table salary_and_strike_cnt_at_7 
        cursor.execute("""
                        select active_salary,strike_cnt
                        from salary_and_strike_cnt_at_7
                        where emp_id = %s
                        """
                    ,(emp_id,))
        
        salary_at_7records = cursor.fetchone()

        if salary_at_7records is not None:
            salary_at_7,strike_cnt = salary_at_7records

        # if there is emp whoes entry is not in emp_slry_strike_cnt it means
        # its entry will also not in salary_at_7,strike_cnt this can be only
        # be happen if emp is new and came after 7 am and he did not send any
        # flaged messages
        else:
            salary_at_7,strike_cnt = None,None


  # if salary == salary_at_7 it means the emp has not got any update other
        # emp Emp_Communication system . which means his salary has changes only
        # due to strikes so which we are already has updated data on our 
        # emp_slry_strike_cnt so no need to update
# ---------------------------------------------------------------------------
        if salary == salary_at_7:
            continue
#--------------------------------------------------------------------------




    # if salary_at_7 == 1 it means the emp entry is not in emp_slry_strike_cnt table
    # because the emp comes after 7 of previous day so his entry is not emp time frame
    # incremental file but the as he is an active emp so he used chat system and send
    # flagged messages so his entry is made of in emp_slry_strike_cnt table but as there
    # is not any information about his salary it is default set to 1
#--------------------------------------------------------------------------------
        elif salary_at_7 == 1:

            update_query = """
                    UPDATE emp_slry_strike_cnt
                    SET designation = %s, salary = %s
                    WHERE emp_id = %s
                    """
             # updating salary and designation
            cursor.execute(update_query, (designation, salary, emp_id))
             #fetching latest strike from table emp_slry_strike_cnt
            
            # fetching latest strike cnt from emp_slry_strike_cnt
            cursor.execute("""
                        select strike_cnt
                        from emp_slry_strike_cnt
                        where emp_id = %s
                        """
                    ,(emp_id,))
            
            latest_slry_strk_cnt = cursor.fetchone()
           
            #updating active salary and strike_cnt_salary
            for i in range(1,latest_slry_strk_cnt+1):
                update_strike_query = f"""
                        UPDATE emp_slry_strike_cnt
                        SET active_salary = {salary} * POWER(0.9, {i}),
                            strike_{i}_salary = {salary} * POWER(0.9, {i})
                        WHERE emp_id = %s
                        """
                cursor.execute(update_strike_query, (emp_id,))



#----------------------------------------------------------------------------------


    # if salary_at_7 == NONE it means the emp entry is not in emp_slry_strike_cnt table
    # because the emp comes after 7 of previous day so his entry is not emp time frame
    # incremental file and he also not sends any flagged messages so entry is not made
    # in emp_slry_strike_cnt table 
# ----------------------------------------------------------------------------------
        elif salary_at_7 == None:
            #fetching latest_slry_strike_cnt from table emp_slry_strike_cnt

            cursor.execute("""
                        select strike_cnt
                        from emp_slry_strike_cnt
                        where emp_id = %s
                        """
                    ,(emp_id,))
            latest_slry_strk_cnt = cursor.fetchone()

            #as salary_and_strike_cnt_at_7 is updated till 7 am if the emp has made
            # any strike when this code is processing . as this code will execute when
            # there is a record in updated salary table which will created when 
            # incremental emp time frame script is executed so again checking latest 
            # strike cnt from emp_slry_strike_cnt if got any update then this if
            # block will execute
            if latest_slry_strk_cnt is not None:
                strike_cnt = latest_slry_strk_cnt[0]
                update_query = """
                    UPDATE emp_slry_strike_cnt
                    SET designation = %s, salary = %s
                    WHERE emp_id = %s
                    """

                # updating salary and designation
                cursor.execute(update_query, (designation, salary, emp_id))
                #updating active salary and strike_cnt_salary
                for i in range(1,strike_cnt+1):
                    update_strike_query = f"""
                        UPDATE emp_slry_strike_cnt
                        SET active_salary = {salary} * POWER(0.9, {i}),
                            strike_{i}_salary = {salary} * POWER(0.9, {i})
                        WHERE emp_id = %s
                        """
                    cursor.execute(update_strike_query, (emp_id,))

            else:
                # If stil it is none then new entry is made on the emp_strike_cnt_table
                insert_query = """
                        INSERT INTO emp_slry_strike_cnt (emp_id, designation, salary, active_salary, strike_cnt)
                        VALUES (%s, %s, %s, %s, 0)
                        """
                cursor.execute(insert_query, (emp_id, designation, salary, salary))



# -----------------------------------------------------------------------------------
                    


    # this will execute if there is change in  salary in emp time frame incremental file
    # due to designation and strike so here we have to remove salary deduction due to
    # strike. we can achive this by salary = salary // (0.9 ** strike_cnt) we will
    # get salary update on the base of designation only now we will update salary,
    # active salary and strike cnt salary on the basis of updated salary
    

#-------------------------------------------------------------------------------------
            

        else:
            # restoring salary removing strikes from salary getting that salary which
            # is increased on the basis of designation
            salary = salary // (0.9 ** strike_cnt) 
            update_query = """
                    UPDATE emp_slry_strike_cnt
                    SET designation = %s, salary = %s,active_salary = %s
                    WHERE emp_id = %s
                    """
            
            # updating salary and designation
            cursor.execute(update_query, (designation, salary,salary, emp_id))

            #fetching latest_slry_strike_cnt from table emp_slry_strike_cnt
            cursor.execute("""
                        select strike_cnt
                        from emp_slry_strike_cnt
                        where emp_id = %s
                        """
                    ,(emp_id,))
            latest_slry_strk_cnt = cursor.fetchone()
            #updating active salary and strike_cnt_salary
            for i in range(1,strike_cnt+1):
                update_strike_query = f"""
                    UPDATE emp_slry_strike_cnt
                    SET active_salary = {salary} * POWER(0.9, {i}),
                    strike_{i}_salary = {salary} * POWER(0.9, {i})
                    WHERE emp_id = %s
                        """
                cursor.execute(update_strike_query, (emp_id,))


        
        cursor.execute("DELETE FROM updated_salary WHERE emp_id = %s",(emp_id,))
# ----------------------------------------------------------------------------------------
        

# ---------------------------------------------------------------------------------
def update_salary():
    try:
        # Connect to the MySQL database
        connection = mysql.connector.connect(
            host=db['host'],
            user='vishal',
            password='Vishal1997',
            database='Emp_Stream')
        cursor = connection.cursor()
        
        # initially setting reuslt to none
        result = None
        
        while result is None:
            # Sleep for a while before checking again
            print("going for sleep")
            sleep(120)

            # checking updated salary audit table latest record status 
            # if got status succeed then we will update salary otherwise raise exception

            last_audit_record_sql = f"""
            SELECT status FROM Audit.updated_salary where 
            DATE(execution_timestamp) = DATE(NOW())
            """

            # Execute the SQL query
            cursor.execute(last_audit_record_sql)

            # Fetch the last audit record
            result = cursor.fetchone()
        else:
            status = result[0]
            if status == "Success":
                cursor.execute("select * from updated_salary")
                total_records = cursor.fetchall()
                update_salary_helper(cursor,total_records)
            else:
                raise Exception("Emp Time Frame script failed")
            
    except Error as e:
        print(f"Error: {e}")
        raise e            
                




# Define the DAG
with DAG(
    'Slry_At_7_and_Update_Slry',
    default_args=default_args,
    description='A DAG to update strike count table using PythonOperator',
    schedule_interval="30 1 * * *",  # At 07:00 AM every day
    start_date=datetime(2024, 6, 17, 1, 30, 0),
    catchup=False,
) as dag:

    salary_strike_count_at_7_task = PythonOperator(
        task_id='salary_strike_count_at_7_task',
        python_callable=salary_strike_count_at_7,
    )

    update_salary_task = PythonOperator(
        task_id='update_salary_task',
        python_callable=update_salary,
    )

    



    salary_strike_count_at_7_task >> update_salary_task

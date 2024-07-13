import boto3
import logging
from airflow import DAG
from init import db
from airflow.operators.python_operator import PythonOperator
from time import sleep
from datetime import timedelta
import mysql.connector
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup':False
}
dag = DAG(
    dag_id="Emp_Communication", default_args=args, schedule_interval=None
)


# Specify configuration to use Glue Data Catalog as the metastore
glue_configuration = [
   { 
     "Classification": "spark-hive-site",
     "Properties": { 
      "hive.metastore.client.factory.class": 
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory" 
      } 
    }
]


aws_access_key_id = ""
aws_secret_access_key = ""

client = boto3.client('emr', region_name='us-east-1'
                      ,aws_access_key_id=aws_access_key_id
                      ,aws_secret_access_key=aws_secret_access_key)

def create_emr_cluster():
  cluster_id = client.run_job_flow(
    Name="Employe_Communication",
    Instances={
    'InstanceGroups': [
    {
    'Name': "Master",
    'Market': 'ON_DEMAND',
    'InstanceRole': 'MASTER',
    'InstanceType': 'm5.xlarge',
    'InstanceCount': 1,
    },
    {
    'Name': "Slave",
    'Market': 'ON_DEMAND',
    'InstanceRole': 'CORE',
    'InstanceType': 'm5.xlarge',
    'InstanceCount': 1,
    }
    ],
    'Ec2KeyName': 'vishal',
    'KeepJobFlowAliveWhenNoSteps': True,
    'TerminationProtected': False,
    'Ec2SubnetId': 'subnet-091f4ae165f5a40aa',
    },
    LogUri="s3://bootcamp-emp-workflow-bronze/sds/",
    ReleaseLabel= 'emr-6.2.1',
    VisibleToAllUsers=True,
    JobFlowRole="EMR_EC2_DefaultRole",
    ServiceRole="EMR_DefaultRole",
    Applications = [ {'Name': 'Spark'},{'Name':'Hive'}],
    Configurations = glue_configuration
    )
    
  print(f"The cluster started with cluster id : {cluster_id}")
  return cluster_id


  
def add_step_emr(cluster_id,step_name,jar_file,step_args):
  print("The cluster id : {}".format(cluster_id))
  print("The step to be added : {}".format(step_args))
  response = client.add_job_flow_steps(
  JobFlowId=cluster_id,
  Steps=[
  {
    'Name': step_name,
    'ActionOnFailure':'CONTINUE',
    'HadoopJarStep': {
  'Jar': jar_file,
  'Args': step_args
  }
  }
  ]
  )
  print("The emr step is added")
  return response['StepIds'][0]
  
def get_status_of_step(cluster_id,step_id):
  response = client.describe_step(
    ClusterId=cluster_id,
    StepId=step_id
  )
  return response['Step']['Status']['State']
  
  
def wait_for_step_to_complete(cluster_id,step_id):
  print("The cluster id : {}".format(cluster_id))
  print("The emr step id : {}".format(step_id))
  while True:
    try:
      status=get_status_of_step(cluster_id,step_id)
      if status =='COMPLETED':
        break
      elif status in ['RUNNING','PENDING']:
        print("The step is {}".format(status))
        sleep(40)
      else:
        raise Exception("Unexpected status: {}".format(status))

    except Exception as e:
      logging.info(e)
      raise

# --------------------------------------------------------------------------
def create_database_and_tables(host, port, user, password, database_name):
    # Establish MySQL connection
    connection = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
    )
    # Create a cursor object to execute SQL queries
    cursor = connection.cursor(buffered=True)

    # Create database if not exists
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    # Use database
    cursor.execute(f"USE {database_name}")

    # Creating tables
    table_create_for_chat_records = """
    CREATE TABLE IF NOT EXISTS chat_records (
        id INT AUTO_INCREMENT PRIMARY KEY,
        sender BIGINT NOT NULL,
        is_flagged ENUM('Yes', 'No') NOT NULL,
        timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        batch_id INT NOT NULL
    );"""
    cursor.execute(table_create_for_chat_records)

    table_create_for_emp_slry_strike_cnt = """ 
    CREATE TABLE IF NOT EXISTS emp_slry_strike_cnt (
        emp_id BIGINT PRIMARY KEY,
        designation VARCHAR(20) NOT NULL DEFAULT 'A',
        salary BIGINT NOT NULL DEFAULT 1,
        status ENUM('ACTIVE', 'INACTIVE') NOT NULL DEFAULT 'ACTIVE',
        active_salary BIGINT NOT NULL DEFAULT 1,
        strike_cnt INT NOT NULL,
        strike_1_salary BIGINT NOT NULL DEFAULT 0,
        strike_2_salary BIGINT NOT NULL DEFAULT 0,
        strike_3_salary BIGINT NOT NULL DEFAULT 0,
        strike_4_salary BIGINT NOT NULL DEFAULT 0,
        strike_5_salary BIGINT NOT NULL DEFAULT 0,
        strike_6_salary BIGINT NOT NULL DEFAULT 0,
        strike_7_salary BIGINT NOT NULL DEFAULT 0,
        strike_8_salary BIGINT NOT NULL DEFAULT 0,
        strike_9_salary BIGINT NOT NULL DEFAULT 0
    );
    """
    cursor.execute(table_create_for_emp_slry_strike_cnt)

    table_create_for_updated_salary = """ 
    CREATE TABLE IF NOT EXISTS updated_salary (
        emp_id BIGINT,
        designation VARCHAR(20) NOT NULL,
        salary BIGINT NOT NULL
    );
    """
    cursor.execute(table_create_for_updated_salary)

    table_create_for_emp_strk_time_history = """
    CREATE TABLE IF NOT EXISTS emp_strk_time_history (
      emp_id BIGINT,
      strike_time TIMESTAMP
    );
    """
    cursor.execute(table_create_for_emp_strk_time_history)

    view_create_for_emp_strike_count ="""
    CREATE OR REPLACE VIEW emp_strike_count AS
    SELECT emp_id, strike_cnt
    FROM emp_slry_strike_cnt;"""
    cursor.execute(view_create_for_emp_strike_count)

    view_create_for_emp_salary ="""
    CREATE OR REPLACE VIEW emp_salary AS
    SELECT emp_id, designation, salary, active_salary
    FROM emp_slry_strike_cnt;"""
    cursor.execute(view_create_for_emp_salary)

    # Close cursor and connection
    cursor.close()
    connection.close()

# -----------------------------------------------------------------
    
def check_record_count(host, port, user, password, database_name):
    # Establish MySQL connection
  connection = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
    )
  # Create a cursor object to execute SQL queries
  cursor = connection.cursor(buffered=True)
  # Use database
  cursor.execute(f"USE {database_name}")
  while True:
    # Execute query to count records in emp_slry_strike_cnt
    cursor.execute("SELECT COUNT(*) FROM emp_slry_strike_cnt")
    record_count = cursor.fetchone()[0]
    connection.commit()
    # If records found, break the loop
    if record_count > 0:
      logging.info("Records found.")
      break
    else:
      logging.info("No records found. Waiting for records...")
      
    # Sleep for a while before checking again
    sleep(120) 
  # Close cursor and connection
  cursor.close()
  connection.close()





with dag:
  create_tables_task = PythonOperator(
        task_id='create_tables_task',
        python_callable=create_database_and_tables,
        op_args=[db['host'], '3306',
                  'vishal','Vishal1997','Emp_Stream']
    )
  
  check_record = PythonOperator(
      task_id='check_record_task',
      python_callable=check_record_count,
      op_args=[db['host'], '3306',
                  'vishal','Vishal1997','Emp_Stream']
    )
  
  create_emr_cluster3 = PythonOperator(
  task_id='create_emr_cluster3',
  python_callable=create_emr_cluster,
  dag=dag, 
  )
  
  Employe_Streaming = PythonOperator(
  task_id='Employe_Streaming',
  python_callable=add_step_emr,
  op_args=['{{ ti.xcom_pull("create_emr_cluster3")["JobFlowId"]}}',"Emp_Stream",
           'command-runner.jar',[ 'spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'client','--packages', 'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1',
            '--jars','s3://bootcamp-emp-workflow-bronze/jars/mysql-connector-java-8.0.28.jar',
            's3://bootcamp-emp-workflow-bronze/scripts/Emp_Com_Pyspark.py',
           '--warehouse','s3://bootcamp-emp-workflow-gold/emp-data-warehouse',
           '--host',db['host']
            ]
            ],
  dag=dag, 
  )

  Emp_Streaming_Step_Layer = PythonOperator(
  task_id='Emp_Streaming_Step_Layer',
  python_callable=wait_for_step_to_complete,
  op_args=['{{ ti.xcom_pull("create_emr_cluster3")["JobFlowId"]}}','{{ ti.xcom_pull("Employe_Streaming")}}'],
  dag=dag, 
  )





create_tables_task >> check_record >> create_emr_cluster3 >>\
Employe_Streaming >> Emp_Streaming_Step_Layer

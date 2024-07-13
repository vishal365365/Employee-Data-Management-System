import boto3
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from time import sleep
from datetime import datetime,timedelta
from init import *
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 17, 1, 30, 0),  # Start at 7 am tomorrow
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['vishaljoshi753@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'catchup':False
}
dag = DAG(
    dag_id="EmployeWorkFlow1", default_args=args, schedule_interval="30 1 * * *"
)

# Specify the bootstrap action script
bootstrap_actions = [
    {
        'Name': 'Custom Bootstrap Action',
        'ScriptBootstrapAction': {
            'Path': 's3://bootcamp-emp-workflow-bronze/scripts/bootstrap.sh'
        }
    }
]

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
    Name="Employe_Workflow1",
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
    BootstrapActions=bootstrap_actions,
    VisibleToAllUsers=True,
    JobFlowRole="EMR_EC2_DefaultRole",
    ServiceRole="EMR_DefaultRole",
    Applications = [ {'Name': 'Spark'},{'Name':'Hive'}],
    Configurations = glue_configuration,
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
      raise e
	  

def terminate_cluster(cluster_id):
    try:
        client.terminate_job_flows(JobFlowIds=[cluster_id])
        logger.info("Terminated cluster %s.", cluster_id)
    except ClientError:
        logger.exception("Couldn't terminate cluster %s.", cluster_id)
        raise
		
with dag:
  create_emr_cluster = PythonOperator(
  task_id='create_emr_cluster1',
  python_callable=create_emr_cluster,
  dag=dag, 
  )

  
# ----------------------------------for Emp Time Frame----------------------------------
  EmpTimeFrame = PythonOperator(
  task_id='EmpTimeFrame',
  python_callable=add_step_emr,
  op_args=['{{ ti.xcom_pull("create_emr_cluster1")["JobFlowId"]}}',empTf["stepName"],
           'command-runner.jar',[ 'spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'client','--jars',empTf['jars'],
            empTf["empTfScriptPath"],
            '--host',db["host"],'--port',db["port"],
            '--user',db["user"],'--pass',db["pass"],
            '--table1',empTf['table1'],'--table2',empTf['table2'],
            '--dbcom',empTf['dbcom'],'--etf_raw',empTf["empTfDataPath"]+'{{ds}}',
            "--etf_pre",empTf["etf_pre"]+'{{ds}}',"--etf_pre_prs",empTf["etf_pre_prs"]+'{{ds}}',
            "--bucket",db["bucket"],"--warehouse",db["warehouse"],
            "--etf_table_path",empTf["etf_table_path"]
            ]
            ],
  dag=dag, 
  )
  Emp_Time_Frame_Step_Layer = PythonOperator(
  task_id='Emp_Time_Frame_Step_Layer',
  python_callable=wait_for_step_to_complete,
  op_args=['{{ ti.xcom_pull("create_emr_cluster1")["JobFlowId"]}}','{{ ti.xcom_pull("EmpTimeFrame")}}'],
  dag=dag, 
  )

# ------------------------------for Employee Data ------------------------------------
  EmpData = PythonOperator(
  task_id='EmpData',
  python_callable=add_step_emr,
  op_args=['{{ ti.xcom_pull("create_emr_cluster1")["JobFlowId"]}}',empAndActEmp["stepName1"],
           'command-runner.jar',[ 'spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'client',
            empAndActEmp["EmpScriptPath"],
            '--host',db["host"],'--port',db["port"],
            '--user',db["user"],'--pass',db["pass"],
            '--emp_raw',empAndActEmp["EmpDataPath"]+'{{ds}}',
            '--bucket',db["bucket"],'--emp_pre',empAndActEmp["emp_pre"]+'{{ds}}',
            '--emp_pre_prs',empAndActEmp["emp_pre_prs"]+'{{ds}}',"--warehouse",db["warehouse"]
            ]
            ],
  dag=dag, 
  )

  Emp_Data_Step_Layer = PythonOperator(
  task_id='Emp_Data_Step_Layer',
  python_callable=wait_for_step_to_complete,
  op_args=['{{ ti.xcom_pull("create_emr_cluster1")["JobFlowId"]}}',
           '{{ ti.xcom_pull("EmpData")}}'],
  dag=dag, 
  )
# ------------------------------for Active Emp by Designation
  ActiveEmpByDesg = PythonOperator(
  task_id='ActiveEmpByDesg',
  python_callable=add_step_emr,
  op_args=['{{ ti.xcom_pull("create_emr_cluster1")["JobFlowId"]}}',empAndActEmp["stepName"],
           'command-runner.jar',[ 'spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'client',
            empAndActEmp["ActEmpByDesgScriptPath"],
            '--host',db["host"],'--port',db["port"],
            '--user',db["user"],'--pass',db["pass"],
            "--warehouse",db["warehouse"]
            ]
            ],
  dag=dag, 
  )

  Active_Emp_By_Desg_Step_Layer = PythonOperator(
  task_id='Active_Emp_By_Desg_Step_Layer',
  python_callable=wait_for_step_to_complete,
  op_args=['{{ ti.xcom_pull("create_emr_cluster1")["JobFlowId"]}}',
           '{{ ti.xcom_pull("ActiveEmpByDesg")}}'],
  dag=dag, 
  )

  
  terminate_emr_cluster = PythonOperator(
  task_id='terminate_emr_cluster',
  python_callable=terminate_cluster,
  op_args=['{{ ti.xcom_pull("create_emr_cluster1")["JobFlowId"]}}'],
  dag=dag, 
  )

  

create_emr_cluster >> [EmpTimeFrame, EmpData]  # Both tasks run in parallel after 'start'
EmpTimeFrame >> Emp_Time_Frame_Step_Layer >> ActiveEmpByDesg >> Active_Emp_By_Desg_Step_Layer
EmpData >> Emp_Data_Step_Layer
# The final_task depends on both Active_Emp_By_Desg_Step_Layer and Emp_Data_Step_Layer
Active_Emp_By_Desg_Step_Layer >> terminate_emr_cluster
Emp_Data_Step_Layer >> terminate_emr_cluster

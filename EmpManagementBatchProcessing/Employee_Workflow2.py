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
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['vishaljoshi753@gmail.com'],
    'catchup':False
}
dag = DAG(
    dag_id="EmployeWorkFlow2", default_args=args, schedule_interval="30 1 * * *"
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
    Name="Employe_Workflow2",
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
      raise

def terminate_cluster(cluster_id):
    try:
        client.terminate_job_flows(JobFlowIds=[cluster_id])
        logger.info("Terminated cluster %s.", cluster_id)
    except ClientError:
        logger.exception("Couldn't terminate cluster %s.", cluster_id)
        raise
		
with dag:
  create_emr_cluster = PythonOperator(
  task_id='create_emr_cluster',
  python_callable=create_emr_cluster,
  dag=dag, 
  )
# ----------------------------------for leave Quota and calender-------------------------
  LeaveQuotaAndCalender = PythonOperator(
  task_id='LeaveQuotaAndCalender',
  python_callable=add_step_emr,
  op_args=['{{ ti.xcom_pull("create_emr_cluster")["JobFlowId"]}}',lvCaQt["stepName"],
           'command-runner.jar',[ 'spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'client',
            lvCaQt["lvcaqtScriptPath"],
            '--host',db["host"],'--port',db["port"],
            '--user',db["user"],'--pass',db["pass"],
            '--quota_raw',lvCaQt["leaveQutaDataPath"]+'{{ ds[:4] }}',
            '--calender_raw',lvCaQt["leaveCalenderDataPath"]+'{{ ds[:4] }}',
            "--lq_pre",lvCaQt["lq_pre"]+'{{ ds[:4] }}',
            "--lc_pre",lvCaQt["lc_pre"]+'{{ ds[:4] }}',
            "--lq_pre_prs",lvCaQt["lq_pre_prs"]+'{{ ds[:4] }}',
            "--lc_pre_prs",lvCaQt["lc_pre_prs"]+'{{ ds[:4] }}',
            "--bucket",db["bucket"],"--warehouse",db["warehouse"]
            ]
            ],
  dag=dag, 
  )
  Leave_Quota_Calender_Step_Layer = PythonOperator(
  task_id='Leave_Quota_Calender_Step_Layer',
  python_callable=wait_for_step_to_complete,
  op_args=['{{ ti.xcom_pull("create_emr_cluster")["JobFlowId"]}}','{{ ti.xcom_pull("LeaveQuotaAndCalender")}}'],
  dag=dag, 
  )
# ------------------------------for leave data script
  LeaveData = PythonOperator(
  task_id='LeaveData',
  python_callable=add_step_emr,
  op_args=['{{ ti.xcom_pull("create_emr_cluster")["JobFlowId"]}}',lvData["stepName"],
           'command-runner.jar',[ 'spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'client',
            lvData["lvDataScriptPath"],
            '--host',db["host"],'--port',db["port"],
            '--user',db["user"],'--pass',db["pass"],
            '--leave_data',lvData["leaveDataPath"]+'{{ds}}',
            '--bucket',db["bucket"],'--ld_pre',lvData["ld_pre"]+'{{ds}}',
            '--ld_pre_prs',lvData["ld_pre_prs"]+'{{ds}}',
            '--warehouse',db["warehouse"],
            '--ld_tb_path',lvData["ld_tb_path"]
            ]
            ],
  dag=dag, 
  )

  Leave_Data_Step_Layer = PythonOperator(
  task_id='Leave_Data_Step_Layer',
  python_callable=wait_for_step_to_complete,
  op_args=['{{ ti.xcom_pull("create_emr_cluster")["JobFlowId"]}}',
           '{{ ti.xcom_pull("LeaveData")}}'],
  dag=dag, 
  )
# ----------------------------leave exceding 8%-------------------------
  LeaveExeceding8per = PythonOperator(
  task_id='LeaveExeceding8per',
  python_callable=add_step_emr,
  op_args=['{{ ti.xcom_pull("create_emr_cluster")["JobFlowId"]}}',lvExdUsed["stepName"],
           'command-runner.jar',[ 'spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'client',
            lvExdUsed["lvExdScriptPath"],
            '--host',db["host"],'--port',db["port"],
            '--user',db["user"],'--pass',db["pass"],
            "--warehouse",db["warehouse"]
            ]
            ],
  dag=dag, 
  )

  Leave_Execed_8per_Step_Layer = PythonOperator(
  task_id='Leave_Execed_8per_Step_Layer',
  python_callable=wait_for_step_to_complete,
  op_args=['{{ ti.xcom_pull("create_emr_cluster")["JobFlowId"]}}',
           '{{ ti.xcom_pull("LeaveExeceding8per")}}'],
  dag=dag, 
  )


  # ----------------------------leave used 80%---------------------------------
  LeaveUsed80per = PythonOperator(
  task_id='LeaveUsed80per',
  python_callable=add_step_emr,
  op_args=['{{ ti.xcom_pull("create_emr_cluster")["JobFlowId"]}}',lvExdUsed["stepName1"],
           'command-runner.jar',[ 'spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'client',
            lvExdUsed["lvUsedScriptPath"],
            '--host',db["host"],'--port',db["port"],
            '--user',db["user"],'--pass',db["pass"],
            "--warehouse",db["warehouse"]
            ]
            ],
  dag=dag, 
  )

  Leave_Used_80per_Step_Layer = PythonOperator(
  task_id='Leave_Used_80per_Step_Layer',
  python_callable=wait_for_step_to_complete,
  op_args=['{{ ti.xcom_pull("create_emr_cluster")["JobFlowId"]}}',
           '{{ ti.xcom_pull("LeaveUsed80per")}}'],
  dag=dag, 
  )
  
  terminate_emr_cluster = PythonOperator(
  task_id='terminate_emr_cluster',
  python_callable=terminate_cluster,
  op_args=['{{ ti.xcom_pull("create_emr_cluster")["JobFlowId"]}}'],
  dag=dag, 
  )

  



create_emr_cluster >> LeaveQuotaAndCalender \
>> Leave_Quota_Calender_Step_Layer >> LeaveData >> Leave_Data_Step_Layer \
>> [LeaveExeceding8per,LeaveUsed80per]
LeaveExeceding8per >> Leave_Execed_8per_Step_Layer
LeaveUsed80per >> Leave_Used_80per_Step_Layer
Leave_Execed_8per_Step_Layer >> terminate_emr_cluster
Leave_Used_80per_Step_Layer >> terminate_emr_cluster

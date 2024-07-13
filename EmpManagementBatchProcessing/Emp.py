from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType,LongType,IntegerType
import sys
sys.path.append('/home/hadoop')
from audit_table import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("EmpData") \
    .config("spark.sql.warehouse.dir", sys.argv[sys.argv.index('--warehouse')+1]) \
    .enableHiveSupport()\
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS Emp_Mangement")
spark.sql("use Emp_Mangement")
# config variables used in spark app
host = sys.argv[sys.argv.index('--host')+1]
port = sys.argv[sys.argv.index('--port')+1]
user = sys.argv[sys.argv.index('--user')+1]
password = sys.argv[sys.argv.index('--pass')+1]

# defining schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age",IntegerType(),True),
    StructField("emp_id", LongType(), True)
])
# creating connection with DB for creting audit table
mysql_connection = connect_to_mysql(host,port,user,password,"Audit")

# creating audit table for emp if not exists
create_audit_table(mysql_connection,"emp")

#from audit table finding current run is first load or incremental
type_of_script = get_next_script_to_run(mysql_connection,"emp")

emp_data_location = sys.argv[sys.argv.index('--emp_raw')+1]
bucket = sys.argv[sys.argv.index('--bucket')+1]
emp_pre = sys.argv[sys.argv.index('--emp_pre')+1]
emp_pre_prs = sys.argv[sys.argv.index('--emp_pre_prs')+1]



if type_of_script == "First Load":
    try:
        # Read raw data from S3
        emp = spark.read.csv(emp_data_location,schema=schema ,header=True)
        #there are such emp records like  5897722297 present which repeate twice 
        emp = emp.dropDuplicates(["emp_id"])
        #writing the data
        emp.write.format("parquet").mode("overwrite")\
            .saveAsTable("emp")
        # moving raw data to processed and getting file names
        file_name = rawToProcessed(bucket,emp_pre,bucket,emp_pre_prs)

        #adding entry in audit table that data processed successfully
        write_audit_record(mysql_connection,"emp"
                           ,"Active_Employee_by_Designation.py",
                           "Success","First Load",file_name,"successfully processed")
    except Exception as e:
        # if any Exception comes adding that entry in audit table
        write_audit_record(mysql_connection,"emp"
                           ,"Active_Employee_by_Designation.py",
                           "Failed","First Load","",str(e))
        # after logging Exception raising it again
        raise e
else:
    try:
        # Read raw data from S3
        emp = spark.read.csv(emp_data_location,schema=schema ,header=True)
        #droping those emp id which repeated more than one if exists
        emp = emp.dropDuplicates(["emp_id"])
        emp.write.format("parquet").mode("append")\
            .saveAsTable("emp")
        # moving raw data to processed and getting file names
        file_name = rawToProcessed(bucket,emp_pre,bucket,emp_pre_prs)

        #adding entry in audit table that data processed successfully
        write_audit_record(mysql_connection,"emp"
                       ,"Active_Employee_by_Designation.py",
                       "Success","Incremental",file_name,"successfully processed")
    except Exception as e:
            # if any Exception comes adding that entry in audit table
            write_audit_record(mysql_connection,"emp"
                           ,"Active_Employee_by_Designation.py",
                           "Failed","Incremental","",str(e))
            # after logging Exception raising it again
            raise e
        

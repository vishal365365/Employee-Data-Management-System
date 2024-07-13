from pyspark.sql import SparkSession
import sys
sys.path.append('/home/hadoop')
from audit_table import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ActiveEmpByDesignationAndEmploading") \
    .config("spark.sql.warehouse.dir", sys.argv[sys.argv.index('--warehouse')+1]) \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")\
    .enableHiveSupport()\
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS Emp_Mangement")
spark.sql("use Emp_Mangement")
# config variables used in spark app
host = sys.argv[sys.argv.index('--host')+1]
port = sys.argv[sys.argv.index('--port')+1]
user = sys.argv[sys.argv.index('--user')+1]
password = sys.argv[sys.argv.index('--pass')+1]
# ---------------------------active emp by designation-------------

# creating connection with DB for creting audit table
mysql_connection = connect_to_mysql(host,port,user,password,"Audit")

# creating audit table for active_emp_by_designation if not exists
create_audit_table(mysql_connection,"active_emp_by_designation")

# this table donot require first load and incremental check so skiping that step
try:
    # loading data from emp_time_frame
    emp_time_frame = spark.sql("select * from emp_time_frame")
    #filtring active users (partition puruning)
    active_users = emp_time_frame.filter(emp_time_frame.status == 'ACTIVE')\
                    .select("emp_id",'designation','status')
    #grouping by designation
    active_emp_by_designation = active_users.groupBy("designation").count()

    #writing data 
    active_emp_by_designation.write.format("parquet").partitionBy("designation")\
        .mode("overwrite").saveAsTable("active_emp_by_desgination")
    
    #adding entry in audit table coloumn that data processed successfully
    write_audit_record(mysql_connection,"active_emp_by_designation",
                       "active_Employee_by_Designation.py",
                       "Success","First Load","","successfully processed")

except Exception as e:
    # if any Exception comes adding that entry in audit table
    write_audit_record(mysql_connection,"active_emp_by_designation",
                       "active_Employee_by_Designation.py",
                       "Failed","First Load","",str(e))
    raise e

        





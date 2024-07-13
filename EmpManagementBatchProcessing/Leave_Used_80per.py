from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count
from datetime import datetime
import sys
sys.path.append('/home/hadoop')
from audit_table import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("LeaveUsed80%") \
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


# creating connection with DB for creting audit table
mysql_connection = connect_to_mysql(host,port,user,password,"Audit")

# creating audit table for leave_used_eighty if not exists
create_audit_table(mysql_connection,"leave_used_eighty")
type_of_script = get_next_script_to_run(mysql_connection,"leave_used_eighty")


# this will run only at first load
if  type_of_script == "First Load" :
    current_year = datetime.now().year
    ending_date = datetime.now()
    ending_month = ending_date.month
    starting_month = 1
    

    try:
        leave_data = spark.sql("select * from leave_data")
        #filtring on the basis of year,month and status where year,month and status
        #are partioned column
        leave_data = leave_data.filter((leave_data["year"] == current_year)
                               & (leave_data['status'] == 'ACTIVE')
                               & (leave_data["month"]>= starting_month)
                               & (leave_data["month"] <= ending_month))\
                            .select("emp_id","date")
        
        # As future leave data will also come in curent month so have to filter till current_date
        # not required to add starting date in filter as data will come from 1st day of jan of current
        # year
        leave_data = leave_data.filter(leave_data['date'] <= ending_date)

        #counting total active leave an emp have till current_date
        leave_data = leave_data.groupBy("emp_id").agg(count("*").alias("avialed_leaves"))
        #getting leave Quota 
        leave_quota = spark.sql(f"select * from leave_quota where year = {current_year}")

        # joining leave_quota and leave_data on key emp_id
        joined_data = leave_quota.join(leave_data,on="emp_id",how="inner")

        # filtring those emp who exceeded leave quota more than 80%
        leave_used_eighty = joined_data.select("emp_id",(col("avialed_leaves")/col("leave_quota")*100)
                .alias("percent")).filter("percent>80")
        
        #writing data
        leave_used_eighty.write.format("parquet")\
            .mode("overwrite").saveAsTable("leave_used_eighty")
        
        #adding entry in audit table coloumn that data processed successfully
        write_audit_record(mysql_connection,"leave_used_eighty"
                           ,"leave_used_80per.py",
                           "Success","First Load","","processed successfully")
        
            
    except Exception as e:

        # if any Exception comes adding that entry in audit table
        write_audit_record(mysql_connection,"leave_used_eighty"
                           ,"leave_used_80per.py",
                           "Failed","First Load","",str(e))
        # after logging Exception raising it again
        raise e
else:
    if  datetime.now().day == 1:
        current_year = datetime.now().year
        ending_date = datetime.now()
        ending_month = ending_date.month
        starting_month = 1
        try:
            leave_data = spark.sql("select * from leave_data")
             #filtring on the basis of year,month and status where year,month and status
            #are partioned column
            leave_data = leave_data.filter((leave_data["year"] == current_year)
                               & (leave_data['status'] == 'ACTIVE')
                               & (leave_data["month"]>= starting_month)
                               & (leave_data["month"] <= ending_month))\
                            .select("emp_id","date")
            
            # As future leave data of curent month will also come  so have to filter till current_date
            # not required to add starting date in filter as data will come from 1st day of jan of current
            # year
            leave_data = leave_data.filter(leave_data['date'] <= ending_date)

            #counting total active leave an emp have till  current_date
            leave_data = leave_data.groupBy("emp_id").agg(count("*").alias("avialed_leaves"))

            #getting leave Quota cureent year is partitioned column
            leave_quota = spark.sql(f"select * from leave_quota where year = {current_year}")

            # joining leave_quota and leave_data on key emp_id
            joined_data = leave_quota.join(leave_data,on="emp_id",how="inner")

            # filtring those emp who exceeded leave quota more than 80%
            leave_used_eighty = joined_data.select("emp_id",(col("avialed_leaves")/col("leave_quota")*100)
                .alias("percent")).filter("percent>80")
            # writing data
            leave_used_eighty.write.format("parquet")\
            .mode("overwrite").saveAsTable("leave_used_eighty")

            #adding entry in audit table coloumn that data processed successfully
            write_audit_record(mysql_connection,"leave_used_eighty"
                           ,"leave_used_80per.py",
                           "Success","Incremental","","processed successfully")
            
        except Exception as e:
            # if any Exception comes adding that entry in audit table
            write_audit_record(mysql_connection,"leave_used_eighty"
                           ,"leave_used_80per.py",
                           "Failed","Incremental","",str(e))
            #after logging Exception raising it again
            raise e



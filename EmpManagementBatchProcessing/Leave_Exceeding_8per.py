from pyspark.sql import SparkSession
from pyspark.sql.functions import col,count,when
from datetime import datetime
import sys
sys.path.append('/home/hadoop')
from audit_table import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("LeaveExceding8%") \
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
# ---------------------------leave_exceding 8%-------------
# creating connection with DB for creting audit table
mysql_connection = connect_to_mysql(host,port,user,password,"Audit")

# creating audit table for leave_exceding_eight if not exists
create_audit_table(mysql_connection,"leave_exceding_eight")

# this table donot require first load and incremental check so skiping that step
try:
    current_date = datetime.now().date()
    current_year = datetime.now().year
    current_month = current_date.month
    last_date = datetime(current_year, 12, 31).date()
    last_month = 12
    leave_data = spark.sql("select * from leave_data")
    #fetching data on the basis of year and month as the data is partitioned in the form of 
    #year and month fetching only required data (partition puruning)
    leave_data = leave_data.filter((leave_data.year == current_year)
                           & (leave_data.status == 'ACTIVE')
                           & (col("month") >= current_month) 
                           & (col("month") <= last_month))\
         .select("emp_id",'date')
    #filtring total active leave an emp have from current_date not required to add last date in filter 
    # as data will come till last month las date of the current year
    leave_data = leave_data.filter( (col("date") >= current_date))
    # loading weekend table to count working days
    weekends = spark.sql("select * from leave_calender")
    # filtring with year (partition pruning)
    weekends = weekends.filter((weekends["year"] == current_year) &
                       (weekends["date"]>= current_date) &
                       ((weekends["date"]<= last_date)))
    #total weekends in between current date and last date
    weekends_count = weekends.count()
    # total working days between current day and last day of year
    working_days = calculate_working_days(current_date,last_date,weekends_count)
    #counting total active leave an emp have from current_date to end of the year
    leave_data = leave_data.groupBy("emp_id").agg(count("*").alias("leave_count"))
    #finding employee which are  exceding leave more than 8% of
    # working days from present date
    leave_data_exceding_8_percent = leave_data.withColumn("exceding_8%",
                when(((col("leave_count")/working_days)*100)> 8,"yes")
                .otherwise("no"))
    #writing data
    leave_data_exceding_8_percent.write.mode("overwrite").\
    format("parquet").saveAsTable("leave_exceding_8_percent")
    #adding entry in audit table coloumn that data processed successfully
    write_audit_record(mysql_connection,"leave_exceding_eight"
                       ,"leave_exceding_8per.py",
                       "Success","First Load","","processed successfully")
except Exception as e:
    # if any Exception comes adding that entry in audit table
    write_audit_record(mysql_connection,"leave_exceding_eight"
                           ,"leave_exceding_8per.py",
                           "Failed","First Load","",str(e))
    # after logging Exception raising it again
    raise e



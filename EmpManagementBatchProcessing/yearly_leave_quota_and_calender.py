from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id,col,year,date_format,row_number
from pyspark.sql.types import StructType, StructField, DateType,LongType,IntegerType,StringType
from datetime import datetime
import sys
sys.path.append('/home/hadoop')
from audit_table import *
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("YearlyLeaveCalenderAndQuota") \
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
leave_quota_location = sys.argv[sys.argv.index('--quota_raw')+1]
leave_calender_location = sys.argv[sys.argv.index('--calender_raw')+1]
bucket = sys.argv[sys.argv.index('--bucket')+1]
lq_pre = sys.argv[sys.argv.index('--lq_pre')+1]
lc_pre = sys.argv[sys.argv.index('--lc_pre')+1]
lq_pre_prs = sys.argv[sys.argv.index('--lq_pre_prs')+1]
lc_pre_prs = sys.argv[sys.argv.index('--lc_pre_prs')+1]


# ------------------------for leave quota -------------------------

schema = StructType([
    StructField("emp_id", LongType(), True),
    StructField("leave_quota", IntegerType(), True),
    StructField("year", IntegerType(), True)
])

mysql_connection = connect_to_mysql(host,port,user,password,"Audit")
create_audit_table(mysql_connection,"leave_quota")
type_of_script = get_next_script_to_run(mysql_connection,"leave_quota")

if type_of_script == "First Load":
    try:
        leave_quota = spark.read.csv(leave_quota_location, header = True,schema=schema)
        #there are such records like  5897722297 present which repeate twice with 
        #different leave_quota for a year so droping these record on the basis of
        # which is last will remain in list
        leave_quota = leave_quota.withColumn("id", monotonically_increasing_id())
        window = Window.partitionBy("emp_id").orderBy(col("id").desc())
        leave_quota = leave_quota.withColumn("row_number",row_number().over(window))
        leave_quota = leave_quota.filter("row_number == 1")
        leave_quota = leave_quota.select("emp_id","leave_quota","year")
        leave_quota.write.partitionBy("year").format("parquet")\
            .saveAsTable("leave_quota")
        # moving raw data to processed and getting file names
        file_name = rawToProcessed(bucket,lq_pre,bucket,lq_pre_prs)
        write_audit_record(mysql_connection,"leave_quota","yearly_quota_calender",
                           "Success","First Load",file_name,"successfully processed")
        
    except Exception as e:
        write_audit_record(mysql_connection,"leave_quota","yearly_quota_calender"
                           ,"Failed","First Load","",str(e))
else:
    # As this yearly apppend table so for incremental file it will be schecdule 
    #in 1st Jan
    if datetime.today().month == 1 and datetime.today().day == 1:
        try:
            leave_quota = spark.read.csv(leave_quota_location, header = True,schema=schema)
            leave_quota = leave_quota.withColumn("id", monotonically_increasing_id())
            window = Window.partitionBy("emp_id").orderBy(col("id").desc())
            leave_quota = leave_quota.withColumn("row_number",row_number().over(window))
            leave_quota = leave_quota.filter("row_number == 1")
            leave_quota = leave_quota.select("emp_id","leave_quota","year")
            leave_quota.write.partitionBy("year").mode("append").format("parquet")\
            .saveAsTable("leave_quota")
            # moving raw data to processed and getting file names
            file_name = rawToProcessed(bucket,lq_pre,bucket,lq_pre_prs)

            write_audit_record(mysql_connection,"leave_quota","yearly_quota_calender",
                           "Success","Incremental",file_name,"processed successfully")

        except Exception as e:
            write_audit_record(mysql_connection,"leave_quota","yearly_quota_calender"
                           ,"Failed","Incremental","",str(e))


        
# ------------------------------for yearly calender-----------------------
leave_calender_location = leave_calender_location
schema = StructType([
    StructField("reason", StringType(), True),
    StructField("date", DateType(), True)
])

mysql_connection = connect_to_mysql(host,port,user,password,"Audit")
create_audit_table(mysql_connection,"leave_calender")
type_of_script = get_next_script_to_run(mysql_connection,"leave_calender")
print(type_of_script)

if type_of_script == "First Load":
    try:
        leave_calender = spark.read.csv(leave_calender_location, header = True,schema=schema)
        
        leave_calender = leave_calender.withColumn("year",year("date"))\
                        .withColumn("days",date_format("date", "EEEE"))
        # checking if any sat,sun is not present in the data for removing 
        #it from holiday
        leave_calender = leave_calender.filter(~leave_calender.days
                                               .isin(["Saturday","Sunday"]))
        leave_calender = leave_calender.select("reason","date","year")
        leave_calender.write.partitionBy("year").format("parquet")\
            .saveAsTable("leave_calender")
        file_name = rawToProcessed(bucket,lc_pre,bucket,lc_pre_prs)
        write_audit_record(mysql_connection,"leave_calender","yearly_quota_calender",
                           "Success","First Load",file_name,"processed successfully")
    
    except Exception as e:
        write_audit_record(mysql_connection,"leave_calender","yearly_quota_calender"
                           ,"Failed","First Load","",str(e))
else:
    # As this yearly apppend table so for incremental file it will be schecdule 
    #in 1st Jan
    if datetime.today().month == 1 and datetime.today().day == 1:
        try:
            leave_calender = spark.read.csv(leave_calender_location, header = True,schema=schema)
        
            leave_calender = leave_calender.withColumn("year",year("date"))\
                        .withColumn("days",date_format("date", "EEEE"))
            # checking if any sat,sun is not present in the data for removing 
            #it from holiday
            leave_calender = leave_calender.filter(~leave_calender.days
                                               .isin(["Saturday","Sunday"]))
            leave_calender = leave_calender.select("reason","date","year")

            leave_calender.write.partitionBy("year").format("parquet")\
            .saveAsTable("leave_calender")
            file_name = rawToProcessed(bucket,lc_pre,bucket,lc_pre_prs)
            write_audit_record(mysql_connection,"leave_calender","yearly_quota_calender",
                           "Success","Incremental",file_name,"processed successfully")
        except Exception as e:
            write_audit_record(mysql_connection,"leave_calender","yearly_quota_calender"
                           ,"Failed","Incremental","",str(e))
        











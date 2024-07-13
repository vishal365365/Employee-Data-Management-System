from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import year,monotonically_increasing_id,col,lit,month,udf,row_number
from pyspark.sql.types import StructType, StructField, StringType,\
                             DateType,LongType
import sys
sys.path.append('/home/hadoop')
from audit_table import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("LeaveDataFirstandIncremental") \
    .config("spark.sql.warehouse.dir", sys.argv[sys.argv.index('--warehouse')+1]) \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")\
    .config("spark.sql.shuffle.partitions", "8") \
    .enableHiveSupport()\
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS Emp_Mangement")
spark.sql("use Emp_Mangement")
# config variables used in spark app
host = sys.argv[sys.argv.index('--host')+1]
port = sys.argv[sys.argv.index('--port')+1]
user = sys.argv[sys.argv.index('--user')+1]
password = sys.argv[sys.argv.index('--pass')+1]
leave_data_location = sys.argv[sys.argv.index('--leave_data')+1]
bucket = sys.argv[sys.argv.index('--bucket')+1]
ld_pre = sys.argv[sys.argv.index('--ld_pre')+1]
ld_pre_prs = sys.argv[sys.argv.index('--ld_pre_prs')+1]
ld_tb_path = sys.argv[sys.argv.index('--ld_tb_path')+1]

schema = StructType([
    StructField("emp_id", LongType(), True),
    StructField("date", DateType(), True),
    StructField("status", StringType(), True)
])

# creating connection with DB for creting audit table
mysql_connection = connect_to_mysql(host,port,user,password,"Audit")

# creating audit table for leave_data if not exists
create_audit_table(mysql_connection,"leave_data")

#from audit table records finding current run is first load or incremental
type_of_script = get_next_script_to_run(mysql_connection,"leave_data")



# ----------finding holiday list for filtring from leave data--------------------

holiday = spark.sql("select * from leave_calender")

#filtring days
holiday = holiday.select("date")

#changing date in leave_calender dataframe to list of dates
holiday_list = holiday.rdd.map(lambda x: x[0]).collect()

# ------------------------------------------------


#---------------------udf for checking the date in leave  weekends-------------------
def check_for_holiday(date):
    day_name = date.strftime('%A')
    if day_name in ['Saturday', 'Sunday'] or date in holiday_list:
        return "yes"
    return "no"
check_for_holiday_udf = udf(check_for_holiday, StringType())
#-------------------------------------------------------------------------------

if type_of_script == "First Load":
    try:
        # reading the first load data
        raw_data_df = spark.read.csv(leave_data_location, header=True,schema=schema)
        
        raw_data_df = raw_data_df.withColumn("id", monotonically_increasing_id())

        # creating window for selecting last updated leave status for particular
        # date for an emp
        window = Window.partitionBy("emp_id","date").orderBy(col("id").desc())
        raw_data_df = raw_data_df.withColumn("row_number",row_number().over(window))

        #filtring the latest update
        raw_data_df = raw_data_df.filter("row_number == 1")

        #selecting the required columns
        raw_data_df = raw_data_df.select("emp_id","date","status")

        #adding column is_weekend which will give date is in weekend or holiday or not 
        raw_data_df = raw_data_df.withColumn("is_weekend",
                                check_for_holiday_udf(col("date")))
        
        # removing rows which are in weekends and holidays
        raw_data_df = raw_data_df.filter(col("is_weekend") == "no")

        # adding year coloumn
        raw_data_df = raw_data_df.withColumn("year",year(col("date")))

        #adding month coloumn
        raw_data_df = raw_data_df.withColumn("month",month(col("date")))

        raw_data_df = raw_data_df.select("emp_id","date","status","month","year")
        #writing data
        raw_data_df.write.partitionBy("year","month","status")\
                        .format("parquet").saveAsTable("leave_data")
        
        # moving raw data in s3 to processed folder and getting file names
        file_name = rawToProcessed(bucket,ld_pre,bucket,ld_pre_prs)

        #adding entry in audit table coloumn that data processed successfully
        write_audit_record(mysql_connection,"leave_data","Leave_Data",
                           "Success","First Load",file_name,"successfully processed")

    except Exception as e:
        # if any Exception comes adding that entry in audit table
        write_audit_record(mysql_connection,"leave_data","Leave_Data",
                           "Failed","First Load","",str(e))
        # after logging Exception raising it again to stop pipeline
        raise e
        
# ----------------------------------------for incremental----------------------------
else:
    try:
        #reading the incremental data
        new_leave_data = spark.read.csv(leave_data_location, schema = schema,header=True)
        new_leave_data = new_leave_data.withColumn("id", monotonically_increasing_id())

        # creating window for selecting last updated leave status for particular
        # date for an emp
        window = Window.partitionBy("emp_id","date").orderBy(col("id").desc())
        new_leave_data = new_leave_data.withColumn("row_number",row_number().over(window))

        #filtring the latest update
        new_leave_data = new_leave_data.filter("row_number == 1")
        
        #selecting the required columns
        new_leave_data = new_leave_data.select("emp_id","date","status")

        # removing rows which are in weekends and holidays
        new_leave_data = new_leave_data.withColumn("is_weekend",
                                check_for_holiday_udf(col("date")))
        new_leave_data = new_leave_data.filter(col("is_weekend") == "no")
        
        # adding year column
        new_leave_data = new_leave_data.withColumn("year",year(col("date")))

        #adding month column
        new_leave_data = new_leave_data.withColumn("month",month(col("date")))

        #adding flag with value new to identify the record is from new dataset
        new_leave_data  =  new_leave_data.withColumn("flag",lit("new"))

        #finding the distinct year and month from new leave data for getting only
        #those reords which are to be updated from previous leave data
        
        year = new_leave_data.select("year").distinct()\
                .rdd.map(lambda x: x[0]).collect()
        
        month = new_leave_data.select("month").distinct()\
                .rdd.map(lambda x: x[0]).collect()
        
        # partition pruning
        # reading old data from leave_data tables which belongs to only those year 
        # and month which we fetched from new leave data


        previous_leave_data = spark.sql("select * from leave_data")
        previous_leave_data = previous_leave_data\
            .filter((previous_leave_data.year.isin(year)) 
                    & (previous_leave_data.year.isin(month)))\
                    
        #adding flag with value previous to identify the record is from previous dataset
        previous_leave_data =  previous_leave_data.withColumn("flag", lit("previous"))
        
        #column list for removing ambugity during union operation
        coulmn_list = ["emp_id","date","status","year","month","flag"]

        #performing union between old data set and new data set
        combined_leave_data = previous_leave_data.select(*coulmn_list)\
            .union(new_leave_data.select(*coulmn_list))
        
        # creating window for selecting latest updated records 
        window2 = Window.partitionBy("emp_id","date").orderBy(col("flag"))

        #adding row_number
        combined_leave_data = combined_leave_data.\
                            withColumn("row_number",row_number().over(window2))
        
        #filtring the record which have row number 1 . as it is order by flag so 
        # new will come first and got row number 1 and if there is no previous record
        # for emp_id, date then still will have row number 1 and if there are employees
        # in previous leave date which have no update in new data will also get row number
        # so their data will also not be lost

        combined_leave_data = combined_leave_data.filter("row_number == 1")

        #selecting the required coloumns
        combined_leave_data = combined_leave_data.select("emp_id","date","status","month","year")
    
        # writing data
        combined_leave_data.write.partitionBy("year","month","status").format("parquet")\
            .mode("overwrite")\
            .save(ld_tb_path)
        
        # moving raw data to processed and getting file names
        file_name = rawToProcessed(bucket,ld_pre,bucket,ld_pre_prs)


        #adding entry in audit table coloumn that data processed successfully
        write_audit_record(mysql_connection,"leave_data","Leave_Data",
                           "Success","Incremental",file_name,"successfully processed")

    except Exception as e:
         # if any Exception comes adding that entry in audit table
        write_audit_record(mysql_connection,"leave_data","Leave_Data",
                           "Failed","Incremental","",str(e))
         # after logging Exception raising it again to stop pipeline
        raise e
         




# Note:donot partition on the basis of date as at the time of incremental load from 
# new file i have to fetch list of dates which are distinct and i have to put filter 
# in previous data which is compute expensive as years are in less no so it will be efficient
# but in leave execidng then 8% use case i am filtring there on the basis of year and date
# so i partition data on the basis of both date and year 
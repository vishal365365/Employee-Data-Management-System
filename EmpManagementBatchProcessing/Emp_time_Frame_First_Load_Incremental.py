from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import from_unixtime, to_date,col,when,count,lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,LongType,DoubleType
import sys
sys.path.append('/home/hadoop')
from audit_table import *
# Initialize SparkSession
spark = SparkSession.builder \
    .appName("EmpTimeFrameFirstAndIncremental") \
    .config("spark.sql.warehouse.dir", sys.argv[sys.argv.index('--warehouse')+1]) \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")\
    .config("spark.sql.shuffle.partitions", "8")\
    .enableHiveSupport()\
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS Emp_Mangement")
spark.sql("use Emp_Mangement")


# config variables used in spark app
host = sys.argv[sys.argv.index('--host')+1]
port = sys.argv[sys.argv.index('--port')+1]
user = sys.argv[sys.argv.index('--user')+1]
password = sys.argv[sys.argv.index('--pass')+1]
emp_time_frame_location = sys.argv[sys.argv.index('--etf_raw')+1]
bucket = sys.argv[sys.argv.index('--bucket')+1]
etf_pre = sys.argv[sys.argv.index('--etf_pre')+1]
etf_pre_prs = sys.argv[sys.argv.index('--etf_pre_prs')+1]
etf_table_path = sys.argv[sys.argv.index('--etf_table_path')+1]


# ----------------variables for emp_communication----------------
db = sys.argv[sys.argv.index('--dbcom')+1]
table1 = sys.argv[sys.argv.index('--table1')+1]
table2 = sys.argv[sys.argv.index('--table2')+1]



# creating connection with DB for creting audit table
mysql_connection = connect_to_mysql(host,port,user,password,"Audit")

# creating audit table for emp_time_frame if not exists
create_audit_table(mysql_connection,"emp_time_frame")

# creating audit table for emp_slry_strike_cnt if not exists
create_audit_table(mysql_connection,table1)

# creating audit table for updated_salary if not exists
create_audit_table(mysql_connection,table2)

#from audit table records of emp_time_frame finding current run is first load or
# incremental
type_of_script = get_next_script_to_run(mysql_connection,"emp_time_frame")

# Define schema for employee time data
schema = StructType([
    StructField("emp_id", LongType(), True),
    StructField("designation", StringType(), True),
    StructField("start_date", IntegerType(), True),
    StructField("end_date", DoubleType(), True),
    StructField("salary", IntegerType(), True)
])
if type_of_script == "First Load":
    try:

        # Read raw data from S3
        Etf_First_Load = spark.read.csv(emp_time_frame_location,schema = schema,
                                      header=True)


        # Convert Unix timestamps to nearest dates without timestamps
        Etf_First_Load = Etf_First_Load.withColumn("start_date", to_date(from_unixtime(col("start_date"))))
        Etf_First_Load = Etf_First_Load.withColumn("end_date", to_date(from_unixtime(col("end_date"))))
        
        #creating temp view for emp_time_frame
        Etf_First_Load.createTempView("emp_time_frame")

        # removing duplicates on the basis of max(salary)
        emp_time_frame = spark.sql("select emp_id,designation,start_date,end_date,max(salary) salary\
                                   from emp_time_frame\
                                   group by emp_id, designation,start_date,end_date")
        # adding status
        emp_time_frame = emp_time_frame.\
            withColumn("status",when(col("end_date").isNull(),"ACTIVE").\
                                                    otherwise("INACTIVE"))
        
        # --------------------------------------for communication stream-----------------------------

        # creating emp_slry_strike_cnt table in database for emp communication monitoring
        # additional columns required are strike_cnt in first load strike_cnt shuould be zero
        try:
            emp_time_frame.select("emp_id","designation","salary","status").filter("status='ACTIVE'")\
                .withColumn("active_salary",col("salary")).withColumn("strike_cnt",lit(0))\
                .write.jdbc(url = f'jdbc:mysql://{host}:{port}/{db}',table = table1,mode = "append",
                        properties={
                                    "user": user,
                                    "password": password,
                                    "driver": "com.mysql.cj.jdbc.Driver"
                                })
            #adding entry in audit table emp_slry_strike_cnt that data loaded successfully
            write_audit_record(mysql_connection,table1
                           ,"emp_time_frame_first_incremental.py",
                           "Success","First Load","","successfully processed")
        except Exception as y:
                # if any Exception comes adding that entry in audit table
                write_audit_record(mysql_connection,table1
                           ,"emp_time_frame_first_incremental.py",
                           "Failed","First Load","",str(y))
        # ---------------------------------------------------------------------------------------
                
        #writing data        
        emp_time_frame.write \
            .format("parquet") \
            .partitionBy("status") \
            .mode("overwrite") \
            .saveAsTable("emp_time_frame")
        
        # moving raw data to processed and getting file names
        file_name = rawToProcessed(bucket,etf_pre,bucket,etf_pre_prs)

        #adding entry in audit table emp_time_frame that data processed successfully
        write_audit_record(mysql_connection,"emp_time_frame"
                           ,"emp_time_frame_first_incremental",
                           "Success","First Load",file_name,"successfully processed")


    except Exception as e:
        # if any Exception comes adding that entry in audit table
        write_audit_record(mysql_connection,"emp_time_frame"
                           ,"emp_time_frame_first_incremental",
                           "Failed","First Load","",str(e))
        # after logging Exception raising it again to stop pipeline
        raise e
    
        
# ---------------------------------------------for incremental--------------------
else:
    try:
        #reading the incremental data
        Etf_Incremental = spark.read.csv(emp_time_frame_location,schema=schema ,header=True)

        # Convert Unix timestamps to nearest dates without timestamps
        Etf_Incremental = Etf_Incremental.withColumn("start_date", to_date(from_unixtime(col("start_date"))))
        Etf_Incremental = Etf_Incremental.withColumn("end_date", to_date(from_unixtime(col("end_date"))))

        #creating temp view for emp_time_frame1
        Etf_Incremental.createTempView("emp_time_frame1")
        
        # removing duplicates
        emp_time_frame1 = spark.sql("select emp_id,designation,start_date,end_date,max(salary) salary\
                                   from emp_time_frame1\
                                   group by emp_id, designation,start_date,end_date")
        
        # adding status
        emp_time_frame1 = emp_time_frame1.withColumn("status", when(col("end_date").isNull(),"ACTIVE").\
                                                           otherwise("INACTIVE"))
        
        # -----------------------------------for communication stream------------------------------
        try:
            # creating emp_with_updated_slry table in database for emp communication monitoring
            # with emp_id,designation,salary this table is used for updating records
            # in emp_salary_strike_cnt

            emp_time_frame1.filter("status = 'ACTIVE'").select("emp_id","designation","salary")\
            .write.jdbc(url = f'jdbc:mysql://{host}:{port}/{db}',table = table2,mode = "overwrite",
                    properties={"user": user,
                                "password": password,
                                "driver": "com.mysql.cj.jdbc.Driver"
                                })
            
            #adding entry in audit table updated_salary that data loaded successfully
            write_audit_record(mysql_connection,table2
                           ,"emp_time_frame_first_incremental.py",
                           "Success","Incremental","","successfully processed")
            
        except Exception as y:
            write_audit_record(mysql_connection,table2
                           ,"emp_time_frame_first_incremental.py",
                           "Failed","Incremental","",str(y))
            

        
        # ---------------------------------------------------------------------------------------
            
        # loading previous active records from emp_time_frame table and adding watermark as previous
        previous_active = spark.sql("select * from emp_time_frame where status = 'ACTIVE'")\
                            .withColumn("watermark",lit("previous"))


        #adding watermark with value latest to identify the record is from new dataset
        new_records = emp_time_frame1.withColumn("watermark",lit("latest"))

        # creating window on emp_id basis so that same emp_id recprds come to same partition
        window = Window.partitionBy("emp_id")
        

        #here i donot filter active from new records for union because if there is some emp_id with
        #closed record and if i apply row_number and select the row with row_number 1 then emp_id 
        #with closed record in incremental file , the last active will still remain in dataset 
        #which is not correct so i have to include both active and inactive records of new 
        #emp time frame
        
        #so here i am filtring such records which have total 1 row and watermark is previous means
        #that specific emp id has no salary update so will remain same as it was and in second condition
        #if i have more than one record for an emp_id it means there is update in salary and if it has 
        #active status record with latest watermark will remain in dateset otherwise all will remove 
        #including the previous active one as it is closed record


        #union on previous emp_time_frame active records and new emp_time_frame records
        present_active = previous_active.union(new_records)\
                .withColumn("total", count("emp_id").over(window))\
                .filter("(total = 1 and watermark = 'previous') or \
                        (status = 'ACTIVE' and watermark = 'latest' )")
        
        #selecting the required columns
        present_active = present_active.select("emp_id","designation","start_date","end_date","salary","status")
        
        # overwritng the previous active record with updated new records
        present_active.write.format("parquet").partitionBy("status").mode("overwrite")\
            .save(etf_table_path)
        
        #filtring the inactive records
        latest_inactive = emp_time_frame1.filter("status = 'INACTIVE'")

        # appending the latest inactive record in table
        latest_inactive.write.format("parquet").partitionBy("status").mode("append")\
            .save(etf_table_path)
        
        # moving raw data to processed and getting file names
        file_name = rawToProcessed(bucket,etf_pre,bucket,etf_pre_prs)

        #adding entry in audit table emp_time_frame that data processed successfully
        write_audit_record(mysql_connection,"emp_time_frame"
                           ,"emp_time_frame_first_incremental",
                           "Success","Incremental",file_name,"successfully processed")



    except Exception as e:
        # if any Exception comes adding that entry in audit table
        write_audit_record(mysql_connection,"emp_time_frame"
                           ,"emp_time_frame_first_incremental",
                           "Failed","Incremental","",str(e))
        # after logging Exception raising it again to stop pipeline
        raise e 

        
    
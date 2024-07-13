db = {
    "host":"bootcamp.cpimkgacy4iw.us-east-1.rds.amazonaws.com",
    "port":"3306",
    "user":"vishal",
    "pass":"Vishal1997",
    "bucket":"bootcamp-emp-workflow-bronze",
    "warehouse":"s3://bootcamp-emp-workflow-gold/emp-data-warehouse"
}



lvCaQt = {
    "leaveQutaDataPath" :"s3://bootcamp-emp-workflow-bronze/emp-raw-data/leave_quota/",
    "leaveCalenderDataPath" :"s3://bootcamp-emp-workflow-bronze/emp-raw-data/leave_calender/",
    "lvcaqtScriptPath":"s3://bootcamp-emp-workflow-bronze/scripts/yearly_leave_quota_and_calender.py",
    "stepName":"leaveQuotaAndcCalenderStep",
    "lq_pre":"emp-raw-data/leave_quota/",
    "lc_pre":"emp-raw-data/leave_calender/",
    "lq_pre_prs":"emp-raw-data-processed/leave_quota/",
    "lc_pre_prs":"emp-raw-data-processed/leave_calender/",
}
lvData = {
    "leaveDataPath":"s3://bootcamp-emp-workflow-bronze/emp-raw-data/leave_data/",
    "lvDataScriptPath":"s3://bootcamp-emp-workflow-bronze/scripts/Leave_Data_First_and_Incremental.py",
    "stepName":"leaveDataStep",
    "ld_pre":"emp-raw-data/leave_data/",
    "ld_pre_prs":"emp-raw-data-processed/leave_data/",
    "ld_tb_path":"s3://bootcamp-emp-workflow-gold/emp-data-warehouse/emp_mangement.db/leave_data/"

}

lvExdUsed = {
    "lvExdScriptPath":"s3://bootcamp-emp-workflow-bronze/scripts/Leave_Exceeding_8per.py",
    "lvUsedScriptPath":"s3://bootcamp-emp-workflow-bronze/scripts/Leave_Used_80per.py",
    "stepName":"leaveExcStep",
    "stepName1":"leaveUsedStep"
}

empTf = {
    "empTfDataPath":"s3://bootcamp-emp-workflow-bronze/emp-raw-data/emp_time_frame/",
    "empTfScriptPath":"s3://bootcamp-emp-workflow-bronze/scripts/Emp_time_Frame_First_Load_Incremental.py",
    "stepName": "empTimeFrame",
    "etf_pre":"emp-raw-data/emp_time_frame/",
    "etf_pre_prs":"emp-raw-data-processed/emp_time_frame/",
    "table1":"emp_slry_strike_cnt",
    "table2":"updated_salary",
    "etf_table_path":"s3://bootcamp-emp-workflow-gold/emp-data-warehouse/emp_mangement.db/emp_time_frame/",
    "dbcom":"Emp_Stream",
    "jars":"s3://bootcamp-emp-workflow-bronze/jars/mysql-connector-java-8.0.28.jar"

}
# this contain emp_data and activeEmpByDesg task variables
empAndActEmp = {
    "EmpDataPath":"s3://bootcamp-emp-workflow-bronze/emp-raw-data/emp_data/",
    "EmpScriptPath":"s3://bootcamp-emp-workflow-bronze/scripts/Emp.py",
    "ActEmpByDesgScriptPath":"s3://bootcamp-emp-workflow-bronze/scripts/Active_Employee_by_Designation.py",
    "stepName":"activeEmpByDesg",
    "emp_pre":"emp-raw-data/emp_data/",
    "emp_pre_prs":"emp-raw-data-processed/emp_data/",
    "stepName1":"Emp",
}

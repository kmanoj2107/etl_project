import datetime
from datetime import *
import os

write_mode = "append"

last_successful_run = get_last_successful_run(BRONZE_INGESTION_LOGS) # type: ignore
current_run_date =  datetime.date.today()


if(last_successful_run == null):
    read_start_date = datetime.datetime(2025 , 6 , 29)

else:
    read_start_date = last_successful_run.date()


read_end_date = current_run_date.date()

input_paths = []
delta_days = (read_end_date - read_start_date).days


if last_successful_run == null:
    read_start_date = datetime.datetime(2025 , 6 , 29)
    write_mode = "overwrite"

else:
    write_mode = "append"



for i in (delta_days + 1):
    
    current_date = read_start_date + datetime.timedelta(days = i)
    iterative_file_path = os.path.join(
        S3_RAW_DATA_BASE_PATH,
        f"year={current_date.year}",
        f"month={current_date.month:02d}",
        f"day={current_date.day:02d}"  
    )
    is_file_exists = dbutils.fs.ls(iterative_file_path)
    if is_file_exists:
        input_paths.append(iterative_file_path)
    


df = spark.read.format("csv").option("header" , "true").option("inferschema" , "false").schema(definedSchema).path(input_paths)







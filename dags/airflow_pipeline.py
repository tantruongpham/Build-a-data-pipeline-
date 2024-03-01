from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from google_drive_downloader import GoogleDriveDownloader as gdd
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from glob import glob
import os


DataDir = "/usr/local/share/data"
QuestionsFile = "Questions.csv"  
AnswersFile = "Answers.csv"
QuestionsGoogleId = "1FGpHXtZ_b1wOL2kfBTWfRnUHLtwCAFYI"
AnswersGoogleId = "1B6ia1x05hfSEua0ns9Oc7k9k0Rcr0Bbs"
MongoURI = "mongodb://admin:password@mongo:27017/stackoverflow?authSource=admin"
MongoUser = "admin"
MongoPassword = "password"
Databases = "stackoverflow" 
SparkFile = "/usr/local/share/spark/spark_process.py"
OutputDir = "output"
default_args = {
    "owner": "Airflow",
    "start_date": datetime(2022, 1, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": ["tantruong.ph@gmail.com"],  
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


# Hàm kiểm tra trạng thái download
def _check_file_exit():
    Questions = "{}/{}".format(DataDir, QuestionsFile)
    Answers = "{}/{}".format(DataDir, AnswersFile)
    if os.path.isfile(Questions) or os.path.isfile(Answers):
        if os.path.isfile(Questions):
            print(f"Tệp {Questions} đã tồn tại.") 
        if os.path.isfile(Answers):
            print(f"Tệp {Answers} đã tồn tại.") 
        return "end"  
    else:
        print("Không tìm thấy tệp nào.")
        return "clear_file"
    
# Hàm download
def _gg_download(file_id, dest_path):    
    gdd.download_file_from_google_drive(file_id = file_id, dest_path=dest_path)

# Hàm import vào mongo
def import_mongo(MongoURI, MongoUser, MongoPassword, Databases, Collection, Dir): 
    return 'mongoimport "{}" -u {} -p {} --type csv -d {} -c {} --headerline --drop {}'.format(
        MongoURI,
        MongoUser, 
        MongoPassword,
        Databases,
        Collection,
        Dir
    )

# Hàm import output vào mongo
def import_output_mongo():
    files = glob("{}/*.csv".format(os.path.join(DataDir, OutputDir)))
    if files : 
        return import_mongo(
            MongoURI,
            MongoUser,
            MongoPassword,
            Databases,
            "Questions_of_Answers",
            os.path.join(DataDir, OutputDir, files[0])
        )


with DAG(dag_id="airflow_pipeline", schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    # Task start
    start = DummyOperator(task_id="start")
    

    # Task branching
    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=_check_file_exit,
    )

    # Task clear file
    clear_file = BashOperator (
        task_id="clear_file",
        bash_command="rm -f {}/*".format(os.path.join(DataDir))  
    )

    # Task download answers file
    download_answers_file_task = PythonOperator(
        task_id="download_answers_file_task",
        python_callable=_gg_download,
        op_kwargs={"file_id": AnswersGoogleId, "dest_path": os.path.join(DataDir)}
    )

    # Task download questions file
    download_questions_file_task = PythonOperator(
        task_id="download_questions_file_task",
        python_callable=_gg_download,
        op_kwargs={"file_id": QuestionsGoogleId, "dest_path": os.path.join(DataDir, QuestionsFile)} 
    )

    # Task import answers file vào mongo
    import_answers_mongo = BashOperator(
        task_id="import_answers_mongo",
        bash_command=import_mongo(
        MongoURI,
        MongoUser, 
        MongoPassword,
        Databases,
        "answers",
        os.path.join(DataDir, AnswersFile)           
        )
    )

    # Task import question file vào mongo
    import_questions_mongo = BashOperator(
        task_id="import_questions_mongo",
        bash_command=import_mongo(
        MongoURI,
        MongoUser, 
        MongoPassword,
        Databases,
        "questions",
        os.path.join(DataDir, QuestionsFile)           
        )
    )

    # Task process spark
    spark_process = SparkSubmitOperator(
        task_id="spark_process",
        conn_id = "spark_submit_conn",
        application = SparkFile,
        packages="org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
    )

    # Task import mongo
    import_output_mongo = BashOperator(
        task_id="import_output_mongo",
        bash_command= import_output_mongo()
    )

    # Task end
    end = DummyOperator(task_id="end")

    start >> branching >> [clear_file, end]
    clear_file >> [download_answers_file_task, download_questions_file_task] 
    download_answers_file_task >> import_answers_mongo 
    download_questions_file_task >> import_questions_mongo
    [import_answers_mongo, import_questions_mongo] >> spark_process >> import_output_mongo >> end
        

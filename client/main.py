import cleaning
import enrichment
import feature_engineering
from client.logging_config import configure_logging
from client.server_interaction import submit_data_job, submit_sql_job, get_next_job, start_next_job
from server_interaction import download_database

def start_client():
    configure_logging()
    download_database()

def submit_jobs():
    job_response_1 = submit_data_job(
        code_path="enrichment.py",
        requirements_path="../requirements.txt",
        data_file_path="../.gitignore"
    )
    job_response_2 = submit_sql_job(
        code_path="enrichment.py",
        requirements_path="../requirements.txt",
        sql_query="SELECT * FROM table"
    )
    return job_response_1, job_response_2

cleaned_data = cleaning.dummy_func(None)
enriched_data = enrichment.dummy_func(cleaned_data)
c = feature_engineering.dummy_func(enriched_data)

if __name__ == "__main__":
    start_client()
    j1, j2 = submit_jobs()
    print(j1.json(), j2.json())
    print(get_next_job())
    print(start_next_job())
# dags/weekly_pipeline.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ── Default arguments applied to every task in this DAG ──────────────────────
default_args = {
    "owner": "koka",                         # your name, just for labeling
    "retries": 1,                            # retry once if a task fails
    "retry_delay": timedelta(minutes=5),     # wait 5 min before retrying
    "email_on_failure": False,               # set True if you want email alerts
}

# ── Define the DAG ────────────────────────────────────────────────────────────
with DAG(
    dag_id="weekly_data_pipeline",           # unique name shown in the UI
    description="Scrape → Transform → Save to Supabase",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),         # when the schedule starts counting from
    schedule_interval="0 8 * * 1",           # every Monday at 8:00 AM (cron format)
    catchup=False,                           # don't run missed past runs
    tags=["pipeline", "weekly"],             # for organizing in the UI
) as dag:

    # ── Task 1: Scraping ──────────────────────────────────────────────────────
    scrape_booking_task = BashOperator(
        task_id="run_booking_scraper",
        bash_command="python /opt/airflow/scripts/booking_scraper.py",
    )

    scrape_talabat_task = BashOperator(
        task_id="run_talabat_scraper",
        bash_command="python /opt/airflow/scripts/talabat_scraper.py",
    )

    # ── Task 2: Transformation & Cleaning ────────────────────────────────────
    transform_task = BashOperator(
        task_id="run_transform",
        bash_command="python /opt/airflow/scripts/transformation_script.py",
    )

    # ── Task 3: Save to Supabase ──────────────────────────────────────────────
    save_task = BashOperator(
        task_id="run_save_to_supabase",
        bash_command="python /opt/airflow/supabase/supabase_loader.py",
    )

    # ── Define execution ORDER (the pipeline chain) ───────────────────────────
    [scrape_booking_task, scrape_talabat_task] >> transform_task >> save_task
    # This means: scrapers run first in parallel, then transform, then save
    # If any step fails, the next one does NOT rundoc
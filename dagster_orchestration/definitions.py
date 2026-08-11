from dagster import Definitions, asset, ScheduleDefinition, define_asset_job, AssetExecutionContext
import subprocess
import os
import time

@asset
def ingest_kiva_api_to_postgres(context: AssetExecutionContext):
    """
    Asset that triggers the Python ingestion script to pull data from Kiva REST API 
    and load it into PostgreSQL.
    """
    script_path = os.path.join(os.path.dirname(__file__), "..", "src", "ingest_api.py")
    context.log.info(f"Running ingestion script at {script_path}")
    
    # We must pass the internal Docker network host/port, not the localhost port mapped to the host machine
    env = os.environ.copy()
    env["DB_HOST"] = "postgres"
    env["DB_PORT"] = "5432"
    
    result = subprocess.run(["python", script_path], capture_output=True, text=True, env=env)
    if result.returncode != 0:
        context.log.error(f"Ingestion failed: {result.stderr}")
        raise Exception("API Ingestion Failed")
        
    context.log.info(f"Ingestion Output: {result.stdout}")
    return "Ingestion successful"

@asset(deps=[ingest_kiva_api_to_postgres])
def dbt_analytics_models(context: AssetExecutionContext):
    """
    Asset that triggers the dbt transformations in ClickHouse.
    It inherently waits for ingestion to finish, and we add a small 3-second buffer
    to ensure Debezium CDC and Redpanda have fully streamed the records to ClickHouse.
    """
    context.log.info("Allowing 3 seconds for CDC Debezium replication to catch up...")
    time.sleep(3)
    
    dbt_dir = os.path.join(os.path.dirname(__file__), "..", "dbt_project")
    
    # 1. Run dbt models
    context.log.info("Running dbt models...")
    run_result = subprocess.run(["dbt", "run"], cwd=dbt_dir, capture_output=True, text=True)
    context.log.info(run_result.stdout)
    if run_result.returncode != 0:
        raise Exception(f"dbt run failed: {run_result.stderr}")
    
    # 2. Run dbt tests (Data Quality Validation)
    context.log.info("Running dbt data quality tests...")
    test_result = subprocess.run(["dbt", "test"], cwd=dbt_dir, capture_output=True, text=True)
    context.log.info(test_result.stdout)
    if test_result.returncode != 0:
        raise Exception(f"dbt test failed: {test_result.stderr}")
        
    return "dbt models and tests completed successfully"

# Define a job that executes the entire pipeline sequentially
daily_pipeline_job = define_asset_job(name="end_to_end_pipeline", selection="*")

# Schedule the job to run every day at midnight
daily_schedule = ScheduleDefinition(
    job=daily_pipeline_job,
    cron_schedule="0 0 * * *"
)

defs = Definitions(
    assets=[ingest_kiva_api_to_postgres, dbt_analytics_models],
    schedules=[daily_schedule]
)

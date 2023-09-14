from dagster_duckdb import DuckDBResource
from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)


from .assets import etl, clustering

# Job definitions
etl_asset_job = define_asset_job(
    name="etl_job", selection=["source", "transformations", "sink"]
)
ingestion_asset_job = define_asset_job(name="ingestion_job", selection="ingestor")
clustering_job = define_asset_job(
    name="clustering_job",
    selection=["preprocessor", "kmeans_cluster", "Silhouette_evaluator"],
)

# Schedule definitions
hourly_etl_schedule = ScheduleDefinition(
    job=etl_asset_job, cron_schedule="@hourly", execution_timezone="America/New_York"
)
daily_clustering_schedule = ScheduleDefinition(
    job=clustering_job, cron_schedule="@daily", execution_timezone="America/New_York"
)

# Data assets definitions
defs = Definitions(
    assets=load_assets_from_package_module(assets),
    jobs=[etl_asset_job, ingestion_asset_job, clustering_job],
    resources={
        "duckdb": DuckDBResource(database="glmFlash.db"),
    },
    schedules=[hourly_etl_schedule, daily_clustering_schedule],
)

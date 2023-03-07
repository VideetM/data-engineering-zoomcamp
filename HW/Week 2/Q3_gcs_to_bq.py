from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(
        f"pre: missing of passenger counts:")
    # df["passenger_count"].fillna(0, inplace=False)
    print(
        f"post: missing passenger count: ")
    return df


@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-credits")
    df.to_gbq(destination_table="dezoomcamp.fhv_data",
              project_id="dtc-de-375706",
              credentials=gcp_credentials_block.get_credentials_from_service_account(),
              chunksize=500_000,
              if_exists="append"
              )


@flow()
def etl_gcs_to_bq(year: int, month: int, color: str) -> None:

    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


@flow()
def etl_parent_flow(
    # parametrizing gcs to bq flow
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_gcs_to_bq(year, month, color)


if __name__ == "__main__":
    color = "fhv"
    months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    year = 2019
    etl_parent_flow(months, year, color)

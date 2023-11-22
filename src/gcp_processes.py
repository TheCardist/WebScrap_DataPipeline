from google.cloud import bigquery
import pandas as pd

import subprocess
from src.utils import (
    logger,
)

rate_rules_table = "gcp/table"
log_table = "gcp/table"


def copy_files_to_gcs(filename, gcs_path):
    """Copy files from the modified and raw folders to the Google Cloud Storage location in case they are needed in the future"""

    cmd = f"gsutil -m cp -r {filename} {gcs_path}"
    subprocess.check_output(cmd, shell=True, encoding="utf-8").split("\n")


def load_dataframe_to_gcp(df, destination):
    """Load dataframe to destination table."""

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    client.load_table_from_dataframe(df, destination, job_config=job_config)


def remove_current_ind(hotel_list: str):
    """Remove Current_Ind from all hotels in the list if they're set to 'Y' to make room for updates."""

    target_table = "target-table"
    project_id = "project-id"

    formatted_hotel_list = ", ".join(['"' + hotel + '"' for hotel in hotel_list])
    logger.info(
        f"Removing current_ind for the following hotels: {formatted_hotel_list}"
    )
    update_query = f"UPDATE `{target_table}` SET CURRENT_IND = NULL WHERE CURRENT_IND = 'Y' AND LOC_ID in ({formatted_hotel_list})"
    cmd = [
        "bq",
        "--project_id=" + project_id,
        "query",
        "--use_legacy_sql=false",
        update_query,
    ]
    subprocess.run(cmd)


def get_hotel_list(available_hotels) -> pd.DataFrame:
    """Use the scraped list from the vendor website to query the table for Optimization details on the provided hotels."""

    stmt = f"""SELECT QUERY with {available_hotels}"""

    project_id = "project-id"
    df = pd.read_gbq(stmt, dialect="standard", project_id=project_id)
    return df


if __name__ == "__main__":
    pass

import subprocess
import datetime
import json
import glob
import re
import csv
import os
import pandas as pd
import tempfile
from typing import Union
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

debug_handler = logging.FileHandler("./debug.log")

debug_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
debug_handler.setFormatter(formatter)

logger.addHandler(debug_handler)


def clean_up_downloads() -> str:
    """Due to limited storage space, deleting downloads, modified files and temp files."""

    cmd = "rm ./downloads/*.csv ./modified_files/*.csv ./temp_files/*.csv"
    subprocess.run(cmd, shell=True)


def modify_filename(base_filename: str) -> str:
    """Modify the original downloaded filename to save into another location."""

    base_name, extension = os.path.splitext(base_filename)
    modified_filename = base_name + "_modified" + extension

    return modified_filename


def copy_files():
    """Copy both original downloads and modified downloads to GCS location."""

    gcs_path_original = "gs://original_file_path"

    original_files = find_files("./downloads")
    for file in original_files:
        cmd = f"gsutil -m cp -r {file} {gcs_path_original}"
        subprocess.check_output(cmd, shell=True, encoding="utf-8").split("\n")

    gcs_path_modified = "gs://modified_file_path"

    modified_files = find_files("./modified_files")

    for file in modified_files:
        cmd = f"gsutil -m cp -r {file} {gcs_path_modified}"
        subprocess.check_output(cmd, shell=True, encoding="utf-8").split("\n")


def extract_datetime(filename: str) -> datetime:
    """Extract the date and time from the filename and convert them to a datetime obj to upload to the GCP Tables."""

    datetime_pattern = r"\d{8}_\d{2}-\d{2}-\d{2}"
    datetime_match = re.search(datetime_pattern, filename)

    if datetime_match:
        datetime_str = datetime_match.group(0)

    combined_datetime = datetime_str[:8] + "_" + datetime_str[9:].replace("-", ":")
    format_str = "%m%d%Y_%H:%M:%S"

    datetime_obj = datetime.datetime.strptime(combined_datetime, format_str).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return datetime_obj


def find_files(dir: str) -> list[str]:
    """Use today's date and expected filename to get a list of the files to clean and upload."""

    date = datetime.datetime.now().strftime("%m%d%Y")

    file_path = f"{dir}/file-*-{date}*.csv"
    file_paths = glob.glob(file_path)

    return file_paths


def run_bq_load_command(
    project_id, dataset_id, table_id, file_path, format="CSV", skip_leading_rows=1
):
    """Execute BigQuery load command to upload data to tables."""

    cmd = f"bq load --source_format={format} --skip_leading_rows={skip_leading_rows} --project_id={project_id} {dataset_id}.{table_id} {file_path}"
    try:
        subprocess.check_output(cmd, shell=True, encoding="utf-8").split("\n")
    except subprocess.CalledProcessError as e:
        logger.exception(f"Error executing command: {cmd}")
        logger.info(f"Return code: {e.returncode}")
        logger.info(f"Output: {e.output}")


class CustomException(Exception):
    """This is used to catch exceptions for retry attempts."""

    pass


def validate_file_download(hotel: str) -> bool:
    """Validate if the file was downloaded or not from the select website."""

    directory_path = "./downloads"

    for filename in os.listdir(directory_path):
        if hotel in filename:
            logger.debug(f"Hotel: {hotel}, Filename: {filename}")
            return True
    else:
        with open("./jsons/optimizations.json", "r") as json_file:
            data = json.load(json_file)

        # If the file is missing then we set the lst_optimization in the json to blank
        for entry in data:
            if entry.get("hotel_cd") == f"{hotel}" and "lst_optimization" in entry:
                entry["lst_optimization"] = ""

        with open("./jsons/optimizations.json", "w") as json_file:
            json.dump(data, json_file, indent=4)
        return False


def get_hotel_list() -> pd.DataFrame:
    """Query GCP for a list of hotels that are active on the vendors platform. This also gets the optimization times for each hotel which is used to determine which hotels need to be updated by this process."""

    stmt = """select hotel_list
    from dual"""

    cmd = [
        "bq",
        "query",
        "--use_legacy_sql=false",
        "--format=json",
        "--project_id=gcp_project",
        stmt,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        output = result.stdout.strip()  # Get the query output as a string
    except subprocess.CalledProcessError as e:
        logger.exception(f"Error executing command: {cmd}")
        logger.info(f"Return code: {e.returncode}")
        logger.info(f"Output: {e.output}")

    df = pd.read_json(output, orient="records")

    return df


def check_for_missing_hotels(hotel_list: pd.DataFrame) -> pd.DataFrame:
    """Check if hotel is missing from the Optimization TS (for new hotels added to N2P)."""

    with open("./jsons/optimizations.json", "r") as json_file:
        json_data = pd.read_json(json_file)

    # Merge hotel_list and json_data with indicator to find non-matching entries
    merged_df = hotel_list.merge(
        json_data[["hotel_cd"]], on=["hotel_cd"], how="left", indicator=True
    )

    # Filter rows that are only present in hotel_list but not in json_data
    missing_hotels = merged_df[merged_df["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )

    if not missing_hotels.empty:
        json_data = pd.concat(
            [json_data, missing_hotels[["hotel_cd"]]], ignore_index=True
        )

        # Write the updated JSON data back to the file
        with open("./jsons/optimizations.json", "w") as json_file:
            json.dump(json_data.to_dict(orient="records"), json_file, indent=4)

        logger.debug(f"Missing hotels: {missing_hotels}")
        return missing_hotels


def check_lst_optimization(hotel_list: pd.DataFrame) -> pd.DataFrame:
    """If the hotel exist, check the hotel and filename to see if the json entries match the query results."""

    with open("./jsons/optimizations.json", "r") as json_file:
        json_data = pd.read_json(json_file)

    # Check if the data matches or not, mis-matched records are added to the variable.
    merged_df = pd.merge(
        hotel_list,
        json_data,
        how="outer",
        on=["hotel_cd", "lst_optimization"],
        indicator=True,
    )
    hotels_to_update = merged_df[merged_df["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )

    # If the variable has entries then give the list for downloading the report as well as updating the optimization timestamp.
    if not hotels_to_update.empty:
        return hotels_to_update


def update_optimization_record(hotel):
    """Updating the lst_optimization timestamp in the json file with the most recent entry from the query."""

    hotel_codes = get_hotel_list()

    with open("./jsons/optimizations.json", "r") as json_file:
        json_data = pd.read_json(json_file)

    for index, row in hotel_codes.iterrows():
        if row["hotel_cd"] == hotel:
            correct_optimization_ts = row["lst_optimization"]

            json_data.loc[
                json_data["hotel_cd"] == hotel, "lst_optimization"
            ] = correct_optimization_ts

        # Write the updated JSON data back to the file
        with open("./jsons/optimizations.json", "w") as json_file:
            json.dump(json_data.to_dict(orient="records"), json_file, indent=4)


def validate_lst_optimizations() -> Union[None, pd.DataFrame]:
    """This determines whether a hotel is required to be updated or not by comparing the new lst_optimization timestamp against what is stored. It also looks for hotels that are missing from the file."""

    hotel_list = get_hotel_list()
    missing_hotels = check_for_missing_hotels(hotel_list)
    mismatched_optimizations = check_lst_optimization(hotel_list)

    if missing_hotels is None and mismatched_optimizations is None:
        return None
    else:
        if mismatched_optimizations is None:
            logger.info(
                f"The following hotels were added and require downloads: \n{', '.join(missing_hotels['hotel_cd'])}"
            )
            return missing_hotels
        else:
            combined_hotel_list = pd.concat(
                [missing_hotels, mismatched_optimizations], ignore_index=True
            )
            combined_hotel_list.drop_duplicates(subset=["hotel_cd"], inplace=True)
            logger.info(
                f"These hotels require downloads {', '.join(combined_hotel_list['hotel_cd'])}"
            )
            return combined_hotel_list


if __name__ == "__main__":
    df = get_hotel_list()
    check_for_missing_hotels(df)

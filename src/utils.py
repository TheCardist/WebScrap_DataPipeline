# Third Party
import pandas as pd

# Standard library
from typing import Union
from functools import partial
import subprocess
import datetime
import json
import re
import os
import time
import logging
import concurrent.futures


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

debug_handler = logging.FileHandler("./logs/debug.log")

debug_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
debug_handler.setFormatter(formatter)

logger.addHandler(debug_handler)


def process_function_helper(item, func, extra_param):
    """Helper function for initiating_multiprocess; this is to allow for an additional parameter to be passed to a function if required."""
    return func(item, extra_param)


def initiate_multiprocess(func, iterable, workers=3, extra_param=None):
    """For performance gains processpool is utilized in a few areas of the application like
    webscraping, modifying files, and copying files to Google Cloud Storage. Partial is used to allow for another
    parameter for copying files to GCS."""

    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        if extra_param is None:
            return list(executor.map(func, iterable))
        else:
            partial_func = partial(
                process_function_helper, func=func, extra_param=extra_param
            )
            return list(executor.map(partial_func, iterable))


def clean_up_downloads():
    """Removing downloaded, raw, and processed files at the end of application runtime."""

    cmd = "rm ./data/raw/* ./data/processed/* ./data/downloads/*"
    subprocess.run(cmd, shell=True)


def modify_filename(base_filename: str) -> str:
    """Modify the original downloaded filename to save into another location."""

    base_name, extension = os.path.splitext(base_filename)
    modified_filename = base_name + "_modified" + extension

    return modified_filename


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


def find_files(dir: str):
    """Use today's date and expected filename to get a list of the files to clean and upload."""
    files = os.listdir(dir)
    file_paths = [os.path.join(dir, file) for file in files]

    return file_paths


class CustomException(Exception):
    pass


def validate_file_download(hotel: str) -> bool:
    """Validate if the file was downloaded or not from the N2P Site."""

    directory = "./data/downloads"
    timeout = 15
    seconds = 0
    dl_wait = True

    while dl_wait and seconds < timeout:
        for filename in os.listdir(directory):
            if hotel in filename and filename.endswith(".csv"):
                os.rename(f"{directory}/{filename}", f"./data/raw/{filename}")
                dl_wait = False
                return True
            elif hotel in filename and filename.endswith(".crdownload"):
                logger.info(f"File: {filename} has not completed download.")
        time.sleep(1)
        seconds += 1
    return False


def check_for_missing_hotels(hotel_list: pd.DataFrame) -> pd.DataFrame:
    """Check if hotel is missing from the Optimization TS (for new hotels added to N2P)."""

    with open("./data/jsons/optimizations.json", "r") as json_file:
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
        with open("./data/jsons/optimizations.json", "w") as json_file:
            json.dump(json_data.to_dict(orient="records"), json_file, indent=4)

        return missing_hotels


def check_lst_optimization(hotel_list: pd.DataFrame) -> pd.DataFrame:
    """If the hotel exist, check the hotel and filename to see if the json entries match the query results."""

    with open("./data/jsons/optimizations.json", "r") as json_file:
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


def validate_lst_optimizations(
    available_hotels: pd.DataFrame,
) -> Union[None, pd.DataFrame]:
    """Validation if hotels are missing from the optimization json or if the database optimization TS does not match the
    optimization json. Both missing hotels and mismatched hotels are selected for processing.
    """

    missing_hotels = check_for_missing_hotels(available_hotels)
    mismatched_optimizations = check_lst_optimization(available_hotels)

    if missing_hotels is None and mismatched_optimizations is None:
        return None
    else:
        if mismatched_optimizations is None:
            logger.info(
                f"Number of hotels requiring downloads: {len(missing_hotels['hotel_cd'])}"
            )
            return missing_hotels
        else:
            combined_hotel_list = pd.concat(
                [missing_hotels, mismatched_optimizations], ignore_index=True
            )
            combined_hotel_list.drop_duplicates(subset=["hotel_cd"], inplace=True)
            logger.info(
                f"Number of hotels requiring downloads: {len(combined_hotel_list['hotel_cd'])}"
            )
            return combined_hotel_list


if __name__ == "__main__":
    pass

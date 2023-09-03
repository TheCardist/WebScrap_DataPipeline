import datetime
import os
import subprocess
import re
import pandas as pd
from utils import (
    find_files,
    copy_files,
    extract_datetime,
    modify_filename,
    clean_up_downloads,
    run_bq_load_command,
    update_optimization_record,
    logger,
)


def load_to_bigquery(hotel_codes: pd.DataFrame):
    """Runs the functions to find, clean, and upload the files found to BigQuery"""

    file_path = find_files("./downloads")
    for hotel_file in file_path:
        filename = os.path.basename(
            hotel_file
        )  # basename of file for extracting hotel code and timestamp
        hotel, modified_file = modify_files(
            full_hotel_filename=hotel_file, base_filename=filename
        )
        remove_current_ind(hotel)
        update_rate_rule_table(modified_file)
        update_optimization_record(hotel)
    update_log_table(hotel_codes)
    copy_files()
    clean_up_downloads()


def modify_files(full_hotel_filename: str, base_filename: str) -> str:
    """Adds LOC_ID, CURRENT_IND, SRC_FILENAME, and Download Datetime column to each row of that column before saving as a modified version."""

    hotel_name_pattern = r"\b([A-Z]+)\b"

    hotel_code = re.findall(
        hotel_name_pattern, full_hotel_filename
    )  # Get the hotel code from the filename
    modified_filename = modify_filename(base_filename)  # Add _modified to the filename
    datetime_obj = extract_datetime(full_hotel_filename)

    df = pd.read_csv(full_hotel_filename, sep="|", dtype={14: str})

    df.insert(0, "LOC_ID", hotel_code[0])
    df["CURRENT_IND"] = "Y"
    df["SRC_FILENAME"] = modified_filename
    df["LST_UPDT_TS"] = datetime_obj

    df.columns = df.columns.str.upper()  # Uppercase all columns in the document

    df.reset_index(drop=True)

    df.to_csv(f"./modified_files/{modified_filename}", sep=",", index=False)

    logger.info(f"Saved {modified_filename}")

    modified_file = f"./modified_files/{modified_filename}"

    return hotel_code[0], modified_file


def update_rate_rule_table(modified_file: str):
    """Upload the CSV files to the appropriate table."""

    project_id = "project_id"
    dataset_id = "dataset_id"
    table_id = "table_id"

    schema_path = "./jsons/schema.json"  # Set datatypes for the table

    cmd = f"bq load --project_id={project_id} --schema={schema_path} --source_format=CSV --skip_leading_rows=1 {dataset_id}.{table_id} {modified_file}"
    try:
        logger.info(f"Uploading rate rule data for {modified_file}")
        subprocess.run(cmd, shell=True)
    except subprocess.CalledProcessError as e:
        logger.exception(f"Error executing command: {cmd}")
        logger.info(f"Output: {e.result}")


def remove_current_ind(hotel: str):
    """Remove Current_Ind from all hotels in the list if they're set to 'Y' to make room for updates."""

    target_table = "target_table"
    project_id = "project_id"

    logger.info(f"Removing existing indicators for {hotel}")

    update_query = f"UPDATE `{target_table}` SET CURRENT_IND = NULL WHERE CURRENT_IND = 'Y' AND LOC_ID = '{hotel}'"
    cmd = [
        "bq",
        "--project_id=" + project_id,
        "query",
        "--use_legacy_sql=false",
        update_query,
    ]

    subprocess.run(cmd)


def update_log_table(hotel_codes: str):
    """Update the log table with file information for hotels that did and did not get downloaded."""

    project_id = "project_id"
    dataset_id = "dataset_id"
    table_id = "table_id"

    directory_path = "./downloads/"

    date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    list_of_dict_data = []

    # Loop through files in the directory
    for hotel in hotel_codes:
        for filename in os.listdir(directory_path):
            if hotel in filename:
                download_time = extract_datetime(filename)
                file_path = os.path.join(directory_path, filename)
                df = pd.read_csv(file_path)
                data_amt = len(df)

                table_data = {
                    "LOC_ID": hotel,
                    "DATA_AMT": data_amt,
                    "SRC_FILENAME": filename,
                    "SRC_FILE_TS": download_time,
                    "CREAT_TS": date,
                }

                list_of_dict_data.append(table_data)
                break

        else:  # If the hotel doesn't have a file upload with null values for 3 fields.
            table_data = {
                "LOC_ID": hotel,
                "DATA_AMT": None,
                "SRC_FILENAME": None,
                "SRC_FILE_TS": None,
                "CREAT_TS": date,
            }

            list_of_dict_data.append(table_data)

    df = pd.DataFrame(list_of_dict_data)

    csv_temp_file = "./temp_files/log_file.csv"
    df.to_csv(csv_temp_file, index=False)

    run_bq_load_command(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        file_path=csv_temp_file,
    )


if __name__ == "__main__":
    hotels = ["RCHMD", "LEXGT", "SEFLR"]
    update_log_table(hotels)

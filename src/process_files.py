# Third Party
import pandas as pd

# Standard Library
import os
import datetime
import re

# Internal
from src.utils import (
    extract_datetime,
)


def update_optimization_json(hotels: pd.DataFrame):
    """Updating the optimization json with the new timestamp from the database once upload is completed.
    This is so we do not pull this hotel again for processing in the next hourly run unless the database timestamp is different.
    """

    with open("./data/jsons/optimizations.json", "r") as json_file:
        json_data = pd.read_json(json_file)

    for index, row in hotels.iterrows():
        hotel = row["hotel_cd"]
        current_optimization_ts = row["lst_optimization"]
        json_data.loc[
            json_data["hotel_cd"] == hotel, "lst_optimization"
        ] = current_optimization_ts

        with open("./data/jsons/optimizations.json", "w") as json_file:
            json_file.write(json_data.to_json(orient="records", indent=4))


def create_modified_files(full_filename: str) -> str:
    """Modifying the raw files into new modified version that will be upload to the database tables."""

    hotel_name_pattern = r"\b([A-Z]+)\b"
    base_filename = os.path.basename(full_filename)

    # Get hotel code using hotel_name_pattern
    hotel_code = re.findall(hotel_name_pattern, full_filename)

    # Use basename to add _modified after basename 'ABCDE_{yyyymmddhhmmss}_modified.csv'
    modified_filename = modify_filename(base_filename)
    file_datetime = extract_datetime(base_filename)

    df = pd.read_csv(full_filename, sep="|", dtype={14: str})

    df.insert(0, "LOC_ID", hotel_code[0])
    df["CURRENT_IND"] = "Y"
    df["SRC_FILENAME"] = modified_filename
    df["LST_UPDT_TS"] = file_datetime

    df.columns = df.columns.str.upper()  # Uppercase all columns
    df.reset_index(drop=True)

    df.to_csv(f"./data/processed/{modified_filename}", sep=",", index=False)

    return hotel_code[0]


def create_rate_rule_dataframe(filenames):
    """Create dataframe for upload to GCP, after creation quick replaces are done so the column headers align with the GCP columns."""

    dataframes = []

    for filename in filenames:
        df = pd.read_csv(filename)

        dataframes.append(df)

    combined_df = pd.concat(dataframes, axis=0, ignore_index=True)
    combined_df.columns = (
        combined_df.columns.str.replace(" ", "_")
        .str.replace("[^\w\s]", "")
        .str.replace("(", "")
        .str.replace(")", "")
        .str.replace("-", "_")
    )
    combined_df["LST_UPDT_TS"] = pd.to_datetime(combined_df["LST_UPDT_TS"], utc=True)

    return combined_df


def create_log_dataframe(hotels, directory):
    """Create dataframe for upload to GCP. New datafields are in the process."""

    date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    list_of_hotel_dicts = []

    for hotel in hotels:
        for filename in os.listdir(directory):
            if hotel in filename:
                download_time = extract_datetime(filename)
                file_path = os.path.join(directory, filename)
                df = pd.read_csv(file_path)
                data_amt = len(df)

                table_data = {
                    "LOC_ID": hotel,
                    "DATA_AMT": data_amt,
                    "SRC_FILENAME": filename,
                    "SRC_FILE_TS": download_time,
                    "CREAT_TS": date,
                }

                list_of_hotel_dicts.append(table_data)
                break
        else:
            table_data = {
                "LOC_ID": hotel,
                "DATA_AMT": None,
                "SRC_FILENAME": None,
                "SRC_FILE_TS": None,
                "CREAT_TS": date,
            }

            list_of_hotel_dicts.append(table_data)

    df = pd.DataFrame(list_of_hotel_dicts)
    df["DATA_AMT"] = df["DATA_AMT"].fillna(0).astype("int32")

    return df


def modify_filename(base_filename: str) -> str:
    """Add modified string to filename upload to Google Cloud Storage."""

    base_name, extension = os.path.splitext(base_filename)
    modified_filename = base_name + "_modified" + extension

    return modified_filename


if __name__ == "__main__":
    pass

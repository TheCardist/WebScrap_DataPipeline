# Standard library
import time
import os

# Internal
from src.utils import (
    find_files,
    initiate_multiprocess,
    validate_lst_optimizations,
    clean_up_downloads,
    logger,
)
from src.gcp_processes import (
    load_dataframe_to_gcp,
    remove_current_ind,
    rate_rules_table,
    log_table,
    copy_files_to_gcs,
    get_hotel_list,
)
from src.process_files import (
    create_modified_files,
    create_rate_rule_dataframe,
    create_log_dataframe,
    update_optimization_json,
)
from src.web_scrape import multiprocess_downloads, get_hotels_for_query


def main():
    """
    Application flow:
    1. Scrapes available hotel list from the vendor website, this is used to query the GCP database to get optimization details. We do it this way because there are 'Enabled' hotels on the vendor site that are not known in the database, so to avoid checking against 2600 hotels to see if they're enabled we are grabbing the list directly from the source.
    2. Query the database for optimization details then use this same hotel list to log into and download files from the vendor site, using up to 3 multiprocesses workers for performance. We validate that downloads were successful, if not it will retry twice. Files that are validated are moved from ./data/downloads to ./data/raw for additional processing.
    3. Files modified with additional data points and saved as csv files to ./data/processed
    4. Database query runs to turn all transactions with current_ind = 'Y' to null
    5. Files are then read into Pandas to be converted to a DataFrame and then uploaded to GCP.
    6. A log DataFrame is created from the files in ./data/raw and uploaded to the appropriate target table.
    7. Copy process kicks off which copies both ./data/raw and ./data/processed files to GCS Storage for later reference.
    8. Clean up process deletes files from ./data/downloads ./data/raw and ./data/processed
    """
    raw_directory = "./data/raw"
    processed_directory = "./data/processed"
    raw_gcs_path = "gs://storage/path"
    modified_gcs_path = "gs://storage/path"

    start_timer = time.perf_counter()
    logger.info("++++++ Beginning Process ++++++")

    # Scrape website for hotel list
    available_hotels = get_hotels_for_query()

    # Use that scraped list to query the database
    queried_hotel_list = get_hotel_list(available_hotels=available_hotels)
    hotels_to_download = validate_lst_optimizations(queried_hotel_list)

    # Only hotels where their optimization in the DB doesn't match the json require additional action
    if hotels_to_download is None:
        logger.info("No hotels to update at this time.")
    else:
        multiprocess_downloads(hotels_to_download["hotel_cd"])

        # Sanity check to make sure there are downloaded files
        contents = os.listdir(raw_directory)

        if contents:
            logger.debug(f"File Contents: {contents}")

            # Multiprocess modifying and saving new files
            raw_hotel_files = find_files(raw_directory)
            hotels = initiate_multiprocess(
                func=create_modified_files, iterable=raw_hotel_files
            )

            # Create dataframe for rate rule table
            modified_hotel_files = find_files(processed_directory)
            rate_rule_df = create_rate_rule_dataframe(modified_hotel_files)

            # Create dataframe for log table
            log_df = create_log_dataframe(hotels, processed_directory)

            # Clear indicators in table for existing records
            remove_current_ind(hotels)

            # Load rate rules data to GCP
            load_dataframe_to_gcp(rate_rule_df, destination=rate_rules_table)
            # Load log data to GCP
            load_dataframe_to_gcp(log_df, destination=log_table)

            # Update the optimization json with the information from the database
            update_optimization_json(queried_hotel_list)

            # Copy Files to Google Cloud Storage
            initiate_multiprocess(
                func=copy_files_to_gcs,
                iterable=raw_hotel_files,
                extra_param=raw_gcs_path,
            )
            initiate_multiprocess(
                func=copy_files_to_gcs,
                iterable=modified_hotel_files,
                extra_param=modified_gcs_path,
            )

            # Remove downloaded, processed, and raw files
            clean_up_downloads()
        else:
            logger.warning("No files were found for upload.")

    end_time = time.perf_counter()
    logger.info(f"Finished in: {round(end_time-start_timer, 2)} second(s)")
    logger.info("------ Complete ------")


if __name__ == "__main__":
    main()

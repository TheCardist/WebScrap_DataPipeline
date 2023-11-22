# Third Party
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Standard library
from retry import retry
import time

# Internal
from .utils import (
    CustomException,
    validate_file_download,
    logger,
    initiate_multiprocess,
)
from .webdriver_setup import (
    get_env_details,
    create_webdriver,
)


def login_to_site(driver: webdriver, USERNAME: str, PASSWORD: str):
    """Log into the Vendor website using environment variables, then load the correct page for downloading the required report."""

    WebDriverWait(driver, 15).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="1-email"]'))
    ).send_keys(USERNAME)

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                '//*[@id="auth0-lock-container-1"]/div/div[2]/form/div/div/div/div/div[2]/div[2]/span/div/div/div/div/div/div/div/div/div/div/div[2]/div/div/input',
            )
        )
    ).send_keys(PASSWORD)

    login_button = WebDriverWait(driver, 15).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                '//*[@id="auth0-lock-container-1"]/div/div[2]/form/div/div/div/button',
            )
        )
    )

    login_button.click()
    time.sleep(3)

    driver.get("website-page.com")


def get_available_n2p_hotels(driver: webdriver) -> list:
    """Pull list of hotels from the page drop down. This ensures that we're only querying the database for hotels that are available on the website."""

    combobox_button = WebDriverWait(driver, 15).until(
        EC.element_to_be_clickable(
            (By.XPATH, '//kendo-combobox//button[@aria-label="Select"]')
        )
    )

    combobox_button.click()

    hotel_dropdown = WebDriverWait(driver, 15).until(
        EC.visibility_of_element_located((By.CLASS_NAME, "k-list-ul"))
    )
    time.sleep(1)  # Allow for loading
    hotels = hotel_dropdown.find_elements(By.CLASS_NAME, "k-list-item-text")

    hotel_list = [hotel.text for hotel in hotels]

    return hotel_list


def multiprocess_downloads(hotels: list):
    """Setup for multiprocess of the downloads through the vendor website. Using 3 workers by default"""

    num_workers = 3
    batch_size = len(hotels) // num_workers
    remainder = len(hotels) % num_workers

    hotel_batches = []

    start_index = 0
    for i in range(num_workers):
        end_index = start_index + batch_size + (1 if i < remainder else 0)
        hotel_batches.append(hotels[start_index:end_index])
        start_index = end_index

    initiate_multiprocess(func=download_main, iterable=hotel_batches)


def download_main(hotels):
    """Main function for downloading from the vendor website, this is so each multiprocess worker has it's own driver
    and batch of hotels to work through."""

    USERNAME, PASSWORD = get_env_details()
    driver = create_webdriver()
    driver.get("mainwebsite.com")

    login_to_site(driver, USERNAME, PASSWORD)
    for hotel in hotels:
        select_hotel_for_download(driver, hotel)
        try:
            download_differentials(driver, hotel)
        except CustomException:
            logger.error(f"Unable to download or validate file for {hotel}")
    driver.close()


def get_hotels_for_query() -> list:
    """Logins into the vendor site to scrape the available hotels for further processing."""

    driver = create_webdriver()
    driver.get("mainwebsite.com")

    USERNAME, PASSWORD = get_env_details()
    login_to_site(driver, USERNAME, PASSWORD)

    available_hotels = get_available_n2p_hotels(driver)

    return ", ".join(['"' + hotel + '"' for hotel in available_hotels])


def select_hotel_for_download(driver, hotel: str):
    """Types in the hotel code on the page search bar."""

    hotel_selection = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                "//kendo-searchbar//input",
            )
        )
    )

    time.sleep(1)
    hotel_selection.clear()
    time.sleep(1)
    hotel_selection.send_keys(hotel)
    time.sleep(1)
    hotel_selection.send_keys(Keys.RETURN)
    time.sleep(1)


@retry(delay=2, tries=2, backoff=2)
def download_differentials(driver: webdriver, hotel: str):
    """Inputting the provided hotel into the search bar, loading the page, and then clicking the download button for the required report. We then validate that the report has downloaded, if not we raise the CustomException which kicks off the retry decorator for additional attempts."""

    try:
        button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    '//*[@id="home"]/div[3]/app-dynamic-diff/div/div[2]/div/div/button[2]',
                )
            )
        )

        button.click()

    except Exception:
        logger.warning("Timeout of Vendor website occurred")
        raise CustomException  # Kick off retry

    try:
        validate_file = validate_file_download(hotel)
        if not validate_file:
            logger.warning(f"Download failed for {hotel}, trying again")
            select_hotel_for_download(
                driver, hotel
            )  # Ensure the right hotel is selected before retry
            raise FileNotFoundError
    except FileNotFoundError:
        raise CustomException  # Kick off retry


if __name__ == "__main__":
    pass

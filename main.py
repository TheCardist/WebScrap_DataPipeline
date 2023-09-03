from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from utils import (
    validate_lst_optimizations,
    validate_file_download,
    CustomException,
    logger,
)
from load_data import load_to_bigquery
import time
import os
from retry import retry
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# Credentials to login to N2P Site
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")


def create_webdriver() -> webdriver:
    """Setup chrome driver for Selenium, running headless mode with limited logs and custom download destination."""

    chrome_binary_path = "/usr/bin/google-chrome"
    prefs = {"download.default_directory": "./downloads"}
    options = Options()
    options.add_experimental_option("prefs", prefs)
    options.binary_location = chrome_binary_path
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--diable-gpu")
    options.add_argument("--log-level=3")

    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()), options=options
    )
    return driver


def login_to_site(driver: webdriver):
    """Login to the selected website using environment credentials and load differential page."""

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
    time.sleep(4)

    # Go to destination page of the website.
    driver.get("<destinationURL>")


def select_hotel(driver: webdriver, hotel: str):
    """Type in the hotel code in the search bar of the website to return appropriate screen."""
    try:
        hotel_selection = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable(
                (By.XPATH, '//kendo-searchbar//input[@class="k-input"]')
            )
        )
    except Exception:
        logger.error(f"Unable to select {hotel}")

    # Selecting the hotel and waiting for the page to load. Added sleep between each to prevent errors if the site isn't loading quickly.
    hotel_selection.clear()
    time.sleep(1)
    hotel_selection.send_keys(hotel)
    time.sleep(2)
    hotel_selection.send_keys(Keys.RETURN)


@retry(delay=2, tries=3, backoff=2)
def download_differentials(driver: webdriver, hotel: str):
    """Navigating to and waiting for differentials page to load before clicking the download button. Sleep at the end is for the download to finish completely. This function retries 3 times to download the files for the hotel and if it finally cannot it goes to CustomException which passes so we move on to the next hotel."""

    try:
        button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    '//*[@id="home"]/div[3]/app-dynamic-diff/div/div[2]/div/div/button[2]',
                )
            )
        )
        button.click()
        time.sleep(6)  # Give enough time for the download to finish before moving on

    except Exception:
        logger.error("TimeOut occurred on selected Website")
        raise CustomException

    try:
        validate_file = validate_file_download(hotel)

        if not validate_file:
            logger.warning(f"Download failed for {hotel}, trying again.")
            select_hotel(driver, hotel)
            raise FileNotFoundError
    except FileNotFoundError:
        raise CustomException


if __name__ == "__main__":
    hotel_codes = validate_lst_optimizations()

    if hotel_codes is None:
        logger.info("No hotels to update at this time.")
    else:
        driver = create_webdriver()
        driver.get("<loginURL>")

        login_to_site(driver)

        for hotel in hotel_codes["hotel_cd"]:
            select_hotel(driver, hotel)
            try:
                download_differentials(driver, hotel)
            except CustomException:
                logger.error(f"Unable to download or validate file for {hotel}")

        driver.close()

        contents = os.listdir("./downloads")

        # Only proceed if at least 1 file was downloaded.
        if contents:
            logger.debug(f"File contents: {contents}")
            load_to_bigquery(hotel_codes["hotel_cd"])
        else:
            logger.warning("No files were found for upload.")

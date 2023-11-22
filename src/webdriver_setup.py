# Third Party
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from dotenv import load_dotenv

# Standard library
import os


def get_env_details() -> str:
    """Setup environment details and return them for use."""

    load_dotenv()

    USERNAME = os.getenv("VENDORUSERNAME")
    PASSWORD = os.getenv("VENDORPASSWORD")

    return USERNAME, PASSWORD


def create_webdriver() -> webdriver:
    """Create webdriver for selenium web scraping."""

    chrome_binary_path = "/usr/bin/google-chrome"

    prefs = {"download.default_directory": "./data/downloads"}

    options = Options()
    options.add_experimental_option("prefs", prefs)
    options.binary_location = chrome_binary_path
    options.add_argument("--no-sandbox")
    options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("log-level=3")

    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()), options=options
    )

    return driver


if __name__ == "__main__":
    pass

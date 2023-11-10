import time
from src.unified_crowler import UnifiedCrowler
import undetected_chromedriver as uc
from config import config
import pandas as pd


def __init_driver(allow_options: bool = True):
    if allow_options:
        options = __init_options()
        driver = uc.Chrome(
            options=options,
            driver_executable_path=config.local.driver_dir,
            # headless=True,
            version_main=109)
    else:
        driver = uc.Chrome(
            driver_executable_path=config.local.driver_dir)
    return driver


def __init_options() -> uc.ChromeOptions:
    chrome_options = uc.ChromeOptions()
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-popup-blocking")
    return chrome_options


def main():
    exclude = [
        "youtube", "instagram", "twitter", "youtube", "facebook",
        "threads", "google", "bing", "yahoo", "yandex", "tiktok",
        "slack", "microsoft", "cisco", "gitlab", "github", "amazone",
        "youtu", "coca-cola", "canon", "graphql"
        ]
    df = pd.read_csv("./data/cleaned_urls.csv")
    driver = __init_driver()
    for index, row in df.iterrows():
        # Accessing columns by name
        url = row['urls']
        checked = row['checked']
        if not checked:
            time.sleep(5)
            crowler = UnifiedCrowler(config, f"https://{url}", url, exclude, driver)
            crowler.run()


if __name__ == "__main__":
    main()

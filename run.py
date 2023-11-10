from undetected_chromedriver.reactor import asyncio
from src.crowler import Crowler
from src.unified_crowler import UnifiedCrowler
from config import config
from time import time
import pandas as pd
import undetected_chromedriver as uc


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


def run_uni_crowler(file_path: str):
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
            try:
                crowler = UnifiedCrowler(
                    config, f"https://{url}", url, exclude, driver)
                crowler.run()
            except Exception:
                continue


async def main():
    crowler = Crowler(config)
    file_path = await crowler.run()
    if file_path:
        run_uni_crowler(file_path)


if __name__ == "__main__":
    asyncio.run(main())

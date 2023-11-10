from undetected_chromedriver.reactor import asyncio
from src.crowler import Crowler
from src.unified_crowler import UnifiedCrowler
from config import config
import time
import pandas as pd
import undetected_chromedriver as uc


def get_latest_file(dir: str = "data\\") -> Union[str, None]:
    files = [f for f in os.listdir("data")]

    if not files:
        return None
    latest_file = None
    latest_date = None

    for file in files:
        try:
            file_date = datetime.strptime(file[13:-4], "%Y-%m-%d_%H-%M-%S")
            if not latest_date:
                latest_date = file_date
                latest_file = file
            if file_date > latest_date:
                latest_date = file_date
                latest_file = file
        except Exception as e:
            logging.error(e)

    return latest_file


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
    df = pd.read_csv(file_path)
    driver = __init_driver()
    for index, row in df.iterrows():
        # Accessing columns by name
        url = row['urls']
        checked = row['checked']
        print(url, checked)
        if not checked:
            time.sleep(5)
            print(2)
            try:
                crowler = UnifiedCrowler(
                    config=config, 
                    url=f"https://{url}", 
                    domain=url, 
                    exclued=exclude, 
                    driver=driver, 
                    file_dir=file_path)
                crowler.run()
            except Exception as e:
                print(e)
                continue

if __name__ == "__main__":
    file_dir = get_latest_file()
    run_uni_crowler(file_dir)
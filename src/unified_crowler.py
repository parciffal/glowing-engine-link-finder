from os import link
import time
from typing import Any, List, Set, Union
from pprint import pprint
from aiogram import Bot
from config import Config
import undetected_chromedriver as uc
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup as bs
import requests as re
import pandas as pd
from urllib.parse import urlparse
import re as rgs
from datetime import datetime

from src.utils.url_queue import URLQueue


def extract_domain(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc


class UnifiedCrowler:
    def __init__(self,
                 config: Config,
                 url: str,
                 domain: str,
                 exclued: List[str],
                 driver: uc.Chrome,
                 allow_options: bool = True,
                 file_dir: str = "./data/cleaned_urls.csv") -> None:
        self._config = config
        self.__url = url
        self.__domain = domain
        # self.__init_driver(allow_options)
        self.__driver = driver
        self._home_urls: Set[str] = set()
        self._current_links: Set[str] = set()
        self._link_queue = URLQueue()
        self.__exclued = exclued
        self.running = False
        self.__file_dir = file_dir
        self.__outgoing_urls: Set[str] = set()

    def __init_driver(self, allow_options: bool) -> None:
        if allow_options:
            options = self.__init_options()
            self.__driver = uc.Chrome(
                options=options,
                driver_executable_path=self._config.local.driver_dir,
                # headless=True,
                version_main=109)
        else:
            self.__driver = uc.Chrome(
                driver_executable_path=self._config.local.driver_dir)

    def __init_options(self) -> uc.ChromeOptions:
        chrome_options = uc.ChromeOptions()
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        return chrome_options

    def __get_page_main(self) -> bool:
        self.__driver.get(self.__url)
        time.sleep(2)
        return True

    def save_file(self, df: pd.DataFrame) -> str:
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
        cleaned_csv_file_path = f'./data/cleaned_urls_{formatted_datetime}.csv'

        def remove_www(url):
            if url.startswith("www."):
                return url[4:]
            return url

        def clean_subdomains_from_url(url):
            pattern = r'(?:\w+\.)?([\w-]+\.\w+)'
            cleaned_url = rgs.sub(
                pattern, r'\1', url)
            return cleaned_url

        df['urls'] = df['urls'].apply(remove_www)
        df['urls'] = df['urls'].apply(clean_subdomains_from_url)

        df = df.drop_duplicates() 
        df.to_csv(cleaned_csv_file_path, index=False)
        return cleaned_csv_file_path

    async def send_csv_to_waiters(self, bot: Union[Bot, None], file_path: Union[str, None]):
        if bot and file_path:
            for i in self._config.waiters_set:
                await bot.send_document(i, file_path)

    def check_excluded(self, url: str) -> bool:
        for i in self.__exclued:
            if i in url:
                return False
        return True

    def set_linked_in(self, linked_in_url: str) -> bool:
        df = pd.read_csv(self.__file_dir)
        target_url = self.__domain
        target_row = df[df['urls'] == target_url]
        if not target_row.empty:
            df.loc[df['urls'] == target_url, 'linked_in'] = linked_in_url
            df.to_csv(self.__file_dir, index=False)
            return True
        return False

    def set_url_checked(self) -> bool:
        df = pd.read_csv(self.__file_dir)
        target_url = self.__domain
        target_row = df[df['urls'] == target_url]
        if not target_row.empty:
            df.loc[df['urls'] == target_url, "checked"] = True
            df.to_csv(self.__file_dir, index=False)
            return True
        return False

    def get_all_urls_from_page(self, blog_url: str, count: int = 0):
        print(blog_url)
        if "https://" in blog_url or "http://" in blog_url:
            self.__driver.get(blog_url)
        else:
            self.__driver.get(self.__url+blog_url)
        time.sleep(1)
        page_source = self.__driver.page_source
        page_bs = bs(page_source, "html.parser")
        a_hrefs = page_bs.find_all("a", href=True)
        urls_list: List[Union[str, None]] = [a.get('href') for a in a_hrefs if self.check_excluded(a.get("href")) and a.get('href') not in self.__home_urls]
        ingoing_urls: Set[str] = set()
        for a in urls_list:
            if not a or a.startswith("#"):
                continue
            elif a in self.__domain or self.__domain in a or a.startswith("/"):
                ingoing_urls.add(a)
            else:
                self.__outgoing_urls.add(a)
        ingoing_urls = ingoing_urls.difference(self.__home_urls)
        self._link_queue.add_from_set(ingoing_urls)
        next_link = self._link_queue.get()
        if not next_link:
            return self.set_url_checked()
        if count > 500:
            return self.set_url_checked()
        count += 1
        return self.get_all_urls_from_page(next_link, count)

    def get_all_home_page_urls(self):
        page_source = self.__driver.page_source
        page_bs = bs(page_source, "html.parser")
        a_hrefs = page_bs.find_all("a", href=True)
        home_urls = [a.get('href') for a in a_hrefs if self.check_excluded(a.get("href"))]
        search_term = "blog"
        self.__home_urls = set(home_urls)
        if "blog" in self.__driver.current_url:
            ingoing_urls: Set[str] = set()
            for a in self.__home_urls:
                if not a :
                    continue
                elif a in self.__domain or self.__domain in a or str(a).startswith("#") or not str(a).startswith("mailto") or "comment" not in str(a):
                    ingoing_urls.add(a)
            self._link_queue.add_from_set(ingoing_urls)
        blog_urls = [s for s in home_urls if search_term in s]

        linked_in_url = [s for s in home_urls if "linkedin" in s]
        if linked_in_url:
            print("Check linked in")
            self.set_linked_in(linked_in_url[0])
        if blog_urls:
            sorted_blog_urls = sorted(blog_urls, key=len, reverse=True)
            print("Found blog")
            self.get_all_urls_from_page(sorted_blog_urls[0])
        else:
            print("Set checked")
            self.set_url_checked()

    def upped_new_outgoing_urls(self) -> None:
        """
        Append self.__outgoing_urls to an existing CSV file or create a new one.

        Parameters:
            file_dir (str): The directory path for the CSV file.
        """
        file_dir = self.__file_dir
        try:
            # Read existing CSV file if it exists
            existing_df = pd.read_csv(file_dir)
        except FileNotFoundError:
            # If the file doesn't exist, create an empty DataFrame
            existing_df = pd.DataFrame(columns=['urls', 'linked_in', 'checked'])
        # Create a new DataFrame for self.__outgoing_urls
        outgoing_df = pd.DataFrame({
            'urls': list(self.__outgoing_urls),
            'linked_in': '',
            'checked': False
        })

        def remove_www(url):
            if url.startswith("www."):
                return url[4:]
            return url

        def clean_subdomains_from_url(url):
            pattern = r'(?:\w+\.)?([\w-]+\.\w+)'
            cleaned_url = rgs.sub(
                pattern, r'\1', url)
            return cleaned_url
        outgoing_df['urls'] = outgoing_df['urls'].apply(extract_domain)
        outgoing_df['urls'] = outgoing_df['urls'].apply(remove_www)
        outgoing_df['urls'] = outgoing_df['urls'].apply(clean_subdomains_from_url)
        outgoing_df = outgoing_df.drop_duplicates()
        print(self.__domain, f" Outgoing Links: {len(outgoing_df)}")
        # Concatenate existing and new DataFrames
        updated_df = pd.concat([existing_df, outgoing_df], ignore_index=True)
        updated_df = updated_df.drop_duplicates()
        print(self.__domain, f" Cleaned Links: {len(updated_df)}")
        # Save the updated DataFrame back to the CSV file
        updated_df.to_csv(file_dir, index=False)

    def run(self) -> Union[bool, None]:
        self.running = True
        self.__get_page_main()
        self.get_all_home_page_urls()
        self.upped_new_outgoing_urls()

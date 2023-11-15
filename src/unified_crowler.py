from inspect import isdatadescriptor
import time
from typing import List, Set, Union
from aiogram import Bot
import undetected_chromedriver as uc
from bs4 import BeautifulSoup as bs
import requests as re
import pandas as pd
from urllib.parse import urlparse
import re as rgs
from pprint import pprint
from datetime import datetime
from src.utils.url_queue import URLQueue
from config import Config

import threading
import time

def timeout_decorator(timeout):
    def decorator(func):
        def wrapper(*args, **kwargs):
            result = None
            exception = None

            def worker():
                nonlocal result, exception
                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    exception = e

            thread = threading.Thread(target=worker)
            thread.start()
            thread.join(timeout=timeout)

            if thread.is_alive():
                thread.join()  # Wait for the thread to finish
                raise TimeoutError(f"{func.__name__} took more than {timeout} seconds and was terminated.")

            if exception is not None:
                raise exception

            return result

        return wrapper

    return decorator


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
        self.__domain = domain.split(".")[0]+"."
        self.__domain_full = domain
        # self.__init_driver(allow_options)
        self.__driver = driver
        self._home_urls: Set[str] = set()
        self._current_links: Set[str] = set()
        self._link_queue = URLQueue(exclued)
        self.__excluded = exclued
        self.running = False
        self.__file_dir = file_dir
        self.__outgoing_urls: Set[str] = set()
        self.__ingoing_urls: Set[str] = set()

    def get_url_http(self, url: str):
        request = re.get(url)
        html = request.content
        return html

    def __init_driver(self, allow_options: bool) -> None:
        if allow_options:
            options = self.__init_options()
            self.__driver = uc.Chrome(
                options=options,
                driver_executable_path=self._config.local.driver_dir,
                headless=True,
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
        return not any(excluded_value in url for excluded_value in self.__excluded)

    def set_linked_in(self, linked_in_url: str) -> bool:
        df = pd.read_csv(self.__file_dir)
        target_url = self.__domain_full
        target_row = df[df['urls'] == target_url]
        if not target_row.empty:
            df.loc[df['urls'] == target_url, 'linked_in'] = linked_in_url
            df.to_csv(self.__file_dir, index=False)
            return True
        return False

    def set_url_checked(self) -> bool:
        df = pd.read_csv(self.__file_dir)
        target_url = self.__domain_full
        target_row = df[df['urls'] == target_url]
        if not target_row.empty:
            df.loc[df['urls'] == target_url, "checked"] = True
            df.to_csv(self.__file_dir, index=False)
            return True
        return False

    #@timeout_decorator(timeout=60)
    def get_all_urls_from_page(self, blog_url: str):
        try:
            if "https://" in blog_url or "http://" in blog_url:
                self.__driver.get(blog_url)
            else:
                self.__driver.get(self.__url + blog_url)
            self.__driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            time.sleep(2)
            page_source = self.__driver.page_source
            
            page_bs = bs(page_source, "html.parser")
            a_hrefs = page_bs.find_all("a", href=True)
            
            urls = [a.get("href") for a in a_hrefs]
            urls_list = [a for a in urls if a not in self.__home_urls]
            
            for a in urls_list:
                if self.__domain.lower() in a.lower() or a.startswith("/"):
                    self._link_queue.add(a)
                else:
                    if not a.startswith("#"):
                        self.__outgoing_urls.add(a)
            print(f"Out: {len(self.__outgoing_urls)}")
            next_link = self._link_queue.get()
            if not next_link:
                return self.set_url_checked()
            return self.get_all_urls_from_page(next_link)
        except Exception as e:
            print(e)
            try:
                next_link = self._link_queue.get()
                if not next_link:
                    return self.set_url_checked()
                return self.get_all_urls_from_page(next_link)
            except Exception as e:
                print(e)
                return self.set_url_checked()

    def get_all_home_page_urls(self):
        page_source = self.__driver.page_source
        page_bs = bs(page_source, "html.parser")
        a_hrefs = page_bs.find_all("a", href=True)
        home_urls = [a.get('href') for a in a_hrefs]
        search_term = "blog"
        self.__home_urls = set(home_urls)
        self.__home_urls.add(self.__driver.current_url)
        if "blog" in self.__driver.current_url:
            ingoing_urls: Set[str] = set()
            for a in self.__home_urls:
                if not a :
                    continue
                elif self.__domain.lower() in a.lower() or str(a).startswith("#"):
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

    def add_statistic(self, domain, outgoing, ingoing, time_s, added):
        max_length = max(len(outgoing), len(ingoing), 1, len(added))
        out = list(outgoing) + [""] * (max_length - len(outgoing))
        inr = list(ingoing) + [""] * (max_length - len(ingoing))
        add = list(added) + [""] * (max_length - len(added))
        # Create a DataFrame
        statistic = pd.DataFrame({
            "outgoing": out,
            "ingoing": inr,
            "time": [time_s] * max_length,  # Repeat 'time' to match the length
            "added": add,
        })
        statistic.to_csv(f"./statistic/{domain}csv", index=False)
        stats = pd.read_csv("stats.csv")
        stats.loc[len(stats)] = {
                "domain": self.__domain_full,
                "outgoing": len(outgoing),
                "ingoing": len(ingoing),
                "added": len(added),
                "time": time_s,
                "file": f"./statistic/{domain}csv"
                }
        stats.to_csv("stats.csv", index=False)



    def clean_outgoing(self) -> Union[pd.DataFrame, None]:
        out = [a for a in self.__outgoing_urls if not any(excluded_value in a for excluded_value in self.__excluded)]
        outgoing_df = pd.DataFrame({
            'urls': list(out),
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
        used_urls_df = pd.read_csv("./no_domains.csv")
        filtered_df = outgoing_df[~outgoing_df['urls'].isin(used_urls_df['urls'])]

        filtered_df = filtered_df.drop_duplicates()
        return filtered_df

    def upped_new_outgoing_urls(self, time_dif) -> None:
        """
        Append self.__outgoing_urls to an existing CSV file or create a new one.

        Parameters:
            file_dir (str): The directory path for the CSV file.
        """
        stat_in = self._link_queue.used_links | set(self._link_queue.urls)
        stat_out = self.__outgoing_urls

        file_dir = self.__file_dir
        try:
            existing_df = pd.read_csv(file_dir)
        except FileNotFoundError:
            existing_df = pd.DataFrame(columns=['urls', 'linked_in', 'checked'])

        outgoing_df = self.clean_outgoing()
        print(self.__domain, f" Outgoing Links: {len(outgoing_df)}")
        # Concatenate existing and new DataFrames
        
        updated_df = pd.concat([existing_df, outgoing_df], ignore_index=True)
        updated_df = updated_df.drop_duplicates()
        
        new_urls_df = pd.merge(outgoing_df, existing_df, on='urls', how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
        print(len(new_urls_df))
        updated_df.to_csv(file_dir, index=False)

        self.add_statistic(domain=self.__domain, 
                           outgoing=stat_out,
                           ingoing=stat_in,
                           added=new_urls_df["urls"],
                           time_s=time_dif)


    def run(self) -> Union[bool, None]:
        try:
            self.running = True
            print(1)
            self.__get_page_main()
            now = datetime.now()
            self.get_all_home_page_urls()
            end = datetime.now()
            time_dif = end - now
            time_dif = str(time_dif).split(".")[0]
            self.upped_new_outgoing_urls(time_dif)
        except Exception as e:
            print(e)

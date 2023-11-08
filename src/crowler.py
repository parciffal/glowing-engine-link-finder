import time
from typing import Any, List, Union

from aiogram import Bot
from config import Config
import undetected_chromedriver as uc
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup as bs
import requests as re
import pandas as pd
from urllib.parse import urlparse
import asyncio
import aiohttp
import re as rgs
from datetime import datetime


def extract_domain(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc


class Crowler:
    def __init__(self, config: Config, allow_options: bool = True) -> None:
        self._config = config
        # self.__init_driver(allow_options)
        self.__links = set()
        self.running = False

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
        try:
            self.__driver.get(self._config.url.hubspot_blog_url)
            return True
        except Exception as e:
            repr(e)
            return False

    def __get_blog_page_categories(self) -> Union[List[str], None]:
        try:
            self.__driver.get(self._config.url.hubspot_blog_url)
            blog_names = self.__driver.find_element(
                uc.By.XPATH,
                self._config.xpath.blog_categorys)
            html = blog_names.get_attribute("innerHTML")
            cat_names = bs(html, "html.parser")
            name_links: List[str] = []
            na = cat_names.find_all({"li"})
            for i in na:
                if i:
                    link = i.find('a', href=True)
                    name_links.append(link['href'])
            return name_links
        except Exception as e:
            repr(e)

    def __get_page(self, url: str):
        try:
            self.__driver.get(url)
        except Exception as e:
            repr(e)

    async def __get_single_article_links(self, url):
        try:
            response, status = await self.__async_get(url)
            if status == 200:
                html = response
                post = bs(html, "html.parser")
                try:
                    post_body = post.find("div", {'class': 'hsg-rich-text blog-post-body'})
                    a_links = post_body.find_all("a", href=True)
                except AttributeError:
                    try:
                        post_body = post.find("div", {'class': 'hsg-rich-text__wrapper'})
                        a_links = post_body.find_all("a", href=True)
                    except AttributeError:
                        pass
                for a in a_links:
                    if "hubspot.com" not in a['href'].lower() and a['href'].startswith("https:"):
                        self.__links.add(extract_domain(a['href']))
        except Exception as e:
            time.sleep(40)
            print("129: ", e)

    async def __async_get(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                html = await response.text()
                status = response.status
                return html, status

    def __get_blog_articles(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.__async_get_blog_articles())

    async def __async_get_blog_articles(self):
        try:
            art_els = self.__driver.find_element(
                uc.By.XPATH,
                self._config.xpath.blog_articles
            )
            html = bs(art_els.get_attribute("innerHTML"), "html.parser")
            na = html.find_all("li")
            article_links = [i.find('a', href=True).get('href') for i in na]
            tasks = [self.__get_single_article_links(article) for article in article_links]
            await asyncio.gather(*tasks)
            print(len(self.__links))
        except Exception as e:
            print("155: ", e)

    def __scrap_articles(self, categoies: List[str]) -> None:
        try:
            for category in categoies:
                self.__get_page(category)
                time.sleep(1)
                first_button = self.__driver.find_element(
                    uc.By.XPATH,
                    self._config.xpath.first_button
                )
                first_button.click()
                for i in range(5):
                    self.__get_blog_articles()
                    try:
                        self.__driver.find_element(
                            uc.By.XPATH,
                            self._config.xpath.blog_article_next).click()
                    except NoSuchElementException:
                        break
        except Exception as e:
            print(repr(e))

    def save_file(self, df: pd.DataFrame):
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

    async def run(self, bot: Union[Bot, None]) -> Union[bool, None]:
        try:
            self.running = True
            # now = time.time()
            # self.__get_page_main()
            # blog_pages = self.__get_blog_page_categories()
            blog_pages = [
                    "https://blog.hubspot.com/marketing/page/",
                    "https://blog.hubspot.com/sales/page/",
                    "https://blog.hubspot.com/service/page/",
                    "https://blog.hubspot.com/website/page/",
                    "https://blog.hubspot.com/the-hustle/page/",
                    "https://blog.hubspot.com/ai/page/"
                    ]
            if blog_pages:
                self.__scrap_articles(blog_pages)
                articles = list(self.__links)
                df = pd.DataFrame({'urls': articles})
                file_path = self.save_file(df)
                await self.send_csv_to_waiters(bot, file_path)
        except Exception as e:
            print(e)
        finally:
            self.running = False

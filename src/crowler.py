from typing import List, Union
from aiogram.types import FSInputFile
from aiogram import Bot
from config import Config
import undetected_chromedriver as uc
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


class PageNotFounfException(Exception):
    def __init__(self, message="Page not found"):
        self.message = message
        super().__init__(self.message)


class Crowler:
    def __init__(self, config: Config, allow_options: bool = True) -> None:
        self._config = config
        self.__init_driver(allow_options)
        self._links = set()
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
                a_links = None
                try:
                    post_body = post.find(
                        "div",
                        {'class': 'hsg-rich-text blog-post-body'}
                    )
                    a_links = post_body.find_all("a", href=True)
                except AttributeError:
                    try:
                        post_body = post.find(
                            "div",
                            {'class': 'hsg-rich-text__wrapper'}
                        )
                        a_links = post_body.find_all("a", href=True)
                    except AttributeError:
                        pass
                if a_links:
                    for a in a_links:
                        if "hubspot.com" in a['href'].lower() and a['href'].startswith("https:"):
                            continue
                        self._links.add(extract_domain(a['href']))
            else:
                raise PageNotFounfException
        except PageNotFounfException:
            return None
        except Exception as e:
            print("99", e)

    async def __async_get(self, url):
        await asyncio.sleep(1)
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                html = await response.text()
                status = response.status
                return html, status

    async def __async_get_blog_articles(self, url):
        try:
            page_source, status = await self.__async_get(url)
            await asyncio.sleep(2)
            print(status)
            if status != 200:
                return None
            html = bs(page_source, "html.parser")
            blog_post_list = html.find("ul", {'class': 'blog-post-list'}) 
            na = blog_post_list.find_all("li")
            article_links = [i.find('a', href=True).get('href') for i in na]
            tasks = [self.__get_single_article_links(article) for article in article_links]
            await asyncio.gather(*tasks)
            print(len(self._links))
        except PageNotFounfException:
            return None
        except Exception as e:
            print("121", e)

    async def __scrap_articles(self, categoies: List[str]) -> None:
        try:
            for category in categoies:
                for i in range(1, 30):
                    try:
                        print(category+str(i))
                        await self.__async_get_blog_articles(category+str(i))
                    except PageNotFounfException:
                        break
                    except Exception as e:
                        print(e)
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
                file = FSInputFile(file_path)
                await bot.send_document(i, file)

    async def run(self, bot: Union[Bot, None]) -> Union[bool, None]:
        try:
            self.running = True
            blog_pages = [
                    "https://blog.hubspot.com/marketing/page/",
                    "https://blog.hubspot.com/sales/page/",
                    "https://blog.hubspot.com/service/page/",
                    "https://blog.hubspot.com/website/page/",
                    "https://blog.hubspot.com/the-hustle/page/",
                    "https://blog.hubspot.com/ai/page/",
                    ]
            if blog_pages:
                await self.__scrap_articles(blog_pages)
                articles = list(self._links)
                df = pd.DataFrame(
                    {
                     'urls': articles,
                     'linked_in': "",
                     "checked": False
                    }
                )
                file_path = self.save_file(df)
                await self.send_csv_to_waiters(bot, file_path)
        except Exception as e:
            print(e)
        finally:
            self.running = False

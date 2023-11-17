from aiogram.types import (BotCommand, FSInputFile,
                           Message, BotCommandScopeDefault)
from aiogram.filters import Command
from aiogram import Bot, Dispatcher, Router
import aiohttp
import asyncio

from typing import Union, List
import os
from datetime import datetime
import logging
from logging.config import DictConfigurator
import coloredlogs

import undetected_chromedriver as uc
from bs4 import BeautifulSoup as bs
import pandas as pd
from urllib.parse import urlparse
import re as rgs

from src.config import Config


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
            print(e)

    async def __async_get(self, url):
        try:
            await asyncio.sleep(1)
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    html = await response.text()
                    status = response.status
                    return html, status
        except Exception:
            return "", 404

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
            print(e)

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
            print(e)

    def save_file(self, df: pd.DataFrame) -> Union[str, None]:
        try:
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
        except Exception as e:
            print(e)
            return None

    async def send_csv_to_waiters(self, bot: Union[Bot, None], file_path: Union[str, None]):
        try:
            if bot and file_path:
                for i in self._config.waiters_set:
                    file = FSInputFile(file_path)
                    await bot.send_document(i, file)
        except Exception as e:
            print(e)

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



def get_latest_file(dir: str = "./data") -> Union[str, None]:
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


logging_config = DictConfigurator(
    {
        "version": 1,
        "formatters": {"standard": {"format": "%(asctime)s - %(message)s"}},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "standard",
                "level": "INFO",
            }
        },
        "root": {"handlers": ["console"], "level": "INFO"},
    }
)


class CrowlerBot:
    def __init__(self, config: Config) -> None:
        self._config: Config = config
        self._bot: Union[Bot, None] = None
        self.__dp = None
        self._router = None
        self._crowler = Crowler(config=config)

    def register_router(self) -> Router:
        router = Router()
        router.message.register(self.start_cmd, Command('start'))
        router.message.register(self.scrap_cmd, Command('scrap'))
        router.message.register(self.health_cmd, Command('health'))
        router.message.register(self.export_cmd, Command('export'))
        return router

    async def setup_bot_commands(self, bot: Bot):
        users_commands = {
            "start": "🤖 Get information about the bot",
            "scrap": "🌐 Start scraping domains",
            "health": "❤️ Check the bot's health",
            "export": "📦 Export the latest results",
        }
        await bot.set_my_commands(
            [
                BotCommand(command=command, description=description)
                for command, description in users_commands.items()
            ],
            scope=BotCommandScopeDefault(),
        )
        pass

    async def setup_bot(self):
        coloredlogs.install(level=logging.INFO)
        logging_config.configure()
        self._bot = Bot(token=self._config.local.bot_token, parse_mode="HTML")
        self.__dp = Dispatcher()
        self._router = self.register_router()
        self.__dp.include_router(self._router)
        await self.setup_bot_commands(self._bot)
        logging.info("Bot start")
        await self.__dp.start_polling(self._bot)

    def start_polling(self):
        try:
            asyncio.run(self.setup_bot())
        except (KeyboardInterrupt, SystemExit):
            logging.error("Bot stopped")

    async def start_cmd(self, message: Message):
        try:
            msg_text = (
                f"👋 Hi, {message.chat.first_name}!"
                f" I'm your friendly Crowler Bot.\n\n"
                f"Here's what I can do for you:\n"
                f"▶️ To start scraping, simply send the /scrap command.\n"
                f"📤 To export the latest results, use the /export command.\n"
                f"💉 To check my health and see if I'm currently working,"
                f" send the /health command.\n"
            )
            await message.answer(text=msg_text)
        except Exception as e:
            logging.error(e)

    async def scrap_cmd(self, message: Message):
        try:
            self._config.waiters_set.add(message.chat.id)
            if not self._crowler.running:
                msg_text = (
                    "🛠️ I've started scraping! To check my work status and"
                    " see how many results I've collected, send /health."
                )
                asyncio.create_task(self._crowler.run(self._bot))
            else:
                msg_text = (
                    f"🔄 I'm already hard at work! I'm currently "
                    f"scraping results from {len(self._crowler._links)}"
                    f" sources. Keep an eye out for the latest data!")
            await message.answer(msg_text)
        except Exception as e:
            logging.error(e)

    async def health_cmd(self, message: Message):
        try:
            self._config.waiters_set.add(message.chat.id)
            if self._crowler.running:
                msg_text = (f"⚙️ I'm still in the midst of scraping!"
                            f"Currently, I'm processing results from"
                            f"{len(self._crowler._links)} sources."
                            f"Stay tuned for the latest updates!")
            else:
                msg_text = "I'm not scrapping now"
            await message.answer(text=msg_text)
        except Exception as e:
            logging.error(e)

    async def export_cmd(self, message: Message):
        try:
            latest_file = get_latest_file(self._config.local.download_dir)
            if latest_file:
                file_path = os.path.join(
                    self._config.local.download_dir,
                    latest_file)
                file = FSInputFile(file_path)
                await message.answer_document(
                    document=file,
                    caption="Here is latest results")
            else:
                msg_text = "Sorry i have no file to send\nTry /scrap"
                await message.answer(msg_text)
        except Exception as e:
            logging.error(e)

    async def scrap_all_cmd(self, message: Message):
        try:
            pass
        except Exception as e:
            logging.error(e)

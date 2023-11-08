from typing import Union
from aiogram.types import BotCommand, FSInputFile, Message, BotCommandScopeDefault
from aiogram.filters import Command
from config import Config
from src.crowler import Crowler
from aiogram import Bot, Dispatcher, Router
import asyncio
import os
from datetime import datetime
import logging
from logging.config import DictConfigurator
import coloredlogs


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
            "start": "Get info about bot",
            "scrap": "Start scraping domains",
            "health": "Check healt",
            "export": "Export last result"}

        await bot.set_my_commands(
            [
                BotCommand(command=command, description=description)
                for command, description in users_commands.items()
            ],
            scope=BotCommandScopeDefault(type="default"),
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
        msg_text = f"Hi {message.chat.first_name} i'm crowler bot\nTo start scraping send /scrap command\nTo export last result send /export\nTo check health send /health command"
        await message.answer(text=msg_text)

    async def scrap_cmd(self, message: Message):
        self._config.waiters_set.add(message.chat.id)
        if not self._crowler.running:
            msg_text = "I start to scrap to check my work status send /health"
            await self._crowler.run(self._bot)
        else:
            msg_text = f"I'm already scrapping results now {len(self._crowler.__links)}"
        await message.answer(msg_text)

    async def health_cmd(self, message: Message):
        self._config.waiters_set.add(message.chat.id)
        if self._crowler.running:
            msg_text = f"I'm steel scrapping results now {len(self._crowler.__links)}"
        else:
            msg_text = "I'm not scrapping now"
        await message.answer(text=msg_text)

    async def export_cmd(self, message: Message):
        latest_file = get_latest_file(self._config.local.download_dir)
        print(latest_file)
        if latest_file:
            file_path = os.path.join(self._config.local.download_dir, latest_file)
            print(file_path)
            file = FSInputFile(file_path)
            print(2)
            await message.answer_document(document=file, caption="Here is latest results")
        else:
            await message.answer("Sorry i have no file to send\nTry /scrap")

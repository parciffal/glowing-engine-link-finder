from subprocess import run
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
        try:
            msg_text = f"👋 Hi, {message.chat.first_name}! I'm your friendly Crowler Bot.\n\n"
            msg_text += "Here's what I can do for you:\n"
            msg_text += "▶️ To start scraping, simply send the /scrap command.\n"
            msg_text += "📤 To export the latest results, use the /export command.\n"
            msg_text += "💉 To check my health and see if I'm currently working, send the /health command.\n"
        
            await message.answer(text=msg_text)
        except Exception as e:
            logging.error(e)

    async def scrap_cmd(self, message: Message):
        try:
            self._config.waiters_set.add(message.chat.id)
            if not self._crowler.running:
                msg_text = "🛠️ I've started scraping! To check my work status and see how many results I've collected, send /health."
                asyncio.create_task(self._crowler.run(self._bot))
            else:
                msg_text = f"🔄 I'm already hard at work! I'm currently scraping results from {len(self._crowler._links)} sources. Keep an eye out for the latest data!"
            await message.answer(msg_text)
        except Exception as e:
            logging.error(e)

    async def health_cmd(self, message: Message):
        try:
            self._config.waiters_set.add(message.chat.id)
            if self._crowler.running:
                msg_text = f"⚙️ I'm still in the midst of scraping! Currently, I'm processing results from {len(self._crowler._links)} sources. Stay tuned for the latest updates!"

            else:
                msg_text = "I'm not scrapping now"
            await message.answer(text=msg_text)
        except Exception as e:
            logging.error(e)

    async def export_cmd(self, message: Message):
        try:
            latest_file = get_latest_file(self._config.local.download_dir)
            if latest_file:
                file_path = os.path.join(self._config.local.download_dir, latest_file)
                file = FSInputFile(file_path)
                await message.answer_document(document=file, caption="Here is latest results")
            else:
                await message.answer("Sorry i have no file to send\nTry /scrap")
        except Exception as e:
            logging.error(e)

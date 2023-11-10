from undetected_chromedriver.reactor import asyncio
from src.crowler import Crowler
from config import config


async def main():
    crowler = Crowler(config)
    await crowler.run()

if __name__ == "__main__":
    asyncio.run(main())

from src.crowler_bot import CrowlerBot
from config import config


def main():
    bot = CrowlerBot(config=config)
    bot.start_polling()


if __name__ == "__main__":
    main()

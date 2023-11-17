import platform

from pydantic import BaseModel


class Local(BaseModel):
    page_count: int = 10
    bot_token: str = "6788175465:AAFQQbj-ZsJBN4577UhsrrxsgYhzgdOhKwk"
    download_dir: str = "../data"
    driver_dir_lin: str = "../chromedriver"
    csv_file_dir: str = "./links.csv"
    driver_dir_win: str = "../chromedriver.exe"

    @property
    def driver_dir(self) -> str:
        if platform.system() == "Windows":
            return self.driver_dir_win
        return self.driver_dir_lin


class Xpath(BaseModel):
    blog_categorys: str = """//*[@id="blogs-mega-menu-submenu"]/div/div/div/div[2]/div/ul"""
    blog_articles: str = """/html/body/div[4]/main/ul"""
    blog_article_next: str = """/html/body/div[4]/div[3]/nav/a[2]"""
    article_post_body: str = """//*[@id="blog-page-read-time"]/div[2]"""
    first_button: str = """/html/body/div[4]/div[6]/nav/div/a[1]"""


class Url(BaseModel):
    hubspot_url: str = "https://www.hubspot.com/"
    hubspot_blog_url: str = "https://blog.hubspot.com/"


class Config(BaseModel):
    local: Local = Local()
    url: Url = Url()
    xpath: Xpath = Xpath()
    waiters_set: set = set()


config = Config()

import aiohttp
from bs4 import BeautifulSoup as bs
import pandas as pd
from urllib.parse import urlparse
from datetime import datetime
import re as rgs
from pprint import pprint
from src.utils.url_queue import URLQueue


def extract_domain(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc


class UnifiedCrawlerAsync:
    def __init__(self, config, url, domain, excluded, file_dir="./data/cleaned_urls.csv"):
        self._config = config
        self.__url = url
        self.__domain = domain.split(".")[0] + "."
        self.__domain_full = domain
        self._home_urls = set()
        self._current_links = set()
        self._link_queue = URLQueue(excluded)
        self.__excluded = excluded
        self.running = False
        self.__file_dir = file_dir
        self.__outgoing_urls = set()

    async def fetch(self, session, url):
        async with session.get(url) as response:
            return await response.text()

    async def get_page_main(self):
        async with aiohttp.ClientSession() as session:
            html = await self.fetch(session, self.__url)
            return html

    async def send_csv_to_waiters(self, bot, file_path):
        if bot and file_path:
            for i in self._config.waiters_set:
                await bot.send_document(i, file_path)

    def check_excluded(self, url):
        return not any(excluded_value in url for excluded_value in self.__excluded)

    async def set_linked_in(self, linked_in_url):
        df = pd.read_csv(self.__file_dir)
        target_url = self.__domain_full
        target_row = df[df['urls'] == target_url]
        if not target_row.empty:
            df.loc[df['urls'] == target_url, 'linked_in'] = linked_in_url
            df.to_csv(self.__file_dir, index=False)
            return True
        return False

    async def set_url_checked(self):
        df = pd.read_csv(self.__file_dir)
        target_url = self.__domain_full
        target_row = df[df['urls'] == target_url]
        if not target_row.empty:
            df.loc[df['urls'] == target_url, "checked"] = True
            df.to_csv(self.__file_dir, index=False)
            return True
        return False

    async def get_all_urls_from_page(self, blog_url):
        try:
            async with aiohttp.ClientSession() as session:
                if "https://" in blog_url or "http://" in blog_url:
                    url = blog_url
                else:
                    url = self.__url + blog_url
                html = await self.fetch(session, url)

                page_bs = bs(html, "html.parser")
                a_hrefs = page_bs.find_all("a", href=True)

                urls = [a.get("href") for a in a_hrefs]
                urls_list = [a for a in urls if a not in self.__home_urls]

                for a in urls_list:
                    if self.__domain.lower() in a.lower() or a.startswith("/"):
                        self._link_queue.add(a)
                    else:
                        if not a.startswith("#"):
                            self.__outgoing_urls.add(a)
                print(f"{self.__domain}: {len(self.__outgoing_urls)} loop count: {self._link_queue.count}")
                next_link = self._link_queue.get()
                if not next_link:
                    return await self.set_url_checked()
                return await self.get_all_urls_from_page(next_link)
        except Exception as e:
            print(e)
            try:
                next_link = self._link_queue.get()
                if not next_link:
                    return await self.set_url_checked()
                return await self.get_all_urls_from_page(next_link)
            except Exception as e:
                print(e)
                return await self.set_url_checked()

    async def get_all_home_page_urls(self):
        async with aiohttp.ClientSession() as session:
            html = await self.fetch(session, self.__url)

            page_bs = bs(html, "html.parser")
            a_hrefs = page_bs.find_all("a", href=True)
            home_urls = [a.get('href') for a in a_hrefs]
            search_term = "blog"
            self.__home_urls = set(home_urls)
            self.__home_urls.add(self.__url)
            if "blog" in self.__url:
                ingoing_urls = set()
                for a in self.__home_urls:
                    if not a:
                        continue
                    elif self.__domain.lower() in a.lower() or str(a).startswith("#"):
                        ingoing_urls.add(a)
                self._link_queue.add_from_set(ingoing_urls)
            blog_urls = [s for s in home_urls if search_term in s]

            linked_in_url = [s for s in home_urls if "linkedin" in s]
            if linked_in_url:
                print("Check linked in")
                await self.set_linked_in(linked_in_url[0])
            if blog_urls:
                sorted_blog_urls = sorted(blog_urls, key=len, reverse=True)
                print("Found blog")
                await self.get_all_urls_from_page(sorted_blog_urls[0])
            else:
                print("Set checked")
                await self.set_url_checked()

    async def add_statistic(self, domain, outgoing, ingoing, time_s, added):
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
        print(f"Domain: {self.__domain_full} Out: {len(outgoing)} In: {len(ingoing)} Time: {time_s} Added: {len(added)}")
        pprint(added)
        stats.to_csv("stats.csv", index=False)

    async def clean_outgoing(self):
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

    async def upped_new_outgoing_urls(self, time_dif):
        stat_in = self._link_queue.used_links | set(self._link_queue.urls)
        stat_out = self.__outgoing_urls

        file_dir = self.__file_dir
        try:
            existing_df = pd.read_csv(file_dir)
        except FileNotFoundError:
            existing_df = pd.DataFrame(columns=['urls', 'linked_in', 'checked'])

        outgoing_df = await self.clean_outgoing()
        # Concatenate existing and new DataFrames

        updated_df = pd.concat([existing_df, outgoing_df], ignore_index=True)
        updated_df = updated_df.drop_duplicates()

        new_urls_df = pd.merge(outgoing_df, existing_df, on='urls', how='left', indicator=True).query(
            '_merge == "left_only"').drop('_merge', axis=1)
        
        updated_df.to_csv(file_dir, index=False)

        await self.add_statistic(domain=self.__domain,
                                  outgoing=stat_out,
                                  ingoing=stat_in,
                                  added=new_urls_df["urls"],
                                  time_s=time_dif)

    async def run(self):
        try:
            self.running = True
            await self.get_page_main()
            now = datetime.now()
            await self.get_all_home_page_urls()
            end = datetime.now()
            time_dif = end - now
            time_dif = str(time_dif).split(".")[0]
            await self.upped_new_outgoing_urls(time_dif)
        except Exception as e:
            print(e)


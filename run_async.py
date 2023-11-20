import asyncio
from src.classes.unified_crowler_async import UnifiedCrawlerAsync
from src.config import config
import pandas as pd

MAX_CONCURRENT_TASKS = 10  # Maximum number of concurrent tasks


async def process_links_in_batches(file_path, batch_size):
    exclude = pd.read_csv("data/excluded.csv", index_col=False)
    exclude = list(exclude["excluded"])
    df = pd.read_csv(file_path)
    urls_stack = [row.get("urls") for _, row in df.iterrows() if not row.get("checked")]
    tasks = []
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

    async def process_single_url(url):
        async with semaphore:
            try:
                await asyncio.sleep(2)
                crowler = UnifiedCrawlerAsync(
                    config=config,
                    url=f"https://{url}",
                    domain=url,
                    excluded=exclude,
                    file_dir=file_path,
                )
                await crowler.run()
            except Exception as e:
                print(e)
                pass

    index = 0
    while index < len(urls_stack):
        try:
            tasks.append(process_single_url(urls_stack[index]))
            index += 1

            if len(tasks) == batch_size or index == len(urls_stack):
                print(f"Number of running tasks: {len(asyncio.all_tasks())}")
                await asyncio.gather(*tasks)
                print(f"Number of running tasks: {len(asyncio.all_tasks())}")
                tasks = []
            print("i'm alive")
        except Exception:
            pass


if __name__ == "__main__":
    # file_dir = get_latest_file()
    file_dir = "data/your_data.csv"
    if file_dir:
        asyncio.run(process_links_in_batches(file_dir, batch_size=4))
    else:
        print("Check Files")

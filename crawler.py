import asyncio
import aiohttp
import async_timeout
from lxml import html
from timeit import default_timer as timer

from db import DBData


class Crawler:
    def __init__(self, **kwargs):
        self.domains = kwargs["domains"]
        self.max_depth = kwargs["max_depth"]
        self.max_retries = 3
        self.max_workers = 10
        self.Q = asyncio.Queue()
        self.db_Q = asyncio.Queue()
        self.cache = set()
        self.count = 0
        self.loop = asyncio.get_event_loop()
        self.db_data = DBData()

        # Clear
        self.db_data.clear_crawler()

    async def get(self, url, timeout):
        with async_timeout.timeout(timeout):
            async with self.session.get(url) as response:
                return await response.text()

    async def extract_urls(self, url, timeout=10):
        tree = html.fromstring(await self.get(url, timeout))
        # Search only in domains
        return {p for p in tree.xpath("//a/@href")}
                # if any(domain in p for domain in self.domains)}

    async def worker(self):
        while True:
            url, depth, retries = await self.Q.get()
            if url in self.cache:
                self.db_Q.put_nowait(url)
                self.Q.task_done()
                continue
            try:
                new_urls = await self.extract_urls(url)
            except Exception as e:
                if retries <= self.max_retries:
                    self.Q.put_nowait((url, depth, retries + 1))
                else:
                    print("Error in %s: %s" % (url, repr(e)))
            else:
                self.cache.add(url)
                self.count += 1
                self.db_Q.put_nowait(url)
                print("Depth: %s Retry: %s Visited: %s" % (depth, retries, url))
                if depth+1 <= self.max_depth:
                    for x in new_urls:
                        self.Q.put_nowait((x, depth + 1, retries))
            self.Q.task_done()

    async def run(self):
        async with aiohttp.ClientSession(loop=self.loop) as session:
            self.session = session
            workers = [self.worker() for _ in range(self.max_workers)]
            workers += [self.write_to_db() for _ in range(self.max_workers)]
            tasks = [self.loop.create_task(x) for x in workers]
            await asyncio.sleep(5)
            await self.Q.join()
            await self.db_Q.join()
            for task in tasks:
                task.cancel()

    def start(self):
        for domain in self.domains:
            print("Crawling %s start..." % domain)

            self.Q.put_nowait((domain, 0, 0))
            start_time = timer()
            self.loop.run_until_complete(asyncio.gather(self.run()))
            self.loop.close()
            runtime = timer() - start_time

            print("Crawling %s end. Exec time: %s. Requests: %s" % (
                domain, runtime, self.count))

    async def write_to_db(self):
        while True:
            address = await self.db_Q.get()
            if await self.db_data.check_url(address) is None:
                self.db_data.add_url(address)
                print("Write to DB: %s" % address)
            self.db_Q.task_done()


if __name__ == "__main__":
    options = {
        "domains": ["https://www.yahoo.com/news/"],
        "max_depth": 1
    }
    c = Crawler(**options)
    c.start()

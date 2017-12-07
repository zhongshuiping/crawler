import asyncio
import aiohttp
from bs4 import BeautifulSoup
from pymongo import MongoClient


class Parser(object):
    db = DB = MongoClient("localhost")["local"]

    def __init__(self):
        self.urls = [i["url"] for i in self.db.crawler.find({})]
        self.max_threads = 10

    def parse_results(self, url, page):
        try:
            soup = BeautifulSoup(page, "html.parser")
            title = soup.find("title").get_text()
        except Exception as error:
            raise error

        if title:
            self.db.parsed_urls.insert_one({"url": str(url), "title": title})

    @staticmethod
    async def get_body(url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=30) as response:
                assert response.status == 200
                page = await response.read()
                return response.url, page

    async def get_results(self, url):
        url, page = await self.get_body(url)
        self.parse_results(url, page)

    async def handle_tasks(self, task_id, work_queue):
        while not work_queue.empty():
            current_url = await work_queue.get()
            try:
                await self.get_results(current_url)
            except Exception as error:
                print(repr(error))

    def process(self):
        q = asyncio.Queue()
        for url in self.urls:
            q.put_nowait(url)
        loop = asyncio.get_event_loop()
        tasks = [self.handle_tasks(task_id, q) for task_id in
                 range(self.max_threads)]
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()


if __name__ == "__main__":
    p = Parser()
    p.process()

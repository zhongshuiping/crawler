from motor import motor_asyncio


class DBData(object):
    client = motor_asyncio.AsyncIOMotorClient("localhost")
    db = client["local"]

    def __init__(self):
        self.crawler = self.db["crawler"]
        self.parsed_urls = self.db["parsed_urls"]

    def clear_crawler(self):
        return self.crawler.remove({})

    def check_url(self, url):
        return self.crawler.find_one({"url": url})

    def add_url(self, url):
        return self.crawler.insert_one({"url": url})

    def get_urls(self):
        return self.crawler.find({})

    def save_parsed_url(self, data):
        url = data["url"]
        exist = self.parsed_urls.find_one({"url": url})
        if not exist:
            return self.parsed_urls.insert_one(data)
        else:
            return self.parsed_urls.update(exist, data)

import sys
import json

from crawler import Crawler
from parser import Parser


def run_crawler_and_parse(file_path, crawling_depth):
    with open(file_path) as data_file:
        data = json.load(data_file)
        options = {"domains": data["domains"], "max_depth": int(crawling_depth)}
        c = Crawler(**options)
        c.start()
        p = Parser()
        p.process()


if __name__ == "__main__":
    try:
        run_crawler_and_parse(sys.argv[1], sys.argv[2])
    except Exception as error:
        print(repr(error))

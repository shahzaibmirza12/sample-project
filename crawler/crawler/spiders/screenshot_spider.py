import os

from scrapy.utils.project import get_project_settings
from .base import BaseSpider

settings = get_project_settings()


class ScreenshotSpiderSpider(BaseSpider):
    name = 'screenshot_spider'
    start_urls = ['https://quotes.toscrape.com/']

    def parse(self, response):
        while True:
            jobs = self.get_job_from_api()
            for input_dict in jobs:
                try:
                    item = dict()
                    print(input_dict)
                    item['screenshotUrl'], sel = self.get_screenshot_and_post_it_to_s3(input_dict.get('rootUrl'),
                                                                                  input_dict.get('rootId'))
                    self.update_job(item, input_dict.get('jobId'))
                    yield item
                except:
                    pass

            if len(jobs) == 0:
                break





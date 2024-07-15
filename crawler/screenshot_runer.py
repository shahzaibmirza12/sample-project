import schedule
import time
import os


while True:
    os.system('scrapy crawl screenshot_spider')
    time.sleep(60)
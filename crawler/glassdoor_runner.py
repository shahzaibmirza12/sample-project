import schedule
import time
import os


while True:
    os.system('scrapy crawl glassdoor')
    time.sleep(60)
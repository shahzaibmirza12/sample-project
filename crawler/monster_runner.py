import schedule
import time
import os


while True:
    os.system('scrapy crawl monster')
    time.sleep(60)
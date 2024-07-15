import schedule
import time
import os


while True:
    os.system('scrapy crawl ziprecruiter')
    time.sleep(60)
import schedule
import time
import os


while True:
    os.system('scrapy crawl indeed')
    time.sleep(60)
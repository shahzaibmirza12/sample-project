# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


# class ZiprecruiterPipeline:
#     def process_item(self, item, spider):
#         item['screenshotUrl'] = f"https://backblazeb2.s3.us-west-004.amazonaws.com/{item.get('image_path')}"
#         return item
#


# useful for handling different item types with a single interface
import json
import re
from random import choice







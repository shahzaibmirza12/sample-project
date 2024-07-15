import json
import os
import re
import time
from datetime import datetime

import chromedriver_autoinstaller
import requests
import scrapy
from scrapy import Selector
from scrapy.utils.project import get_project_settings
from b2sdk.v2 import *
from selenium import webdriver
from PIL import Image, ImageDraw, ImageFont
from selenium.webdriver.common.by import By

settings = get_project_settings()


class BaseSpider(scrapy.Spider):
    name = 'base'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.today_dir = settings.get('MAIN_CREATED_DIRECTORY')

        if os.path.isdir(self.today_dir) is False:
            os.mkdir(self.today_dir)

        info = InMemoryAccountInfo()
        self.b2_api = B2Api(info)

        self.b2_api.authorize_account("production", settings.get('APPLICATION_KEY_ID'),
                                      settings.get('APPLICATION_KEY'))

    def check_job_exist(self, job_id):
        response = requests.get(
            settings.get('BASE_JOB_API_URL').format(f"jobs?rootId={job_id}"))
        json_data = json.loads(response.text)
        if json_data.get('total') != 0:

            return json_data.get('results', [])[0].get('jobId')
        else:
            return None

    def get_search_keywords(self):
        keywords_response = requests.get(url=settings.get('KEYWORD_API'))
        locations_response = requests.get(url=settings.get('LOCATION_API'))
        keyword_data_list = json.loads(keywords_response.text)
        location_data_list = json.loads(locations_response.text)
        return keyword_data_list, location_data_list

    def post_job(self, item):
        response = requests.request("POST",
                                    settings.get('BASE_JOB_API_URL').format('ns/jobs'),
                                    headers=settings.get('JOB_HEADERS'),
                                    data=json.dumps(item))
        print(
            f'job posted with status {response.status_code} and job_id : {item["rootId"]}')

    def get_job_from_api(self):
        response = requests.get(settings.get('BASE_JOB_API_URL').format('jobs/screenshot?limit=100'))

        json_data = json.loads(response.text)
        return json_data.get('results', [])

    def get_screenshot_and_post_it_to_s3(self, detail_page_url, job_id):
        opt = webdriver.ChromeOptions()
        opt.add_argument("--start-maximized")
        opt.add_argument("--headless")
        opt.add_argument("--disable-notifications")
        opt.add_argument('--disable-popup-blocking')
        opt.add_argument("test-type")

        opt.add_argument('--disable-gpu')
        opt.add_argument('--no-sandbox')
        opt.add_argument('--disable-notifications')
        opt.add_argument('--proxy-server=%s' % "173.208.152.162:19006")

        driver = webdriver.Chrome(options=opt, executable_path=chromedriver_autoinstaller.install())
        driver.get(detail_page_url)

        time.sleep(5)
        if 'https://www.glassdoor.com/' in detail_page_url:
            try:
                show_more = driver.find_element(By.CSS_SELECTOR, "#JobDescriptionContainer>div:nth-child(2)")
                driver.execute_script("arguments[0].click();", show_more)
                time.sleep(3)
            except:
                pass
        try:
            if 'ziprecruiter.com' in driver.current_url:
                driver.execute_script(
                    "document.querySelector(`[class^='pc_message_wrapper']`).style.display = 'block';")
        except:
            pass

        S = lambda X: driver.execute_script('return document.body.parentNode.scroll' + X)
        driver.set_window_size(S('Width'),
                               S('Height'))  # May need manual adjustment
        print(f"{self.today_dir}/{job_id}.png")
        driver.find_element(By.TAG_NAME, 'body').screenshot(f"{self.today_dir}/{job_id}.png")
        sel = Selector(text=driver.page_source)

        driver.close()
        self.image_watermark(self.today_dir, f"{job_id}.png", detail_page_url)

        local_file_path = f"{self.today_dir}/{job_id}.png"
        b2_file_name = f"{self.today_dir}/{job_id}.png"
        file_info = {'how': 'good-file'}

        bucket = self.b2_api.get_bucket_by_name(settings.get('BUCKET_NAME'))
        bucket.upload_local_file(
            local_file=local_file_path,
            file_name=b2_file_name,
            file_infos=file_info,
        )

        screenshot_url = f"https://f004.backblazeb2.com/file/{settings.get('BUCKET_NAME')}/{self.today_dir}/{job_id}.png"
        os.remove(f'{self.today_dir}/{job_id}.png')
        return screenshot_url, sel

    def update_job(self, item, status):
        response = requests.request("PUT",
                                    settings.get('BASE_JOB_API_URL').format(f"ns/jobs/{status}"),
                                    headers=settings.get('JOB_HEADERS'),
                                    data=json.dumps(item))
        print(
            f'job updated with status {response.status_code} and job_id : {status}')

    def success_posting(self, meta, base):
        posting_json = {
            "keyword": meta['keyword_dict']['keywordId'],
            "location": meta['location_dict']['locationId'],
            "rootName": base,
            "postedCount": meta['keyword_dict']['kw_postedCount'],
            "failedCount": meta['keyword_dict']['kw_failedCount']
        }

        payload = json.dumps(posting_json)
        headers = {
            'Content-Type': 'application/json'
        }

        response = requests.request("POST", settings.get('KEYWORD_SUCCESS_API'),
                                    headers=headers, data=payload)
        print('/////////////////////////////////////////////////')
        print(response.text)
        print('/////////////////////////////////////////////////')

    def image_watermark(self, path, image_name, root_url):

        # Opening Image & Creating New Text Layer
        img = Image.open(f'{path}/{image_name}').convert("RGBA")
        txt = Image.new('RGBA', img.size, (255, 255, 255, 0))

        # Creating Text
        text = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ==> {root_url}'

        text_1 = text[0:125] if len(text) > 125 else ''
        text_2 = text[125:] if len(text) > 125 else ''

        font = ImageFont.load_default()

        # Creating Draw Object
        d = ImageDraw.Draw(txt)

        # Positioning Text
        width, height = img.size
        textwidth, textheight = d.textsize(text_1, font)
        x = width - width - textwidth / 4 + 200
        y = height - height + textheight + 50

        # Applying Text
        d.text((x, y), text_1, fill=(0, 0, 0, 125), font=font)

        textwidth, textheight = d.textsize(text_2, font)
        x = width - width - textwidth / 4 + 200
        y = height - height + textheight + 60

        # Applying Text
        d.text((x, y), text_2, fill=(0, 0, 0, 125), font=font)

        # Combining Original Image with Text and Saving
        watermarked = Image.alpha_composite(img, txt)
        watermarked.save(f'{path}/{image_name}')

    def close(spider, reason):
        try:
            path_to_dir = os.getcwd() + '/' + settings.get('MAIN_CREATED_DIRECTORY')
            files_in_dir = os.listdir(path_to_dir)
            for file in files_in_dir:
                os.remove(f'{path_to_dir}/{file}')

            os.rmdir(path_to_dir)
        except:
            print('directory already deleted')

    def extract_text_nodes(self, selector, dont_skip=None):
        dont_skip = dont_skip or []
        assert isinstance(dont_skip, list), "'dont_skip' must be a 'list' or None type"

        required_tags = ['p', 'i', 'u', 'strong', 'b', 'em', 'span', 'sup', 'sub', 'font', 'div',
                         'li']
        required_tags.extend(dont_skip)

        texts = selector.extract()
        if not type(texts) is list:
            texts = [texts]
        results = []
        for text in texts:
            for tag in required_tags:
                text = re.sub(r'<\s*%s>' % tag, '', text)
                text = re.sub(r'</\s*%s>' % tag, '', text)
                text = re.sub(r'<\s*%s[^\w][^>]*>' % tag, '', text)
                text = re.sub(r'</\s*%s[^\w]\s*>' % tag, '', text)

            text = text.replace('\r\n', ' ')
            text = re.sub(r'<!--.*?-->', '', text, re.S)
            sel = Selector(text=text)

            # extract all texts except tabular texts
            all_texts = sel.xpath(''.join([
                'descendant::text()/parent::*[name()!="td"]',
                '[name()!="script"][name()!="style"]/text()'
            ])).extract()
            all_texts = map(lambda x: x.strip(), all_texts)
            results += all_texts

        results = [text for text in results if text]
        return results

import json
import time
from copy import deepcopy

from scrapy import Request, Selector
from scrapy_scrapingbee import ScrapingBeeRequest
from scrapy.utils.project import get_project_settings
from .base import BaseSpider

settings = get_project_settings()


class MonsterSpider(BaseSpider):
    name = 'monster'
    start_urls = ['https://www.monster.com/jobs/search?q=Node&where=North+Carolina']

    def parse(self, response):
        json_data = json.loads(response.css('#__NEXT_DATA__::text').get())
        finger_print = json_data.get('runtimeConfig', {}).get('api', {}).get('splitClientAuthKey')
        keyword_data_list, location_data_list = self.get_search_keywords()

        for keyword_item in keyword_data_list.get('results', [])[:1]:
            for location_item in location_data_list.get('results', [])[:1]:
                location = location_item.get('name')
                keyword = keyword_item.get('name')
                payload = deepcopy(settings.get('MONSTER_PAYLOAD'))
                payload['offset'] = 0
                payload['fingerprintId'] = finger_print
                payload['jobQuery']['query'] = keyword
                payload['jobQuery']['locations'][0]['address'] = location
                keyword_item['kw_failedCount'] = 0
                keyword_item['kw_postedCount'] = 0
                time.sleep(0.1)
                yield ScrapingBeeRequest(
                    url=settings.get('MONSTER_LISTING_API'),
                    callback=self.parse_listings,
                    body=json.dumps(payload),
                    headers=settings.get('MONSTER_HEADERS'),
                    method='POST',
                    params={
                        'render_js': False,
                    },
                    dont_filter=True,
                    meta={'keyword': keyword, 'location': location, 'page_no': 1, 'offset': 0,
                          'keyword_dict': keyword_item, 'location_dict': location_item,
                          'finger_print': finger_print}
                )

    def parse_listings(self, response):
        finger_print = response.meta['finger_print']
        json_data = json.loads(response.text)
        for job_dict in json_data.get('jobResults', []):
            salary = f"{job_dict.get('jobPosting', {}).get('baseSalary', {}).get('value', {}).get('minValue', '')}$ - {job_dict.get('jobPosting', {}).get('baseSalary', {}).get('value', {}).get('maxValue', '')}$ {job_dict.get('jobPosting', {}).get('baseSalary', {}).get('value', {}).get('unitText', '')}" if job_dict.get('jobPosting', {}).get('baseSalary') else ''

            job_location = ''
            if len(job_dict.get('jobPosting', {}).get('jobLocation', {})) > 0:
                job_location = f"{job_dict.get('jobPosting', {}).get('jobLocation', {})[0].get('address', {}).get('addressLocality', '')}, {job_dict.get('jobPosting', {}).get('jobLocation', {})[0].get('address', {}).get('addressRegion', '')}"
            sel = Selector(text=job_dict.get('jobPosting', {}).get('description', ''))
            item = {
                'title': job_dict.get('jobPosting', {}).get('title', ''),
                'summary': 'N/A',
                'description': ' '.join(sel.css('*::text').getall()),
                'company': job_dict.get('jobPosting', {}).get('hiringOrganization', {}).get('name', ''),
                'location': job_location,
                'postDate': 'PostedJust posted',
                'type': ', '.join(job_dict.get('jobPosting', {}).get('employmentType', [])),
                'salary': salary,
                'rootId': job_dict.get('jobId', {}),
                'rootName': 'monster.com',
                'rootUrl': job_dict.get('jobPosting', {}).get('url', ''),
                'screenshotUrl': ''
            }
            status = self.check_job_exist(item['rootId'])

            if status:
                response.meta['keyword_dict']['kw_failedCount'] += 1
                print(f'job already exist on api job_id : {job_dict.get("jobId", {})}')
            else:
                response.meta['keyword_dict']['kw_postedCount'] += 1
                if settings.get('TAKE_SCREENSHOTS'):
                    item['screenshotUrl'] = self.get_screenshot_and_post_it_to_s3(item['rootUrl'], item['rootId'])

                self.post_job(item)
                yield item

        offset = response.meta['offset'] + 9
        keyword = response.meta['keyword']
        location = response.meta['location']
        if response.meta['keyword_dict']['kw_failedCount'] < settings.get('FAILED_COUNT_LIMIT'):
            if int(json_data.get('estimatedTotalSize')) > offset:
                payload = deepcopy(settings.get('MONSTER_PAYLOAD'))
                payload['offset'] = offset
                payload['jobQuery']['query'] = keyword
                payload['jobQuery']['locations'][0]['address'] = location
                payload['fingerprintId'] = finger_print
                yield ScrapingBeeRequest(
                    url=settings.get('MONSTER_LISTING_API'),
                    callback=self.parse_listings,
                    params={
                        'render_js': False,
                    },
                    body=json.dumps(payload),
                    headers=settings.get('MONSTER_HEADERS'),
                    method='POST',
                    meta={'keyword': keyword, 'location': location, 'page_no': 1, 'offset': offset,
                          'keyword_dict': response.meta['keyword_dict'],
                          'location_dict': response.meta['location_dict'], 'finger_print': finger_print}
                )
            else:
                self.success_posting(response.meta, 'monster.com')

        else:
            self.success_posting(response.meta, 'monster.com')


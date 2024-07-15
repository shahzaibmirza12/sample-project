import json
import time

from scrapy import Request, Selector

from .base import BaseSpider
from scrapy_scrapingbee import ScrapingBeeRequest
from scrapy.utils.project import get_project_settings

settings = get_project_settings()


class GlassdoorSpider(BaseSpider):
    name = 'glassdoor'

    def __init__(self):
        self.keyword_data_list, self.location_data_list = self.get_search_keywords()

    def start_requests(self):
        for keyword_item in self.keyword_data_list.get('results', []):
            for location_item in self.location_data_list.get('results', []):
                location = location_item.get('name')
                time.sleep(0.1)
                yield ScrapingBeeRequest(
                    url=settings.get('GLASSDOOR_LOCATION_API').format(location),
                    callback=self.parse,
                    params={
                        'render_js': False,
                    },
                    meta={'location': location,
                          'location_dict': location_item, 'keyword_item': keyword_item}
                )

    def parse(self, response):
        keyword_item = response.meta['keyword_item']
        location = response.meta['location']
        json_data = json.loads(response.text or '{}')
        location_id = None
        for location_dict in json_data:
            if location_dict.get('locationType', '') == 'S':
                location_id = location_dict.get('locationId', '')
                break
        if location_id:
            keyword = keyword_item.get('name')
            response.meta['keyword'] = keyword
            response.meta['keyword_dict'] = keyword_item
            yield ScrapingBeeRequest(
                url=settings.get('GLASSDOOR_LISTING_URL').format(keyword, location_id),
                callback=self.parse_listing_url,
                params={
                    'render_js': False,
                },
                headers=settings.get('GLASSDOOR_HEADERS'),
                meta=response.meta
            )

    def parse_listing_url(self, response):
        current_url = response.url.replace('.htm', '_IP1.htm?fromAge=1&radius=100')
        response.meta['current_page_slug'] = '_IP1.htm'
        response.meta['current_page'] = 1
        response.meta['keyword_dict']['kw_failedCount'] = 0
        response.meta['keyword_dict']['kw_postedCount'] = 0

        yield ScrapingBeeRequest(
            url=current_url,
            callback=self.parse_listing,
            params={
                'render_js': False,
            },
            headers=settings.get('GLASSDOOR_HEADERS'),
            meta=response.meta
        )

    def parse_listing(self, response):
        for job_url in response.css('.react-job-listing'):
            job_id = job_url.css('::attr(data-id)').get()
            status = self.check_job_exist(job_id)
            response.meta['job_id'] = job_id
            if status:
                response.meta['keyword_dict']['kw_failedCount'] += 1
                print(f'job already exist on api job_id : {job_id}')
            else:
                response.meta['keyword_dict']['kw_postedCount'] += 1
                yield ScrapingBeeRequest(
                    url=response.urljoin(job_url.css('.jobLink ::attr(href)').get()),
                    callback=self.parse_job_detail,
                    params={
                        'render_js': False,
                    },
                    headers=settings.get('GLASSDOOR_HEADERS'),
                    meta=response.meta
                )
        next_page_check = response.css('.nextButton[disabled]').get()
        next_page = response.css('.nextButton').get()

        if response.meta['keyword_dict']['kw_failedCount'] < settings.get('FAILED_COUNT_LIMIT'):

            if next_page is not None and next_page_check is None:
                slug = response.meta['current_page_slug']
                current_page = response.meta['current_page'] + 1
                next_page_slug = f'_IP{current_page}.htm'
                current_url = response.url.replace(slug, next_page_slug)
                response.meta['current_page_slug'] = next_page_slug
                response.meta['current_page'] = current_page
                yield ScrapingBeeRequest(
                    url=current_url,
                    callback=self.parse_listing,
                    params={
                        'render_js': False,
                    },
                    headers=settings.get('GLASSDOOR_HEADERS'),
                    meta=response.meta
                )
            else:
                self.success_posting(response.meta, 'glassdoor.com')

        else:
            self.success_posting(response.meta, 'glassdoor.com')

    def parse_job_detail(self, response):
        try:
            json_data = json.loads(response.css('script').re_first('window.appCache=(.*);'))
            job_node = json_data.get('initialState', {}).get('jlData')

            sel = Selector(text=job_node.get('job', {}).get('description', ''))
            item = {
                'title': job_node.get('header', {}).get('jobTitleText'),
                'summary': 'N/A',
                'description': ' '.join(sel.css('*::text').getall()),
                'company': job_node.get('header', {}).get('employerNameFromSearch'),
                'location': job_node.get('header', {}).get('locationName'),
                'postDate': 'PostedJust posted',
                'type': job_node.get('header', {}).get('jobTypeKeys', [])[0].split('.')[
                    -1] if job_node.get('header', {}).get('jobTypeKeys', []) else '',
                'salary': response.css('div>span.small::text').get('').strip(),
                'rootId': response.meta['job_id'],
                'rootName': 'glassdoor.com',
                'rootUrl': response.url,
                'screenshotUrl': ''
            }

        except:
            item = {
                'title': response.css('div.css-17x2pwl::text').get('').strip(),
                'summary': 'N/A',
                'description': ' '.join(response.css('div.desc *::text').getall()),
                'company': response.css('div.css-16nw49e::text').get('').strip(),
                'location': response.css('div.css-1v5elnn::text').get('').strip(),
                'postDate': 'PostedJust posted',
                'type': '',
                'salary': response.css('div>span.small::text').get('').strip(),
                'rootId': response.meta['job_id'],
                'rootName': 'glassdoor.com',
                'rootUrl': response.url,
                'screenshotUrl': ''
            }

        if settings.get('TAKE_SCREENSHOTS'):
            item['screenshotUrl'] = self.get_screenshot_and_post_it_to_s3(item['rootUrl'],
                                                                          item['rootId'])
        if item['title']:
            self.post_job(item)
            yield item

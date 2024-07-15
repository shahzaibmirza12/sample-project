import json

from scrapy import Request, Selector
from scrapy.utils.project import get_project_settings
from .base import BaseSpider

settings = get_project_settings()


class IndeedSpider(BaseSpider):
    name = 'indeed'
    zyte_key = '35c506991df94633b87320ed9107bcfe'

    custom_settings = {
        'CONCURRENT_REQUESTS': 8,
        'DOWNLOAD_DELAY': 0.3,
        'ZYTE_SMARTPROXY_ENABLED': True,
        'ZYTE_SMARTPROXY_APIKEY': zyte_key,
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_zyte_smartproxy.ZyteSmartProxyMiddleware': 610
        },
    }

    def start_requests(self):
        keyword_data_list, location_data_list = self.get_search_keywords()

        for location_item in location_data_list.get('results', []):
            for keyword_item in keyword_data_list.get('results', []):
                location = location_item.get('name')
                keyword = keyword_item.get('name')
                keyword_item['kw_failedCount'] = 0
                keyword_item['kw_postedCount'] = 0
                yield Request(
                    url=settings.get('INDEED_LISTING_URL').format(keyword, location),
                    callback=self.parse_listings,
                    headers=settings.get('INDEED_HEADERS'),
                    dont_filter=True,
                    meta={'keyword': keyword, 'location': location,
                          'keyword_dict': keyword_item,
                          'location_dict': location_item}
                )

    def parse_listings(self, response):
        job_id_list = list()
        jobs_dict = dict()
        for job_div in response.css('div.tapItem'):
            job_id = job_div.css('.jobTitle>a::attr(data-jk)').get()
            status = self.check_job_exist(job_id)
            if status:
                response.meta['keyword_dict']['kw_failedCount'] += 1
                print(f'job already exist on api job_id : {job_id}')
            else:
                response.meta['keyword_dict']['kw_postedCount'] += 1
                job_id_list.append(job_id)
                jobs_dict[job_id] = {
                    'title': job_div.css('.jobTitle>a>span::attr(title)').get('').strip(),
                    'summary': ' '.join(job_div.css('.job-snippet *::text').getall()).strip(),
                    'description': '',
                    'company': ''.join(job_div.css('.companyName *::text').getall()),
                    'location': ''.join(job_div.css('.companyLocation *::text').getall()),
                    'postDate': 'PostedJust posted',
                    'type': job_div.xpath('//*[@aria-label="Job type"]/following::text()[1]').get('').strip(),
                    'salary': job_div.xpath('//*[@aria-label="Salary"]/following::text()[1]').get('').strip(),
                    'rootId': job_id,
                    'rootName': 'indeed.com',
                    'rootUrl': response.urljoin(job_div.css('.jobTitle>a::attr(href)').get('')),
                    'screenshotUrl': ''
                }
        if job_id_list:
            yield Request(
                url=settings.get('INDEED_DESCRIPTION_API').format(','.join(job_id_list)),
                headers=settings.get('INDEED_HEADERS'),
                callback=self.parse_description,
                meta={'jobs': jobs_dict}
            )

        next_page = response.css('a[aria-label="Next Page"]::attr(href)').get()
        if next_page is None:
            next_page = response.css('a[aria-label="Next"]::attr(href)').get()

        if next_page is None:
            next_page = response.css('a[data-testid="pagination-page-next"]::attr(href)').get()

        if response.meta['keyword_dict']['kw_failedCount'] < settings.get('FAILED_COUNT_LIMIT'):
            if next_page:
                yield Request(
                    url=response.urljoin(next_page),
                    callback=self.parse_listings,
                    headers=settings.get('INDEED_HEADERS'),
                    meta=response.meta
                )
            else:
                self.success_posting(response.meta, 'indeed.com')

        else:
            self.success_posting(response.meta, 'indeed.com')

    def parse_description(self, response):
        jobs_dict = response.meta['jobs']
        json_data = json.loads(response.text)
        for job_key in list(jobs_dict.keys()):
            sel = Selector(text=json_data.get(job_key, ''))
            jobs_dict[job_key]['description'] = ' '.join(sel.css('*::text').getall())
            self.post_job(jobs_dict[job_key])
            yield jobs_dict[job_key]


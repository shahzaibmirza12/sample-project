import time

from scrapy_scrapingbee import ScrapingBeeRequest
from scrapy.utils.project import get_project_settings
from .base import BaseSpider

settings = get_project_settings()


class ZiprecruitSpider(BaseSpider):
    name = 'ziprecruiter'

    def start_requests(self):
        keyword_data_list, location_data_list = self.get_search_keywords()
        for keyword_item in keyword_data_list.get('results', []):
            for location_item in location_data_list.get('results', []):
                location = location_item.get('name')
                keyword = keyword_item.get('name')
                keyword_item['kw_failedCount'] = 0
                keyword_item['kw_postedCount'] = 0
                time.sleep(0.1)
                yield ScrapingBeeRequest(
                    url=settings.get('LISTING_API').format(location.replace(' ', '%20'), 1, keyword.replace(' ', '%20')),
                    callback=self.parse_listings,
                    dont_filter=True,
                    meta={'keyword': keyword, 'location': location, 'page_no': 1,
                          'keyword_dict': keyword_item, 'location_dict': location_item}
                )

    def parse_listings(self, response):
        keyword = response.meta['keyword']
        location = response.meta['location']
        page_no = response.meta['page_no']
        job_length_check = len(response.css('.job_result'))
        for job_div in response.css('.job_result'):
            job_id = job_div.css('::attr(data-listing-version-key)').get('')
            status = self.check_job_exist(job_id)
            item = {
                'title': job_div.css('.just_job_title::text').get(''),
                'summary': ''.join(self.extract_text_nodes(job_div.css('.job_snippet'))) if job_div.css('.job_snippet') else 'N/A',
                'description': '',
                'company': job_div.css('.t_org_link::text').get(''),
                'location': job_div.css('.t_location_link::text').get('').strip(),
                'postDate': 'PostedJust posted',
                'type': job_div.css('.perks_type>p>span::text').get(''),
                'salary': job_div.css('.perks_compensation>p>span::text').get(''),
                'rootId': job_id,
                'rootName': 'ziprecruiter.com',
                'rootUrl': job_div.css('.job_link::attr(href)').get(''),
                'screenshotUrl': '',
            }

            if status:
                response.meta['keyword_dict']['kw_failedCount'] += 1
                print(f'job already exist on api job_id : {job_id}')

            else:
                response.meta['keyword_dict']['kw_postedCount'] += 1
                if settings.get('TAKE_SCREENSHOTS'):

                    item['screenshotUrl'], sel = self.get_screenshot_and_post_it_to_s3(
                        item['rootUrl'], item['rootId'])
                    item['description'] = ' '.join(
                        sel.xpath('//div[@class="jobDescriptionSection"]//text()').getall())
                    if item['description'] == '':
                        item['description'] = ' '.join(
                            sel.xpath('//div[@class="job_description"]//text()').getall())
                    self.post_job(item)

                    yield item

                else:
                    yield ScrapingBeeRequest(
                        url=item['rootUrl'],
                        callback=self.parse_job_detail,
                        meta={'item': item, 'status': status}
                    )

        if response.meta['keyword_dict']['kw_failedCount'] < settings.get('FAILED_COUNT_LIMIT'):

            if job_length_check >= 8:
                yield ScrapingBeeRequest(
                    settings.get('LISTING_API').format(location.replace(' ', '%20'), page_no + 1, keyword.replace(' ', '%20')),
                    callback=self.parse_listings,
                    meta={'keyword': keyword, 'location': location, 'page_no': page_no + 1,
                          'keyword_dict': response.meta['keyword_dict'], 'location_dict': response.meta['location_dict']}
                )
            else:

                self.success_posting(response.meta, 'ziprecruiter.com')
        else:
            self.success_posting(response.meta, 'ziprecruiter.com')

    def parse_job_detail(self, response):
        item = response.meta['item']
        item['description'] = ' '.join(
            response.xpath('//div[@class="jobDescriptionSection"]//text()').getall())
        if item['description'] == '':
            item['description'] = ' '.join(
                response.xpath('//div[@class="job_description"]//text()').getall())

        self.post_job(item)
        yield item




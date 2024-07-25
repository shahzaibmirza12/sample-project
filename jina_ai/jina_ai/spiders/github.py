import os

import scrapy
from urllib.parse import urlparse


class GithubSpider(scrapy.Spider):
    name = "github"
    start_urls = ["https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28"]

    jina_base_url = 'https://r.jina.ai/{}'
    jina_ai_headers = {
        'Authorization': 'Bearer jina_ae7c6808521a49609d2f46037d2f120aKnlzUPk1CuByRwoO3U0FCSpiB93q',
        'X-Return-Format': 'markdown'
    }

    def create_directories_from_url(self, url, response):
        # Parse the URL to get the path
        parsed_url = urlparse(url)
        path = parsed_url.path.replace('https://', '')

        # Construct the directory path
        directory_path = path.strip("/").replace("/", os.sep)

        # Add the file extension to the last part of the path
        directory_parts = directory_path.split(os.sep)
        directory_parts[-1] += ".MD"
        final_path = os.path.join(*directory_parts)
        try:
            # Create directories
            os.makedirs(os.path.dirname(final_path), exist_ok=True)
        except:
            pass

        # Create the file
        with open(final_path, 'w') as f:
            f.write(response)

        print(f"Created directories and file: {final_path}")

    def parse(self, response):
        if response.meta.get('count'):
            meta = response.meta.get('count') + 1
        else:
            meta = 0

        # side_nav_urls = response.css('li.Ywlla>div>ul>li>a::attr(href)').getall()

        if meta <= 10:
            link_extracted = response.xpath('//body//a/@href').getall()
            for link in link_extracted:
                if 'github.com' in link:
                    yield scrapy.Request(
                        url=response.urljoin(link),
                        callback=self.parse,
                        meta={'count': meta}
                    )

        yield scrapy.Request(
            url=self.jina_base_url.format(response.url),
            headers=self.jina_ai_headers,
            callback=self.parse_jina_markdown
        )

    def parse_jina_markdown(self, response):
        self.create_directories_from_url(response.url, response.text)


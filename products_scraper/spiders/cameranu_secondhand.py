import json
from collections import defaultdict
from datetime import datetime
import uuid

from scrapy import Request

from .base import BaseSpider


class CameranuSecondHandSpider(BaseSpider):
    name = "cameranu_secondhand"
    base_url = 'https://www.cameranu.nl/'

    custom_settings = {
        'CONCURRENT_REQUESTS': 4,

        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,  # 60 sec page timeout
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",

        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },

        # ⭐ DataImpulse proxy inside Playwright
        # "PLAYWRIGHT_CONTEXTS": {
        #     "default": {
        #         "proxy": {
        #             "server": f"http://{BaseSpider.proxy_domain}:{BaseSpider.proxy_port}",
        #             "username": BaseSpider.proxy_username,
        #             "password": BaseSpider.proxy_password,
        #         }
        #     }
        # },

        "PLAYWRIGHT_MAX_PAGES_PER_CONTEXT": 4,

    }

    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'priority': 'u=0, i',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    }

    def start_requests(self):
        url = 'https://www.cameranu.nl/tweedehands-alle-producten'
        yield Request(url=url, headers=self.headers,
                      meta=self.playwright_meta,
                      errback=self.errback_handler)

    def parse(self, response, **kwargs):
        yield from self.parse_products(response)

    def parse_products(self, response):
        products = response.css('div.cat-item.cat-item-product-v3--portrait')

        for row in products:
            product_url = row.css('.cat-item-product-v3__name::attr(href)').get('')

            if not product_url:
                continue

            product_url = response.urljoin(product_url)

            if product_url in self.seen_product_urls:
                self.duplicate_skipped_counter += 1
                continue

            item = self.get_listing_item(row, product_url)

            meta = self.playwright_meta
            meta['listing_item'] = item
            yield Request(url=product_url,
                          headers=self.headers,
                          meta=meta,
                          errback=self.errback_handler,
                          callback=self.parse_details)

        # next_page_url = response.css('a[aria-label="Volgende pagina"] ::attr(href)').get('')
        # if next_page_url:
        #     yield Request(url=response.urljoin(next_page_url),
        #                   headers=self.headers,
        #                   meta=self.playwright_meta,
        #                   errback=self.errback_handler,
        #                   callback=self.parse_products
        #                   )

    def parse_details(self, response):
        listing_item = response.meta.get('listing_item') or {}

        new_product_tab = response.css('a.product-page__main-tab:contains("Nieuw")')
        new_product_url = new_product_tab.css('::attr(href)').get('')

        item = dict()
        item.update(listing_item)
        item['condition'] = response.css('.product-page__panel .description ::text').get('').strip()
        item['shutter_count'] = ' '.join(response.css('[class="product-page__panel-second-hand-state active"] .specs li:contains("clicks") ::text').getall()).strip()
        item['new_product_url'] = response.urljoin(new_product_url) if new_product_url else ''
        item['new_price'] = self.load_data_track_json(new_product_tab.css('::attr(data-track)').get('')).get('price')
        item['notes'] = response.css('.product-page__information-content-inner div:nth-child(1)::text').get('').strip()

        self.current_scrapped_items.append(item)

        yield item

    def load_data_track_json(self, data_track):
        try:
            data_track_json = json.loads(data_track)['items'][0]
        except:
            try:
                data_track_json = json.loads(data_track)[0]['items'][0]
            except:
                data_track_json = {}

        return data_track_json

    def get_listing_item(self, row, product_url):
        product_json = self.load_data_track_json(row.css('[data-track] ::attr(data-track)').get(''))

        item = dict()
        item['sku'] = product_json.get('item_id')
        item['product_title'] = product_json.get('item_name', '').split('-')[0].strip()
        item['product_url'] = product_url
        item['price'] = product_json.get('price')
        item['condition'] = ''
        item['shutter_count'] = ''
        item['brand'] = product_json.get('item_brand')
        item['category'] = product_json.get('item_category2') or product_json.get('item_category')
        item['page_id'] = product_json.get('page_id')
        item['new_product_url'] = ''
        item['new_price'] = ''
        item['notes'] = ''
        return item

    def format_scraped_data(self, status="completed", failed_pages=0, duration_seconds=0):

        scrape_run_id = str(uuid.uuid4())
        scrape_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        all_items = self.current_scrapped_items + self.out_of_stock_scrapped_items

        products_map = defaultdict(lambda: {
            "product_url": None,
            "product_title": None,
            "product_id": None,
            "variants": []
        })

        total_variants = 0

        for item in all_items:
            key = item.get("page_id")

            product = products_map[key]

            product["product_url"] = item.get("product_url")
            product["product_title"] = item.get("product_title")
            product["product_id"] = item.get("page_id")

            variant = {
                # "product_url": item.get("product_url"),
                "sku": item.get("sku"),
                "price": item.get("price"),
                "condition": item.get("condition"),
                "availability": "in_stock",
                "shutter_count": item.get("shutter_count").strip() if item.get("shutter_count") else None,
                "notes": item.get("notes")
            }

            product["variants"].append(variant)
            total_variants += 1

        products = list(products_map.values())

        result = {
            "scrape_run_id": scrape_run_id,
            "scrape_timestamp": scrape_timestamp,
            "status": status,
            "stats": {
                "total_products": len(products),
                "total_variants": total_variants,
                "failed_pages": failed_pages,
                "duration_seconds": duration_seconds
            },
            "products": products
        }

        self.summary_data = result
        self.write_data_into_json_file(result)
import ast
import json
from math import ceil

from scrapy import Request, Selector

from .base import BaseSpider


class MpbSpider(BaseSpider):
    name = "mpb_variants"
    base_url = 'https://www.mpb.com/nl-nl'

    custom_settings = {
        'CONCURRENT_REQUESTS': 1,

        "PLAYWRIGHT_BROWSER_TYPE": "chromium",
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 60000,  # 60 sec page timeout
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",

        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },

        # ⭐ DataImpulse proxy inside Playwright
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "proxy": {
                    "server": f"http://{BaseSpider.proxy_domain}:{BaseSpider.proxy_port}",
                    "username": BaseSpider.proxy_username,
                    "password": BaseSpider.proxy_password,
                }
            }
        },

        "PLAYWRIGHT_MAX_PAGES_PER_CONTEXT": 4,

    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'content-language': 'nl_NL',
        'priority': 'u=1, i',
        'referer': 'https://www.mpb.com/nl-nl/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    }

    variants_found_existing = 0
    details_called = 0

    def start_requests(self):
        # working url below
        url = 'https://www.mpb.com/search-service/product/query/?filter_query[object_type]=product&filter_query[product_condition_star_rating]=%5B1%20TO%205%5D%20AND%20NOT%200&filter_query[model_market]=EU&filter_query[model_available]=true&filter_query[model_is_published_out]=true&field_list=model_name&field_list=model_description&field_list=product_price&field_list=model_url_segment&field_list=product_sku&field_list=product_condition&field_list=product_shutter_count&field_list=product_hour_count&field_list=product_battery_charge_count&field_list=product_id&field_list=product_images&field_list=model_id&field_list=product_price_reduction&field_list=product_price_original&field_list=product_price_modifiers&field_list=model_available_new&sort[product_last_online]=DESC&facet_minimum_count=1&facet_field=model_brand&facet_field=model_category&facet_field=model_product_type&facet_field=product_condition_star_rating&facet_field=product_price&facet_field=%2A&start=0&rows=1000&minimum_match=100%25'
        yield Request(url=url, headers=self.headers, meta=self.playwright_meta, errback=self.errback_handler)

    def parse(self, response, **kwargs):
        yield from self.parse_products(response)

        # #parse pagination
        yield from self.parse_pagination(response=response, callback=self.parse_products)

        # # if it's a product scraper then we also need to scrap out-of-stock products
        if self.is_mpb_product_scraper:
            out_of_stock_url = 'https://www.mpb.com/search-service/product/query/?filter_query[object_type]=product&filter_query[model_market]=EU&filter_query[object_type]=model&filter_query[model_available]=false&filter_query[model_is_published_out]=true&filter_query[product_condition_star_rating]=%20NOT%200&field_list=model_id&field_list=model_name&field_list=model_description&field_list=model_images&field_list=product_price.minimum&field_list=product_price.maximum&field_list=product_price.count&field_list=model_url_segment&field_list=model_available_new&sort[model_name]=ASC&facet_field=model_brand&facet_field=model_product_type&facet_field=product_condition_star_rating&facet_field=product_price&facet_field=%2A&start=0&rows=1000&minimum_match=100%25'
            yield Request(url=out_of_stock_url, headers=self.headers,
                          meta=self.playwright_meta,
                          errback=self.errback_handler,
                          callback=self.parse_out_of_stock_products
                          )

    def parse_pagination(self, response, callback):
        json_data = self.get_response_json(response)

        self.total_results = json_data.get('total_results') or 0
        total_page = ceil(self.total_results / 1000)

        for page_number in range(1, total_page + 1):
            next_page_url = response.url.replace('&start=0', f'&start={page_number * 1000}')
            yield Request(url=next_page_url, headers=self.headers,
                          meta=self.playwright_meta, callback=callback, errback=self.errback_handler)

    def parse_products(self, response):
        json_data = self.get_response_json(response)

        results = json_data.get('results') or []

        for row in results:
            product_sku = self.get_first_value(row, 'product_sku')
            if not product_sku:
                continue

            product_slug = self.get_first_value(row, 'model_url_segment')
            # model_url = f'https://www.mpb.com/nl-nl/product/{product_slug}'
            model_url = f'{self.base_url}/product/{product_slug}'

            product_url = f'{model_url}/sku-{product_sku}'

            #we skipped variant only when we scrapped the variants
            if product_url in self.seen_product_urls and not self.is_mpb_product_scraper:
                self.duplicate_skipped_counter += 1
                continue

            item = dict()
            item['product_id'] = self.get_first_value(row, 'model_id')
            item['product_title'] = self.get_first_value(row, 'model_name')
            item['sku'] = product_sku
            item['price'] = self.get_product_price(row)
            item['condition'] = self.get_first_value(row, 'product_condition')
            item['availability'] = 'in_stock'
            item['shutter_count'] = self.get_first_value(row, 'product_shutter_count')
            item['notes'] = ''
            item['accessories'] = self.get_whats_include_value(row)
            item['url'] = product_url

            self.seen_product_urls.append(product_url)

            #if it's a mpb_products scraper calls then we need only to get the detail here only
            if self.is_mpb_product_scraper:
                # we are getting specification only for product scraper
                item['specifications'] = self.get_product_specifications(model_url)
                self.current_scrapped_items.append(item)
                yield item
                continue

            # check if product notes already scrapped in file then we don't need to do detail page request
            # we are requesting detail page only for notes
            if product_sku in self.seen_product_notes_skus:
                self.variants_found_existing += 1
                print(f"\n\nVariant's notes found in CSV: {self.variants_found_existing}\n\n")
                item['notes'] = self.seen_product_notes_items.get(product_sku)
                self.current_scrapped_items.append(item)
                yield item
                continue

            yield from self.parse_details(product_url=product_url, listing_item=item)

    def parse_details(self, product_url, listing_item):
        self.details_called += 1
        print(f'\n\nNew variant found: {self.details_called}\n\n')

        product_response = self.fetch_product_url_response(product_url)

        # if product request failed then we write listing page data
        if not product_response:
            self.current_scrapped_items.append(listing_item)
            yield listing_item
            return

        response = Selector(text=product_response.text)

        try:
            json_data = json.loads(response.css('#__NEXT_DATA__ ::text').get(''))['props']['pageProps']
        except:
            json_data = {}

        model_info = json_data.get('modelInfo', {}) or {}
        product_info = json_data.get('productInfo') or {}

        item = dict()
        item['product_title'] = product_info.get('name') or response.css('.product-name ::text').get('') or model_info.get('brand', {}).get('name')
        item['sku'] = product_info.get('sku')
        item['price'] = product_info.get('listPrice')
        item['condition'] = product_info.get('condition')
        item['availability'] = 'in_stock' if not product_info.get('isSold') else 'out_of_stock'
        item['shutter_count'] = ''.join([attr.get('content') for attr in product_info.get('attributes', []) or [] if attr.get('name', '').lower() == 'SHUTTER_COUNT'.lower()][:1]).strip() or response.css('[data-testid="product-details__shutter-count-attribute__title"] strong ::text').get('')
        item['notes'] = ', '.join([r.get('tierDescription') for r in product_info.get('observations', []) or []])
        item['url'] = product_url

        # to make sure we get the product details
        if not item['product_title']:
            self.current_scrapped_items.append(listing_item)
            yield listing_item
            return

        self.current_scrapped_items.append(item)

        # write notes into csv file to reduce request in future
        self.write_item_into_csv_file(filename=self.mpb_notes_filename, item={'sku': item['sku'], 'notes': item['notes']})

        yield item

    def get_first_value(self, row, key, default=None):
        # Safely get the dict, then the list, then the first item
        values = row.get(key, {}).get('values', [])
        return values[0] if values else default

    def get_product_price(self, row):
        try:
            return float(self.get_first_value(row, 'product_price')) / 100
        except:
            return None

    def get_whats_include_value(self, row):
        warranty = 'Standard 12 month warranty'

        try:
            whats_includes = row.get('product_price_modifiers', {}).get('values', [])

            # 12-month warranty is by default in all variants
            if whats_includes and isinstance(whats_includes, list):
                whats_includes = [warranty] + whats_includes

            return whats_includes or [warranty]
        except:
            return [] or [warranty]

    def get_response_json(self, response):
        try:
            json_data = json.loads(response.css('pre ::text').get(''))
        except:
            json_data = {}

        return json_data

    def parse_out_of_stock_products(self, response):
        yield from self.parse_out_of_stock_product_details(response)

        yield from self.parse_pagination(response=response, callback=self.parse_out_of_stock_product_details)

    def parse_out_of_stock_product_details(self, response):
        pass

    def get_product_specifications(self, model_url):
        headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'max-age=0',
            'priority': 'u=0, i',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
        }

        model_specifications = None

        if model_url in self.seen_product_specifications_url:
            print(f"\n\nProduct Specification found in CSV\n\n")
            scrapped_specification = self.seen_product_specifications_items.get(model_url)
            try:
                model_specifications = json.loads(scrapped_specification)
            except:
                try:
                    model_specifications = ast.literal_eval(scrapped_specification)
                except:
                    pass

            return model_specifications

        product_response = self.fetch_product_url_response(url=model_url, headers=headers)

        # if product request failed then we write listing page data
        if not product_response:
            return model_specifications

        response = Selector(text=product_response.text)

        try:
            json_data = json.loads(response.css('#__NEXT_DATA__ ::text').get(''))['props']['pageProps']
            model_specifications = {spec.get('name'):spec.get('displayValue') for spec in json_data.get('modelInfo', {}).get('modelSpecs', [])}
        except Exception as e:
            pass

        # write notes into csv file to reduce request in future
        self.write_item_into_csv_file(filename=self.mpb_specifications_filename, item={'model_url': model_url, 'specifications': model_specifications})

        #store on running time to avoid duplicate request
        self.seen_product_specifications_items.update({model_url: json.dumps(model_specifications)})
        self.seen_product_specifications_url.add(model_url)

        return model_specifications


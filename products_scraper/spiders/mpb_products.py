from collections import defaultdict
from datetime import datetime
import uuid

from .mpb_variants import MpbSpider


class MpbProductsSpider(MpbSpider):
    name = "mpb_products"
    base_url = 'https://www.mpb.com/en-eu'

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9',
        'content-language': 'en_EU',
        'priority': 'u=1, i',
        'referer': 'https://www.mpb.com/en-eu/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.is_mpb_product_scraper = True #define to get the data from listing page only

    def parse_out_of_stock_product_details(self, response):
        json_data = self.get_response_json(response)

        results = json_data.get('results') or []

        for row in results:
            model_slug = self.get_first_value(row, 'model_url_segment')
            model_url = f'{self.base_url}/product/{model_slug}' if model_slug else ''

            item = dict()
            item['product_id'] = self.get_first_value(row, 'model_id')
            item['product_title'] = self.get_first_value(row, 'model_name')
            item['availability'] = 'out_of_stock'
            item['url'] = model_url
            item['specifications'] = self.get_product_specifications(model_url) if model_url else None

            self.out_of_stock_scrapped_items.append(item)

            yield item

    def format_scraped_data(self, status="completed", failed_pages=0, duration_seconds=0):
        scrape_run_id = str(uuid.uuid4())
        scrape_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        products_map = defaultdict(lambda: {
            "variants_count": 0,
            "accessories": set(),
            "product_url": None,
            "availability": None
        })

        # 🔹 Combine both lists
        all_items = self.current_scrapped_items + self.out_of_stock_scrapped_items
        product_specifications = {item.get('url').split('/sku')[0]:item.get('specifications') for item in all_items}

        for item in all_items:
            base_url = item.get("url", "").split("/sku-")[0]
            product_title = item.get("product_title")
            availability = item.get("availability")
            product_id = item.get('product_id')

            key = (base_url, product_title, product_id)

            # Count only in-stock items
            if availability == "in_stock":
                products_map[key]["variants_count"] += 1

            # Store accessories only if available
            products_map[key]["accessories"].update(
                item.get("accessories", [])
            )

            # Keep first URL
            if not products_map[key]["product_url"]:
                products_map[key]["product_url"] = base_url

            # Store availability (out_of_stock if no in_stock found)
            if products_map[key]["availability"] != "in_stock":
                products_map[key]["availability"] = availability

        products = []
        total_in_stock_products = 0
        total_out_of_stock_products = 0

        for (product_url, product_title, product_id), data in products_map.items():
            specification = product_specifications.get(product_url)

            # Count by availability
            if data["availability"] == "in_stock":
                total_in_stock_products += 1
            else:
                total_out_of_stock_products += 1

            products.append({
                "product_url": product_url,
                "product_title": product_title,
                "product_id": product_id,
                "availability": data["availability"],
                "variants_count": data["variants_count"],
                "accessories": sorted(list(data["accessories"])),
                "specifications": specification if specification else None
            })

        result = {
            "scrape_run_id": scrape_run_id,
            "scrape_timestamp": scrape_timestamp,
            "status": status,
            "stats": {
                "total_products_scrapped": len(products),
                "total_in_stock_products": total_in_stock_products,
                "total_out_of_stock_products": total_out_of_stock_products,
                "failed_pages": failed_pages,
                "duration_seconds": duration_seconds
            },
            "products": products
        }

        self.summary_data = result
        self.write_data_into_json_file(result)

    def get_email_body_and_subject(self):
        total_products = self.summary_data["stats"]["total_products_scrapped"]
        total_in_stock_products = self.summary_data["stats"]["total_in_stock_products"]
        total_out_of_stock_products = self.summary_data["stats"]["total_out_of_stock_products"]

        failed_pages = self.summary_data["stats"]["failed_pages"]
        duration_seconds = self.summary_data["stats"]["duration_seconds"]
        scrape_timestamp = self.summary_data["scrape_timestamp"]
        scrape_run_id = self.summary_data["scrape_run_id"]
        status = self.summary_data.get("status", "completed").title()

        # Email subject with source
        subject = f"{self.name.title()} Scrape Summary: {total_products} Products"

        # HTML content with source mention
        content = f"""
        <html>
            <body style="font-family: Arial, sans-serif; color: #333;">
                <div style="max-width: 600px; margin: auto; border: 1px solid #ddd; padding: 20px; border-radius: 8px; background-color: #f9f9f9;">
                    <h2 style="text-align: center; color: #2a7ae2;">📋 Scraping Summary Report</h2>
                    <p><strong>Scrape Run ID:</strong> <code>{scrape_run_id}</code></p>
                    <p><strong>Timestamp:</strong> {scrape_timestamp}</p>
                    <p><strong>Status:</strong> 
                        <span style="
                            color: {'green' if status.lower() == 'completed' else 'red'};
                            font-weight: bold;
                            padding: 3px 8px;
                            border-radius: 5px;
                            background-color: {'#d4edda' if status.lower() == 'completed' else '#f8d7da'};
                        ">
                            {status}
                        </span>
                    </p>

                    <h3 style="color: #2a7ae2;">Statistics</h3>
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Total Products Scrapped</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{total_products}</td>
                        </tr>

                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Total InStock Products</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{total_in_stock_products}</td>
                        </tr>

                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Total Out of Stock Products</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{total_out_of_stock_products}</td>
                        </tr>

                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Failed Pages</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd; color: {'red' if failed_pages > 0 else 'green'};">
                                {failed_pages}
                            </td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Duration (seconds)</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{duration_seconds}</td>
                        </tr>
                        <tr>
                            <td style="padding: 8px; border: 1px solid #ddd;"><strong>Output Filename</strong></td>
                            <td style="padding: 8px; border: 1px solid #ddd;">{self.output_filename}</td>
                        </tr>

                    </table>
                </div>
            </body>
        </html>
        """

        return subject, content

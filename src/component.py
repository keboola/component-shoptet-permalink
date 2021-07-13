import csv
import logging
import tempfile
import requests
from keboola.utils.header_normalizer import get_normalizer, NormalizerStrategy
from requests.exceptions import RequestException, ConnectionError
from keboola.component.base import ComponentBase, UserException
from retry import retry

KEY_ORDERS_URL = 'orders_url'
KEY_PRODUCTS_URL = 'products_url'
KEY_CUSTOMERS_URL = 'customers_url'
KEY_STOCK_URL = 'stock_url'
KEY_ADDITIONAL_DATA = 'additional_data'
KEY_SRC_CHARSET = "src_charset"
KEY_DELIMITER = "delimiter"
KEY_SHOP_BASE_URL = "base_url"
KEY_SHOP_NAME = "shop_name"

REQUIRED_PARAMETERS = [KEY_SRC_CHARSET, KEY_DELIMITER, KEY_SHOP_BASE_URL, KEY_SHOP_NAME]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self):
        params = self.configuration.parameters
        charset = params.get(KEY_SRC_CHARSET)
        delimiter = params.get(KEY_DELIMITER)

        orders_url = params.get(KEY_ORDERS_URL)
        if orders_url:
            logging.info("Downloading orders...")
            self.get_url_data_and_write_to_file(orders_url, "orders.csv", charset, delimiter)

        products_url = params.get(KEY_PRODUCTS_URL)
        if products_url:
            logging.info("Downloading products...")
            self.get_url_data_and_write_to_file(products_url, "products.csv", charset, delimiter)

        customers_url = params.get(KEY_CUSTOMERS_URL)
        if customers_url:
            logging.info("Downloading customers...")
            self.get_url_data_and_write_to_file(customers_url, "customers.csv", charset, delimiter)

        stock_url = params.get(KEY_STOCK_URL)
        if stock_url:
            logging.info("Downloading stocks...")
            self.get_url_data_and_write_to_file(stock_url, "stocks.csv", charset, delimiter)

        additional_data = params.get(KEY_ADDITIONAL_DATA)
        for additional_datum in additional_data:
            logging.info(f"Downloading {additional_datum['name']}...")
            file_name = "".join([additional_datum["name"], ".csv"])
            self.get_url_data_and_write_to_file(additional_datum["url"], file_name, charset, delimiter)

        base_url = params.get(KEY_SHOP_BASE_URL)
        shop_name = params.get(KEY_SHOP_NAME)
        self.write_shoptet_table(base_url, shop_name)

    def get_url_data_and_write_to_file(self, url, table_name, encoding, delimiter):

        temp_file = self.fetch_data_from_url(url, encoding)
        logging.info(f"Downloaded {table_name}, saving to tables")
        table = self.create_out_table_definition(name=table_name)
        fieldnames = self.write_from_temp_to_table(temp_file.name, table.full_path, delimiter)
        header_normalizer = get_normalizer(NormalizerStrategy.DEFAULT)
        table.columns = header_normalizer.normalize_header(fieldnames)
        self.write_tabledef_manifest(table)

    @staticmethod
    def write_from_temp_to_table(temp_file_path, table_path, delimiter):
        with open(temp_file_path, mode='r', encoding='utf-8') as in_file:
            reader = csv.DictReader(in_file, delimiter=delimiter)
            fieldnames = reader.fieldnames
            with open(table_path, mode='wt', encoding='utf-8', newline='') as out_file:
                writer = csv.DictWriter(out_file, reader.fieldnames)
                for row in reader:
                    writer.writerow(row)
        return fieldnames

    def fetch_data_from_url(self, url, encoding):
        try:
            res = self._request_url(url)
            res.raise_for_status()
        except RequestException as invalid:
            raise UserException(invalid) from invalid
        temp = tempfile.NamedTemporaryFile(mode='w+b', suffix='.csv', delete=False)
        with open(temp.name, 'w', encoding='utf-8') as out:
            for chunk in res.iter_content(chunk_size=8192):
                chunk = chunk.decode(encoding)
                out.write(chunk)
        return temp

    @retry(ConnectionError, tries=3, delay=1)
    def _request_url(self, url):
        return requests.get(url, allow_redirects=True)

    def write_shoptet_table(self, base_url, shop_name):
        shoptet_file_name = "shoptet.csv"
        table = self.create_out_table_definition(name=shoptet_file_name, columns=["shop_base_url", "shop_name"])
        with open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, table.columns)
            writer.writerow({"shop_base_url": base_url, "shop_name": shop_name})
        self.write_tabledef_manifest(table)


if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)

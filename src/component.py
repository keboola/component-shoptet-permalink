import csv
import logging
import tempfile
import requests

from keboola.component.base import ComponentBase, UserException

KEY_ORDERS_URL = 'orders_url'
KEY_PRODUCTS_URL = 'products_url'
KEY_CUSTOMERS_URL = 'customers_url'
KEY_STOCK_URL = 'stock_url'
KEY_ADDITIONAL_DATA = 'additional_data'
KEY_SRC_CHARSET = "src_charset"
KEY_DELIMITER = "delimiter"

REQUIRED_PARAMETERS = [KEY_SRC_CHARSET, KEY_DELIMITER]
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
            self.get_url_data_and_write_to_file(additional_datum["url"], file_name, charset)

    def get_url_data_and_write_to_file(self, url, table_name, encoding, delimiter):

        temp_file = self.fetch_data_from_url(url, encoding)
        table = self.create_out_table_definition(name=table_name)
        fieldnames = self.write_from_temp_to_table(temp_file.name, table.full_path, delimiter)
        table.columns = fieldnames
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

    @staticmethod
    def fetch_data_from_url(url, encoding):
        res = requests.get(url, allow_redirects=True)
        res.raise_for_status()
        temp = tempfile.NamedTemporaryFile(
            mode='w+b', suffix='.csv', delete=False
        )
        with open(temp.name, 'w', encoding='utf-8') as out:
            for chunk in res.iter_content(chunk_size=8192):
                chunk = chunk.decode(encoding)
                out.write(chunk)
        return temp


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

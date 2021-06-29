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
KEY_ENCODING = "encoding"

REQUIRED_PARAMETERS = []
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

    def run(self):
        params = self.configuration.parameters
        encoding = params.get(KEY_ENCODING)

        orders_url = params.get(KEY_ORDERS_URL)
        if orders_url:
            logging.info("Downloading orders...")
            self.get_url_data_and_write_to_file(orders_url, "orders.csv", encoding)

        products_url = params.get(KEY_PRODUCTS_URL)
        if products_url:
            logging.info("Downloading products...")
            self.get_url_data_and_write_to_file(products_url, "products.csv", encoding)

        customers_url = params.get(KEY_CUSTOMERS_URL)
        if customers_url:
            logging.info("Downloading customers...")
            self.get_url_data_and_write_to_file(customers_url, "customers.csv", encoding)

        stock_url = params.get(KEY_STOCK_URL)
        if stock_url:
            logging.info("Downloading stocks...")
            self.get_url_data_and_write_to_file(stock_url, "stocks.csv", encoding)

        # additional_data = params.get(KEY_ADDITIONAL_DATA)

    def get_url_data_and_write_to_file(self, url, table_name, encoding):

        temp_file = self.fetch_data_from_url(url, encoding)
        table = self.create_out_table_definition(name=table_name)
        fieldnames = self.write_from_temp_to_table(temp_file.name, table.full_path)
        table.columns = fieldnames
        self.write_tabledef_manifest(table)

    @staticmethod
    def write_from_temp_to_table(temp_file_path, table_path):
        with open(temp_file_path, mode='r', encoding='utf-8') as in_file:
            reader = csv.DictReader(in_file, delimiter=";")
            fieldnames = reader.fieldnames
            with open(table_path, mode='wt', encoding='utf-8', newline='') as out_file:
                writer = csv.DictWriter(out_file, reader.fieldnames)
                for row in reader:
                    writer.writerow(row)
        return fieldnames

    def fetch_data_from_url(self, url, encoding):
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

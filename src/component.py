import csv
import logging
import os
import shutil
import tempfile
import uuid
import warnings
from pathlib import Path
from typing import List

import requests
from furl import furl
from keboola import utils as keboola_utils
from keboola.component.base import ComponentBase, UserException
from keboola.utils.header_normalizer import get_normalizer, NormalizerStrategy
from requests.exceptions import RequestException, ConnectionError
from retry import retry

DATE_FORMAT = '%Y-%m-%d'

KEY_ORDERS_URL = 'orders_url'
KEY_PRODUCTS_URL = 'products_url'
KEY_CUSTOMERS_URL = 'customers_url'
KEY_STOCK_URL = 'stock_url'
KEY_ADDITIONAL_DATA = 'additional_data'
KEY_SRC_CHARSET = "src_charset"
KEY_DELIMITER = "delimiter"
KEY_SHOP_BASE_URL = "base_url"
KEY_SHOP_NAME = "shop_name"
KEY_LOADING_OPTIONS = "loading_options"
KEY_DATE_SINCE = "date_since"
KEY_DATE_TO = "date_to"
KEY_INCREMENTAL = "incremental_output"

REQUIRED_PARAMETERS = [KEY_SRC_CHARSET, KEY_DELIMITER, KEY_SHOP_BASE_URL, KEY_SHOP_NAME]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS,
                         required_image_parameters=REQUIRED_IMAGE_PARS)

        # temp suppress pytz warning
        warnings.filterwarnings(
            "ignore",
            message="The localize method is no longer necessary, as this time zone supports the fold attribute",
        )

    def run(self):
        params = self.configuration.parameters

        loading_options = params.get(KEY_LOADING_OPTIONS, {})
        dt_since_str = loading_options.get(KEY_DATE_SINCE, '2009-01-01')
        if not dt_since_str:
            dt_since_str = '2009-01-01'
        dt_to_str = loading_options.get(KEY_DATE_TO, 'now')
        if not dt_to_str:
            dt_to_str = 'now'

        start_date, end_date = keboola_utils.parse_datetime_interval(dt_since_str, dt_to_str)
        backfill_mode = loading_options.get('backfill_mode')

        if backfill_mode:
            chunk_size = loading_options.get("chunk_size_days")

            date_chunks = keboola_utils.split_dates_to_chunks(start_date, end_date, chunk_size, strformat=DATE_FORMAT)

        else:
            date_chunks = [{"start_date": start_date.strftime(DATE_FORMAT),
                            "end_date": end_date.strftime(DATE_FORMAT)}]

        # download data
        for chunk in date_chunks:
            self._download_all_tables(chunk['start_date'], chunk['end_date'])

        base_url = params.get(KEY_SHOP_BASE_URL)
        shop_name = params.get(KEY_SHOP_NAME)
        self.write_shoptet_table(base_url, shop_name)

    def _download_all_tables(self, start_date: str, end_date: str):
        params = self.configuration.parameters
        charset = params.get(KEY_SRC_CHARSET)
        delimiter = params.get(KEY_DELIMITER)
        loading_options = params.get(KEY_LOADING_OPTIONS, {})
        incremental = loading_options.get(KEY_INCREMENTAL)

        orders_url = params.get(KEY_ORDERS_URL)
        if orders_url:
            logging.info(f"Downloading orders in period {start_date} - {end_date}...")
            orders_url = self._add_date_url_parameters(orders_url, start_date, end_date)
            self.get_url_data_and_write_to_file(orders_url, "orders.csv", charset, delimiter,
                                                primary_key=["code", "itemCode", "itemName"],
                                                alt_primary_key=["code", "orderItemCode", "orderItemName"],
                                                incremental=incremental)

        products_url = params.get(KEY_PRODUCTS_URL)
        if products_url:
            logging.info(f"Downloading products {start_date} - {end_date}....")

            products_url = self._add_date_url_parameters(products_url, start_date, end_date)
            self.get_url_data_and_write_to_file(products_url, "products.csv", charset, delimiter,
                                                primary_key=["code"],
                                                incremental=incremental)

        customers_url = params.get(KEY_CUSTOMERS_URL)
        if customers_url:
            logging.info(f"Downloading customers {start_date} - {end_date}....")
            customers_url = self._add_date_url_parameters(customers_url, start_date, end_date)
            self.get_url_data_and_write_to_file(customers_url, "customers.csv", charset, delimiter,
                                                primary_key=["accountGuid"],
                                                incremental=incremental)

        stock_url = params.get(KEY_STOCK_URL)
        if stock_url:
            logging.info(f"Downloading stocks {start_date} - {end_date}....")
            stock_url = self._add_date_url_parameters(stock_url, start_date, end_date)
            self.get_url_data_and_write_to_file(stock_url, "stocks.csv", charset, delimiter,
                                                primary_key=["itemCode"],
                                                incremental=incremental)

        additional_data = params.get(KEY_ADDITIONAL_DATA, [])
        for additional_datum in additional_data:
            logging.info(f"Downloading {additional_datum['name']} {start_date} - {end_date}....")
            file_name = "".join([additional_datum["name"], ".csv"])
            primary_key = additional_datum.get('primary_key', [])
            add_url = self._add_date_url_parameters(additional_datum["url"], start_date, end_date)
            self.get_url_data_and_write_to_file(add_url, file_name, charset, delimiter,
                                                primary_key=primary_key,
                                                incremental=incremental)

    @staticmethod
    def _add_date_url_parameters(url: str, start_date: str, end_date: str):
        url_parsed = furl(url)
        query_params = url_parsed.query.params
        query_params["dateFrom"] = start_date
        query_params["dateUntil"] = end_date
        url_parsed.set(query_params)
        return url_parsed.url

    def get_url_data_and_write_to_file(self, url, table_name, encoding, delimiter,
                                       primary_key: List[str], alt_primary_key: List[str] = None,
                                       incremental: bool = False):

        try:
            temp_file = self.fetch_data_from_url(url)
        except UnicodeDecodeError:
            raise UserException(f"Failed to decode file with {encoding}, use a different encoding")
        logging.debug(f"Downloaded {table_name}, saving to tables")
        table = self.create_out_table_definition(name=table_name, primary_key=primary_key, incremental=incremental)
        table.delimiter = delimiter

        # sliced table for backfill mode
        Path(table.full_path).mkdir(parents=True, exist_ok=True)
        result_path = os.path.join(table.full_path, str(uuid.uuid4()))

        fieldnames = self.write_from_temp_to_table(temp_file.name, result_path, delimiter, encoding)
        fieldnames = self.strip_quotes(fieldnames)
        if not self.valid_primary_keys(primary_key, fieldnames):
            if self.valid_primary_keys(alt_primary_key, fieldnames):
                table.primary_key = alt_primary_key
            else:
                raise UserException(f"Error adding primary keys to file {table_name}, please contact support. "
                                    f"primary keys {primary_key} not in {fieldnames}")

        header_normalizer = get_normalizer(NormalizerStrategy.DEFAULT)
        table.columns = header_normalizer.normalize_header(fieldnames)
        self.write_tabledef_manifest(table)

    def strip_quotes(self, list_of_str):
        new_list = []
        for val in list_of_str:
            new_list.append(val.replace("\"", ""))
        return new_list

    @staticmethod
    def valid_primary_keys(primary_key, fieldnames):
        if primary_key is None:
            return False
        for p_key in primary_key:
            if p_key not in fieldnames:
                return False
        return True

    @staticmethod
    def write_from_temp_to_table(temp_file_path, table_path, delimiter, encoding) -> List[str]:
        with open(temp_file_path, mode='r', encoding=encoding) as in_file, \
                open(table_path, mode='wt', encoding='utf-8', newline='') as out_file:
            fieldnames = in_file.readline()
            shutil.copyfileobj(in_file, out_file)
            # workaround for:
            # https://stackoverflow.com/questions/40310042/python-read-csv-bom-embedded-into-the-first-key
            if fieldnames.startswith("\ufeff"):
                fieldnames = fieldnames.replace("\ufeff", "")
            return fieldnames.split(delimiter)

    @retry(ConnectionError, tries=3, delay=1)
    def fetch_data_from_url(self, url):
        try:
            res = requests.get(url, stream=True, allow_redirects=True)
            res.raise_for_status()
        except RequestException as invalid:
            raise UserException(invalid) from invalid
        temp = tempfile.NamedTemporaryFile(mode='w+b', suffix='.csv', delete=False)
        with open(temp.name, 'wb+') as out:
            for chunk in res.iter_content(chunk_size=8192):
                out.write(chunk)
        return temp

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

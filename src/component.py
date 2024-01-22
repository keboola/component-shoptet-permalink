import csv
import logging
import tempfile
import warnings
from typing import List
from dataclasses import dataclass

import requests
from furl import furl
from keboola.utils import date
from keboola.utils.header_normalizer import get_normalizer, NormalizerStrategy
from keboola.component.dao import TableDefinition
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from requests.exceptions import RequestException, ConnectionError
from retry import retry
from keboola.csvwriter import ElasticDictWriter

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
RESOURCE_URLS = [KEY_ORDERS_URL, KEY_PRODUCTS_URL, KEY_CUSTOMERS_URL, KEY_STOCK_URL]


@dataclass
class WriterCacheRecord:
    writer: ElasticDictWriter
    table_definition: TableDefinition


class Component(ComponentBase):
    def __init__(self):
        super().__init__(required_parameters=REQUIRED_PARAMETERS)

        # temp suppress pytz warning
        warnings.filterwarnings(
            "ignore",
            message="The localize method is no longer necessary, as this time zone supports the fold attribute",
        )

        self._header_normalizer = get_normalizer(NormalizerStrategy.DEFAULT)

        self._writer_cache: dict[str, WriterCacheRecord] = dict()
        self._last_table_columns = (self.get_state_file().get('table_columns', {})
                                    or self.get_state_file().get('component', {}))  # legacy compatibility

    def run(self):
        params = self.configuration.parameters
        self._check_urls(params)

        loading_options = params.get(KEY_LOADING_OPTIONS, {})
        dt_since_str = loading_options.get(KEY_DATE_SINCE, '2009-01-01')
        if not dt_since_str:
            dt_since_str = '2009-01-01'
        dt_to_str = loading_options.get(KEY_DATE_TO, 'now')
        if not dt_to_str:
            dt_to_str = 'now'

        start_date, end_date = date.parse_datetime_interval(dt_since_str, dt_to_str)
        backfill_mode = loading_options.get('backfill_mode')

        if backfill_mode:
            chunk_size = loading_options.get("chunk_size_days")

            date_chunks = date.split_dates_to_chunks(start_date, end_date, chunk_size, strformat=DATE_FORMAT)

        else:
            date_chunks = [{"start_date": start_date.strftime(DATE_FORMAT),
                            "end_date": end_date.strftime(DATE_FORMAT)}]

        # download data
        for chunk in date_chunks:
            self._download_all_tables(chunk['start_date'], chunk['end_date'])

        base_url = params.get(KEY_SHOP_BASE_URL)
        shop_name = params.get(KEY_SHOP_NAME)
        self.write_shoptet_table(base_url, shop_name)

        table_columns = dict()

        for table, cache_record in self._writer_cache.items():
            cache_record.writer.writeheader()
            cache_record.writer.close()
            table_definition = self.create_out_table_definition(name=cache_record.table_definition.name,
                                                                primary_key=cache_record.table_definition.primary_key,
                                                                incremental=cache_record.table_definition.incremental,
                                                                columns=cache_record.writer.fieldnames)

            self.write_manifest(table_definition)
            table_columns[table] = cache_record.writer.fieldnames

        self.write_state_file({"table_columns": table_columns})

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
                                                incremental=incremental,
                                                columns=["code", "date", "itemCode", "itemName"]  # to have id and date as the first columns  # noqa: E501
                                                )

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

    def get_url_data_and_write_to_file(self, url,
                                       table_name: str,
                                       encoding: str,
                                       delimiter: str,
                                       primary_key: List[str], alt_primary_key: List[str] = None,
                                       incremental: bool = False,
                                       columns: List[str] = []
                                       ):

        try:
            temp_file = self.fetch_data_from_url(url)
        except UnicodeDecodeError:
            raise UserException(f"Failed to decode file with {encoding}, use a different encoding")
        logging.debug(f"Downloaded {table_name}, saving to tables")
        table = self.create_out_table_definition(name=table_name, primary_key=primary_key, incremental=incremental)
        table.delimiter = delimiter

        fieldnames = self.write_from_temp_to_table(temp_file.name, table_name, primary_key, delimiter, encoding,
                                                   columns)

        if not self.valid_primary_keys(primary_key, fieldnames):
            if self.valid_primary_keys(alt_primary_key, fieldnames):
                table.primary_key = alt_primary_key
            else:
                raise UserException(f"Error adding primary keys to file {table_name}, please contact support. "
                                    f"primary keys {primary_key} not in {fieldnames}")

    @staticmethod
    def valid_primary_keys(primary_key, fieldnames):
        if primary_key is None:
            return False
        for p_key in primary_key:
            if p_key not in fieldnames:
                return False
        return True

    def write_from_temp_to_table(self, temp_file_path: str,
                                 table_path: str,
                                 primary_key: List[str],
                                 delimiter: str,
                                 encoding: str,
                                 columns: List[str]):

        with open(temp_file_path, mode='r', encoding=encoding) as in_file:
            reader = csv.DictReader(in_file, delimiter=delimiter)

            self.write_to_csv(reader, table_path, incremental_load=False, primary_keys=primary_key, columns=columns)

            fieldnames = list(reader.fieldnames)

            # handling weirdly named columns
            fieldnames = self._header_normalizer.normalize_header(fieldnames)
            # fieldnames = [n.lstrip("ď»ż").replace('\ufeff\"code\"', 'code') for n in fieldnames]

            return fieldnames

    @retry(ConnectionError, tries=3, delay=1)
    def fetch_data_from_url(self, url):
        try:
            res = requests.get(url, stream=True, allow_redirects=True)
            res.raise_for_status()
        except RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 404:
                    raise UserException(f"Resource {url} cannot be found. "
                                        f"Please check if the url for this resource is valid.")
            raise UserException(e) from e
        temp = tempfile.NamedTemporaryFile(mode='w+b', suffix='.csv', delete=False)
        with open(temp.name, 'wb+') as out:
            for chunk in res.iter_content(chunk_size=8192):
                out.write(chunk)
        return temp

    def write_to_csv(self, input_data: csv.DictReader,
                     table_name: str,
                     incremental_load: bool,
                     primary_keys: List[str],
                     columns: List[str],
                     ) -> None:

        if not self._writer_cache.get(table_name):
            table_def = self.create_out_table_definition(name=table_name,
                                                         primary_key=primary_keys,
                                                         incremental=incremental_load,
                                                         # columns=columns
                                                         )

            columns = self._last_table_columns.get(table_name, []) or columns
            writer = ElasticDictWriter(table_def.full_path, columns)
            self._writer_cache[table_name] = WriterCacheRecord(writer, table_def)

        writer = self._writer_cache[table_name].writer

        for record in input_data:

            # handling weirdly named columns

            normalized_names = self._header_normalizer.normalize_header(list(record.keys()))
            normalized_dict = dict(zip(normalized_names, record.values()))

            # if record.get(''):
            #     record['empty'] = record.pop('')
            # if record.get('\ufeff\"code\"'):
            #     record['code'] = record.pop('\ufeff\"code\"')

            writer.writerow(normalized_dict)

        logging.debug(f"Table columns: {str(writer.fieldnames)}, fieldnames: {len(writer.fieldnames)}")

    def write_shoptet_table(self, base_url, shop_name):
        shoptet_file_name = "shoptet.csv"
        table = self.create_out_table_definition(name=shoptet_file_name, columns=["shop_base_url", "shop_name"])
        with open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
            writer = csv.DictWriter(out_file, table.columns)
            writer.writerow({"shop_base_url": base_url, "shop_name": shop_name})
        self.write_manifest(table)

    def _check_urls(self, params: dict):
        url_found = False
        for resource in RESOURCE_URLS:
            if url := params.get(resource):
                url_found = True
                if not self._is_csv_url(url):
                    raise UserException(
                        f"{url} is not a valid url. The export URL is most likely in unsupported format, "
                        f"please provide a CSV format export URL. "
                        f"If you are having trouble with creating permanent links, "
                        f"please visit the component's documentation.")
        for additional_datum in params.get(KEY_ADDITIONAL_DATA, []):
            url_found = True
            if not self._is_csv_url(additional_datum["url"]):
                raise UserException(f"{additional_datum['url']} is not a valid url. "
                                    f"The export URL is most likely in unsupported format, "
                                    f"please provide a CSV format export URL. "
                                    f"If you are having trouble with creating permanent links, "
                                    f"please visit the component's documentation.")

        if not url_found:
            raise UserException(f"At least one resource url from {RESOURCE_URLS} must be configured.")

    @staticmethod
    def _is_csv_url(url: str) -> bool:
        if ".csv" in url:
            return True
        return False


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

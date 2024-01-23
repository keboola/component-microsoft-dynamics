import logging
import json
import os
import sys
from kbc.env_handler import KBCEnvHandler
from dynamics.client import DynamicsClient
from dynamics.result import DynamicsWriter

from keboola.component.base import sync_action
from keboola.component.sync_actions import SelectElement

APP_VERSION = '1.0.2'

KEY_ORGANIZATION_URL = 'organization_url'
KEY_ENDPOINT = 'endpoint'
KEY_API_VERSION = 'api_version'
KEY_INCREMENTAL = 'incremental'
KEY_QUERY = 'query'
KEY_DEBUG = 'debug'
KEY_DOWNLOAD_FORMATTED_VALUES = "download_formatted_values"

MANDATORY_PARAMS = [KEY_ORGANIZATION_URL, KEY_ENDPOINT, KEY_API_VERSION]

AUTH_APP_KEY = 'appKey'
AUTH_APP_SECRET = '#appSecret'
AUTH_APPDATA = '#data'
AUTH_APPDATA_REFRESH_TOKEN = 'refresh_token'

sys.tracebacklimit = 0


class DynamicsComponent(KBCEnvHandler):

    def __init__(self):
        super().__init__(mandatory_params=MANDATORY_PARAMS, log_level='INFO')
        logging.info(f"Running component version {APP_VERSION}...")
        self.validate_config(MANDATORY_PARAMS)

        if self.cfg_params.get(KEY_DEBUG, False):
            logger = logging.getLogger()
            logger.setLevel(level='DEBUG')
            sys.tracebacklimit = 3

        auth = self.get_authorization()
        self.client_id = auth[AUTH_APP_KEY]
        self.client_secret = auth[AUTH_APP_SECRET]

        auth_data = json.loads(auth[AUTH_APPDATA])
        self.refresh_token = auth_data[AUTH_APPDATA_REFRESH_TOKEN]

        self.endpoint = self.cfg_params[KEY_ENDPOINT].lower()
        self.table = self.endpoint + '.csv'
        self.api_version = self.cfg_params[KEY_API_VERSION]
        self.resource_url = self.cfg_params[KEY_ORGANIZATION_URL]
        self.query = '&'.join([q for q in self.cfg_params.get(KEY_QUERY, '').split('\n') if q != ''])
        self.incremental = bool(self.cfg_params.get(KEY_INCREMENTAL, True))
        self.download_formatted_values = bool(self.cfg_params.get(KEY_DOWNLOAD_FORMATTED_VALUES, False))
        self.client = DynamicsClient(
            self.client_id, self.client_secret, self.resource_url, self.refresh_token, self.api_version)
        self.writer = None

    def get_authorization(self):
        try:
            return self.configuration.config_data["authorization"]["oauth_api"]["credentials"]
        except KeyError:
            logging.error("Authorization is missing in configuration file.")
            exit(1)

    def run(self):
        if self.endpoint not in self.client.api_endpoints:
            logging.error(
                f"Endpoint \"{self.endpoint}\" is not supported by your Dynamics instance. "
                f"Please, refer to documentation at "
                f"https://docs.microsoft.com/en-us/dynamics365/customer-engagement/web-api/entitytypes for"
                "the list of default available resources; or visit"
                f"{os.path.join(self.client.resource_url, 'EntityDefinitions')}"
                "for a complete list of all objects supported by your Dynamics instance.")
            sys.exit(1)

        logging.info(f"Downloading data for endpoint \"{self.endpoint}\".")
        has_more = True
        next_link = None
        request_count = 0
        primary_key = self.client.api_endpoints[self.endpoint]['PrimaryIdAttribute']

        while has_more:
            request_count += 1
            results, next_link = self.client.download_data(
                self.endpoint, query=self.query, next_link_url=next_link,
                download_formatted_values=self.download_formatted_values)  # noqa

            if self.writer is None:
                if not results:
                    logging.info(f"No data returned for endpoint \"{self.endpoint}\".")
                    sys.exit(0)

                self.writer = DynamicsWriter(
                    self.tables_out_path, self.endpoint, results, [primary_key], self.incremental)

            self.writer.writerows(results)
            has_more = True if next_link else False

            if not request_count % 20:
                logging.info(f"Made {request_count} requests to the API so far.")

        logging.info(f"Made {request_count} requests to the API in total for endpoint \"{self.endpoint}\".")

    @sync_action('get_endpoints')
    def get_endpoints(self):
        return [SelectElement(endpoint) for endpoint in self.client.api_endpoints]

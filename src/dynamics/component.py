import logging
import json
import os
import sys
from kbc.env_handler import KBCEnvHandler
from dynamics.client import DynamicsClient
from dynamics.result import DynamicsWriter

APP_VERSION = '1.0.0'

KEY_ORGANIZATIONURL = 'organization_url'
KEY_ENDPOINT = 'endpoint'
KEY_API_VERSION = 'api_version'
KEY_INCREMENTAL = 'incremental'
KEY_QUERY = 'query'
KEY_DEBUG = 'debug'

MANDATORY_PARAMS = [KEY_ORGANIZATIONURL, KEY_ENDPOINT, KEY_API_VERSION]

AUTH_APPKEY = 'appKey'
AUTH_APPSECRET = '#appSecret'
AUTH_APPDATA = '#data'
AUTH_APPDATA_REFRESHTOKEN = 'refresh_token'

sys.tracebacklimit = 0


class DynamicsComponent(KBCEnvHandler):

    def __init__(self):

        super().__init__(mandatory_params=MANDATORY_PARAMS, log_level='DEBUG')
        logging.info("Running component version %s..." % APP_VERSION)
        self.validate_config(MANDATORY_PARAMS)

        if self.cfg_params.get(KEY_DEBUG, False) is True:
            logger = logging.getLogger()
            logger.setLevel(level='DEBUG')

            sys.tracebacklimit = 3

        auth = self.get_authorization()
        self.parClientId = auth[AUTH_APPKEY]
        self.parClientSecret = auth[AUTH_APPSECRET]

        authData = json.loads(auth[AUTH_APPDATA])
        self.parRefreshToken = authData[AUTH_APPDATA_REFRESHTOKEN]

        self.parEndpoint = self.cfg_params[KEY_ENDPOINT].lower()
        self.parTable = self.parEndpoint + '.csv'
        self.parApiVersion = self.cfg_params[KEY_API_VERSION]
        self.parResourceUrl = self.cfg_params[KEY_ORGANIZATIONURL]
        self.parQuery = '&'.join([q for q in self.cfg_params.get(KEY_QUERY, '').split('\n') if q != ''])
        self.parIncremental = bool(self.cfg_params.get(KEY_INCREMENTAL, True))

        self.client = DynamicsClient(self.parClientId, self.parClientSecret,
                                     self.parResourceUrl, self.parRefreshToken,
                                     self.parApiVersion)

    def run(self):

        if self.parEndpoint not in self.client.varApiObjects:

            logging.error(' '.join([f"Endpoint \"{self.parEndpoint}\" is not supported by your Dynamics instance.",
                                    "Please, refer to documentation at",
                                    "https://docs.microsoft.com/en-us/dynamics365/" +
                                    "customer-engagement/web-api/entitytypes for",
                                    "the list of default available resources; or visit",
                                    f"{os.path.join(self.client.parResourceUrl, 'EntityDefinitions')}",
                                    "for a complete list of all objects supported by your Dynamics instance."]))
            sys.exit(1)

        else:

            logging.info(f"Downloading data for endpoint \"{self.parEndpoint}\".")

            _has_more = True
            _has_wrtr = False
            _next_link = None
            _req_count = 0
            _pk = self.client.varApiObjects[self.parEndpoint]['PrimaryIdAttribute']

            while _has_more is True:

                _req_count += 1
                _results, _next_link = self.client.downloadData(self.parEndpoint,
                                                                query=self.parQuery,
                                                                nextLinkUrl=_next_link)

                if _has_wrtr is False:

                    if len(_results) == 0:
                        logging.info(f"No data returned for endpoint \"{self.parEndpoint}\".")
                        _has_more = False
                        sys.exit(0)

                    else:
                        self.writer = DynamicsWriter(self.tables_out_path, self.parEndpoint, _results,
                                                     [_pk], self.parIncremental)

                        _has_wrtr = True

                self.writer.writerows(_results)
                _has_more = True if _next_link else False

                if _req_count % 20 == 0:
                    logging.info(f"Made {_req_count} requests to the API so far.")

        logging.info(f"Made {_req_count} requests to the API in total for endpoint \"{self.parEndpoint}\".")

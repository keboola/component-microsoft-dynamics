import logging
import json
import sys
from kbc.env_handler import KBCEnvHandler
from dynamics.client import DynamicsClient
from dynamics.result import DynamicsWriter

KEY_RESOURCE = 'resource_url'
KEY_ENDPOINT = 'endpoint'
KEY_API_VERSION = 'api_version'
KEY_INCREMENTAL = 'incremental'
KEY_QUERY = 'query'

MANDATORY_PARAMS = [KEY_RESOURCE, KEY_ENDPOINT, KEY_API_VERSION]

AUTH_APPKEY = 'appKey'
AUTH_APPSECRET = '#appSecret'
AUTH_APPDATA = '#data'
AUTH_APPDATA_REFRESHTOKEN = 'refresh_token'


class DynamicsComponent(KBCEnvHandler):

    def __init__(self):

        super().__init__(mandatory_params=MANDATORY_PARAMS)
        self.validate_config(MANDATORY_PARAMS)

        auth = self.getAuthorization()
        self.parClientId = auth[AUTH_APPKEY]
        self.parClientSecret = auth[AUTH_APPSECRET]

        authData = json.loads(auth[AUTH_APPDATA])
        self.parRefreshToken = authData[AUTH_APPDATA_REFRESHTOKEN]

        self.parEndpoint = self.cfg_params[KEY_ENDPOINT].lower()
        self.parTable = self.parEndpoint + '.csv'
        self.parApiVersion = self.cfg_params[KEY_API_VERSION]
        self.parResourceUrl = self.cfg_params[KEY_RESOURCE]
        self.parQuery = self.cfg_params.get(KEY_QUERY, None)
        self.parIncremental = self.cfg_params[KEY_INCREMENTAL]

        self.client = DynamicsClient(self.parClientId, self.parClientSecret,
                                     self.parResourceUrl, self.parRefreshToken, self.parApiVersion)

    def getAuthorization(self):

        try:
            return self.configuration.config_data["authorization"]["oauth_api"]["credentials"]

        except KeyError:
            logging.error("Authorization is missing.")
            sys.exit(1)

    def run(self):

        if self.parEndpoint not in self.client.varApiObjects:

            logging.error(' '.join([f"Endpoint \"{self.parEndpoint}\" is not supported. Please, refer to",
                                    "documentation at https://docs.microsoft.com/en-us/dynamics365/" +
                                    "customer-engagement/web-api/entitytypes for",
                                    "the list of available resources."]))
            sys.exit(1)

        else:
            _pk = self.client.varApiObjects[self.parEndpoint]['PrimaryIdAttribute']
            apiObject = self.client.queryData(self.parEndpoint, self.parQuery)

            if len(apiObject) == 0:
                logging.info(f"No data returned for endpoint \"{self.parEndpoint}\".")

            else:
                DynamicsWriter(self.tables_out_path, self.parTable, apiObject,
                               [_pk], self.parIncremental)

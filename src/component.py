import logging
import json
import os
import sys
from keboola.component import UserException
from keboola.component.base import ComponentBase, sync_action
from keboola.component.sync_actions import SelectElement
from dynamics.client import DynamicsClient
from dynamics.result import DynamicsWriter

APP_VERSION = '1.2.0'

KEY_ORGANIZATIONURL = 'organization_url'
KEY_ENDPOINT = 'endpoint'
KEY_API_VERSION = 'api_version'
KEY_INCREMENTAL = 'incremental'
KEY_QUERY = 'query'
KEY_DEBUG = 'debug'
KEY_DOWNLOAD_FORMATTED_VALUES = "download_formatted_values"

MANDATORY_PARAMS = [KEY_ORGANIZATIONURL, KEY_ENDPOINT, KEY_API_VERSION]

AUTH_APPKEY = 'appKey'
AUTH_APPSECRET = '#appSecret'
AUTH_APPDATA = '#data'
AUTH_APPDATA_REFRESHTOKEN = 'refresh_token'

sys.tracebacklimit = 0


class Component(ComponentBase):

    def __init__(self):

        super().__init__()
        logging.info("Running component version %s..." % APP_VERSION)

        self.cfg_params = self.configuration.parameters

        self._validate_config()

        if self.cfg_params.get(KEY_DEBUG, False) is True:
            logger = logging.getLogger()
            logger.setLevel(level='DEBUG')

            sys.tracebacklimit = 3

        auth = self.get_authorization()
        self.par_client_id = auth[AUTH_APPKEY]
        self.par_client_secret = auth[AUTH_APPSECRET]

        auth_data = json.loads(auth[AUTH_APPDATA])
        self.parRefreshToken = auth_data[AUTH_APPDATA_REFRESHTOKEN]

        self.par_endpoint = self.cfg_params[KEY_ENDPOINT].lower()
        self.parTable = self.par_endpoint + '.csv'
        self.parApiVersion = self.cfg_params[KEY_API_VERSION]
        self.parResourceUrl = self.cfg_params[KEY_ORGANIZATIONURL]
        self.parQuery = '&'.join([q for q in self.cfg_params.get(KEY_QUERY, '').split('\n') if q != ''])
        self.parIncremental = bool(self.cfg_params.get(KEY_INCREMENTAL, True))
        self.download_formatted_values = bool(self.cfg_params.get(KEY_DOWNLOAD_FORMATTED_VALUES, False))

        self.client = DynamicsClient(self.par_client_id, self.par_client_secret,
                                     self.parResourceUrl, self.parRefreshToken,
                                     self.parApiVersion)

    def get_authorization(self):

        try:
            return self.configuration.config_data["authorization"]["oauth_api"]["credentials"]
        except KeyError:
            raise UserException("Authorization is missing in configuration file.")

    def _validate_config(self) -> None:
        missing_parameters = [param for param in MANDATORY_PARAMS if param not in self.configuration.parameters]

        if missing_parameters:
            raise UserException(f"Missing required configuration parameters: {', '.join(missing_parameters)}")

    def run(self):

        pass

        if self.par_endpoint not in self.client.var_api_objects:

            raise UserException(' '.join([
                f"Endpoint \"{self.par_endpoint}\" is not supported by your Dynamics instance.",
                "Please, refer to documentation at",
                "https://docs.microsoft.com/en-us/dynamics365/" +
                "customer-engagement/web-api/entitytypes for",
                "the list of default available resources; or visit",
                f"{os.path.join(self.client.par_resource_url, 'EntityDefinitions')}",
                "for a complete list of all objects supported by your Dynamics instance."]))

        else:

            logging.info(f"Downloading data for endpoint \"{self.par_endpoint}\".")

            _has_more = True
            _has_wrtr = False
            _next_link = None
            _req_count = 0
            _pk = self.client.var_api_objects[self.par_endpoint]['PrimaryIdAttribute']

            while _has_more is True:

                _req_count += 1
                _results, _next_link = self.client.download_data(self.par_endpoint,
                                                                 query=self.parQuery,
                                                                 next_link_url=_next_link,
                                                                 download_formatted_values=self.download_formatted_values)  # noqa

                if _has_wrtr is False:

                    if len(_results) == 0:
                        logging.info(f"No data returned for endpoint \"{self.par_endpoint}\".")
                        _has_more = False
                        sys.exit(0)

                    else:
                        self.writer = DynamicsWriter(self.tables_out_path, self.par_endpoint, _results,
                                                     [_pk], self.parIncremental)

                        _has_wrtr = True

                self.writer.writerows(_results)
                _has_more = True if _next_link else False

                if _req_count % 20 == 0:
                    logging.info(f"Made {_req_count} requests to the API so far.")

        logging.info(f"Made {_req_count} requests to the API in total for endpoint \"{self.par_endpoint}\".")

    @sync_action('list_endpoints')
    def list_endpoints(self):

        try:
            self.client.get_entity_metadata()

        except Exception as e:
            raise UserException(f"Failed to list endpoints: {e}")

        return [SelectElement(value=obj, label=obj) for obj in self.client.var_api_objects.keys()]


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(1)
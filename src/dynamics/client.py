import logging
import os
import requests
from keboola.http_client import HttpClient
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from keboola.component import UserException

class DynamicsClient(HttpClient):
    MSFT_LOGIN_URL = 'https://login.microsoftonline.com/common/oauth2/token'
    MAX_RETRIES = 7
    PAGE_SIZE = 2000

    def __init__(self, client_id, client_secret, resource_url, refresh_token, api_version):

        self.par_client_id = client_id
        self.par_client_secret = client_secret
        self.par_resource_url = str(os.path.join(resource_url, 'api/data', api_version))
        self.par_resource_url_base = os.path.join(resource_url, '')
        self.par_refresh_token = refresh_token
        self.par_api_version = api_version

        super().__init__(base_url=self.par_resource_url, max_retries=self.MAX_RETRIES)
        _accessToken = self.refresh_token()

        _defHeader = {
            'Authorization': f'Bearer {_accessToken}',
            'Accept': 'application/json'
        }

        self._auth_header = _defHeader
        self.get_entity_metadata()

    def refresh_token(self):

        self._auth_header = {}

        headers_refresh = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }

        body_refresh = {
            'client_id': self.par_client_id,
            'grant_type': 'refresh_token',
            'client_secret': self.par_client_secret,
            'resource': self.par_resource_url_base,
            'refresh_token': self.par_refresh_token
        }

        req_refresh = self.post_raw(endpoint_path=self.MSFT_LOGIN_URL, headers=headers_refresh, data=body_refresh)
        sc_refresh, js_refresh = req_refresh.status_code, req_refresh.json()

        if sc_refresh == 200:
            logging.debug("Access token refreshed successfully.")
            return js_refresh['access_token']

        else:
            raise UserException(f"Could not refresh access token. Received {sc_refresh} - {js_refresh}.")

    def __response_hook(self, res, *args, **kwargs):

        if res.status_code == 401:
            token = self.refresh_token()
            self._auth_header = {"Authorization": f'Bearer {token}',
                                 "Accept": "application/json"}

            res.request.headers['Authorization'] = f'Bearer {token}'
            s = requests.Session()
            return self.requests_retry_session(session=s).send(res.request)

    def requests_retry_session(self, session=None):

        session = session or requests.Session()
        retry = Retry(
            total=self.max_retries,
            read=self.max_retries,
            connect=self.max_retries,
            backoff_factor=self.backoff_factor,
            status_forcelist=self.status_forcelist,
            method_whitelist=('GET', 'POST', 'PATCH', 'UPDATE', 'DELETE')
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        # append response hook
        session.hooks['response'].append(self.__response_hook)
        return session

    def get_entity_metadata(self):

        url_meta = os.path.join(self.base_url, 'EntityDefinitions')
        params_meta = {
            '$select': 'PrimaryIdAttribute,EntitySetName'
        }

        req_meta = self.get_raw(endpoint_path=url_meta, params=params_meta)
        sc_meta, js_meta = req_meta.status_code, req_meta.json()

        if sc_meta == 200:

            logging.debug("Obtained logical definitions of entities.")
            self.var_api_objects = {e['EntitySetName'].lower(): e for e in js_meta['value']
                                    if e['EntitySetName'] is not None}

        else:
            raise UserException(f"Could not obtain entity metadata for resource. Received: {sc_meta} - {js_meta}.")

    def download_data(self, endpoint, query=None, next_link_url=None, download_formatted_values=False):

        prefer_value = f"odata.maxpagesize={self.PAGE_SIZE}"
        if download_formatted_values:
            prefer_value = f'{prefer_value}, odata.include-annotations="OData.Community.Display.V1.FormattedValue"'

        headers_query = {
            'Prefer': prefer_value
        }

        if next_link_url is not None and next_link_url != '':
            url_query = next_link_url

        else:
            url_query = os.path.join(self.base_url, endpoint)

            if query is not None and query != '':
                url_query += '?' + query

        req_query = self.get_raw(endpoint_path=url_query, headers=headers_query)
        sc_query, js_query = req_query.status_code, req_query.json()

        if sc_query == 200:

            _results = js_query['value']
            _nextLink = js_query.get('@odata.nextLink', None)

            return _results, _nextLink

        else:

            _err_msg = js_query['error']['message']

            if 'Could not find a property named' in _err_msg:
                _add_msg = 'When querying foreign key fields, do not forget to ommit "fk" part of the field, e.g. ' + \
                           '"fk_accountid" -> "_accountid". Please, refer to the documentation for more information.'

            else:
                _add_msg = ''

            raise UserException(f"Could not query endpoint \"{endpoint}\"."
                                f"Received: {sc_query} - {_err_msg} {_add_msg}")

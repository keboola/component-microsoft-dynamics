import logging
import requests
import sys
from kbc.client_base import HttpClientBase
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urljoin


class DynamicsClient(HttpClientBase):
    MSFT_LOGIN_URL = 'https://login.microsoftonline.com/common/oauth2/token'
    MAX_RETRIES = 7
    PAGE_SIZE = 2000

    def __init__(self, client_id, client_secret, resource_url, refresh_token, api_version):
        self.client_id = client_id
        self.client_secret = client_secret
        self.resource_url = urljoin(resource_url, 'api/data', api_version)
        self.resource_url_base = urljoin(resource_url, '')
        self.refresh_token = refresh_token
        self.api_version = api_version
        super().__init__(base_url=resource_url, max_retries=self.MAX_RETRIES)
        self.refresh_access_token()
        self.api_endpoints = self.get_api_endpoints()

    def refresh_access_token(self):
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }
        data = {
            'client_id': self.client_id,
            'grant_type': 'refresh_token',
            'client_secret': self.client_secret,
            'resource': self.resource_url,
            'refresh_token': self.refresh_token
        }
        logging.debug("Refreshing access token.")
        response = self.post_raw(url=self.MSFT_LOGIN_URL, headers=headers, data=data)
        status_code, response_body = response.status_code, response.json()

        if status_code == 200:
            logging.debug("Token refreshed successfully.")
            access_token = response_body['access_token']
            header = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json'
            }
            self._auth_header = header
            return access_token

        logging.error(f"Could not refresh access token. Received {status_code} - {response_body}.")
        sys.exit(1)

    def __response_hook(self, response, *args, **kwargs):
        if response.status_code == 401:
            token = self.refresh_access_token()
            self._auth_header = {"Authorization": f'Bearer {token}',
                                 "Accept": "application/json"}

            response.request.headers['Authorization'] = f'Bearer {token}'
            session = requests.Session()
            return self.requests_retry_session(session=session).send(response.request)

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

    def get_api_endpoints(self):
        url = urljoin(self.base_url, 'EntityDefinitions')
        params = {'$select': 'PrimaryIdAttribute,EntitySetName'}

        try:
            response = self.get_raw(url=url, params=params)
        except requests.exceptions.ConnectionError as e:
            logging.error("Could not obtain logical object definitions. Please, check the "
                          f"organization URL or authorization.\n{e}")
            sys.exit(1)
        except requests.exceptions.RetryError as e:
            logging.error("Could not obtain logical object definitions. Please, check the "
                          f"the supported API version in correct format (v9.0, v9.1, etc.) is specified.\n{e}")
            sys.exit(1)

        status_code, response_body = response.status_code, response.json()

        if status_code == 200:
            logging.debug("Obtained logical definitions of entities.")
            endpoints_metadata = {
                entity['EntitySetName'].lower(): entity
                for entity in response_body['value']
                if entity['EntitySetName'] is not None
            }
            return endpoints_metadata

        logging.error("Could not obtain entity metadata for resource.")
        logging.error(f"Received: {status_code} - {response_body}.")
        sys.exit(1)

    def download_data(self, endpoint, query=None, next_link_url=None, download_formatted_values=False):
        prefer_value = f"odata.maxpagesize={self.PAGE_SIZE}"
        if download_formatted_values:
            prefer_value = f'{prefer_value}, odata.include-annotations="OData.Community.Display.V1.FormattedValue"'

        headers = {'Prefer': prefer_value}

        if next_link_url:
            url = next_link_url
        else:
            url = urljoin(self.base_url, endpoint)
            if query is not None:
                url += '?' + query

        response = self.get_raw(url=url, headers=headers)
        status_code, response_body = response.status_code, response.json()

        if status_code == 200:
            results = response_body['value']
            next_link_url = response_body.get('@odata.nextLink')
            return results, next_link_url

        error_msg = response_body['error']['message']
        add_msg = ''
        if 'Could not find a property named' in error_msg:
            add_msg = 'When querying foreign key fields, do not forget to ommit "fk" part of the field, e.g. ' + \
                       '"fk_accountid" -> "_accountid". Please, refer to the documentation for more information.'

        logging.error(
            f"Could not query endpoint \"{endpoint}\". Received: {status_code} - {error_msg}{add_msg}")
        sys.exit(1)

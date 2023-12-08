import logging
import os
import requests
import sys
from kbc.client_base import HttpClientBase
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class DynamicsClient(HttpClientBase):
    MSFT_LOGIN_URL = 'https://login.microsoftonline.com/common/oauth2/token'
    MAX_RETRIES = 7
    PAGE_SIZE = 2000

    def __init__(self, clientId, clientSecret, resourceUrl, refreshToken, apiVersion):

        self.parClientId = clientId
        self.parClientSecret = clientSecret
        self.parResourceUrl = os.path.join(resourceUrl, 'api/data', apiVersion)
        self.parResourceUrlBase = os.path.join(resourceUrl, '')
        self.parRefreshToken = refreshToken
        self.parApiVersion = apiVersion

        super().__init__(base_url=self.parResourceUrl, max_retries=self.MAX_RETRIES)
        _accessToken = self.refreshToken()

        _defHeader = {
            'Authorization': f'Bearer {_accessToken}',
            'Accept': 'application/json'
        }

        self._auth_header = _defHeader
        self.getEntityMetadata()

    def refreshToken(self):

        self._auth_header = {}

        headersRefresh = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }

        bodyRefresh = {
            'client_id': self.parClientId,
            'grant_type': 'refresh_token',
            'client_secret': self.parClientSecret,
            'resource': self.parResourceUrlBase,
            'refresh_token': self.parRefreshToken
        }

        reqRefresh = self.post_raw(url=self.MSFT_LOGIN_URL, headers=headersRefresh, data=bodyRefresh)
        scRefresh, jsRefresh = reqRefresh.status_code, reqRefresh.json()

        if scRefresh == 200:
            logging.debug("Access token refreshed successfully.")
            return jsRefresh['access_token']

        else:
            logging.error(f"Could not refresh access token. Received {scRefresh} - {jsRefresh}.")
            sys.exit(1)

    def __response_hook(self, res, *args, **kwargs):

        if res.status_code == 401:
            token = self.refreshToken()
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
            allowed_methods=('GET', 'POST', 'PATCH', 'UPDATE', 'DELETE')
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        # append response hook
        session.hooks['response'].append(self.__response_hook)
        return session

    def getEntityMetadata(self):

        """
        urlMeta = os.path.join(self.base_url, 'EntityDefinitions')
        paramsMeta = {
            '$select': 'PrimaryIdAttribute,EntitySetName'
        }
        """

        urlMeta = os.path.join(self.base_url, 'DMExportJobs')
        reqMeta = self.get_raw(url=urlMeta)

        # reqMeta = self.get_raw(url=urlMeta, params=paramsMeta)
        logging.info(reqMeta)
        logging.info(reqMeta.text)
        sys.exit(1)

        scMeta, jsMeta = reqMeta.status_code, reqMeta.json()

        if scMeta == 200:

            logging.debug("Obtained logical definitions of entities.")
            self.varApiObjects = {e['EntitySetName'].lower(): e for e in jsMeta['value']
                                  if e['EntitySetName'] is not None}

        else:

            logging.error("Could not obtain entity metadata for resource.")
            logging.error(f"Received: {scMeta} - {jsMeta}.")
            sys.exit(1)

    def downloadData(self, endpoint, query=None, nextLinkUrl=None, download_formatted_values=False):

        prefer_value = f"odata.maxpagesize={self.PAGE_SIZE}"
        if download_formatted_values:
            prefer_value = f'{prefer_value}, odata.include-annotations="OData.Community.Display.V1.FormattedValue"'

        headersQuery = {
            'Prefer': prefer_value
        }

        if nextLinkUrl is not None and nextLinkUrl != '':
            urlQuery = nextLinkUrl

        else:
            urlQuery = os.path.join(self.base_url, endpoint)

            if query is not None and query != '':
                urlQuery += '?' + query

        reqQuery = self.get_raw(url=urlQuery, headers=headersQuery)
        scQuery, jsQuery = reqQuery.status_code, reqQuery.json()

        if scQuery == 200:

            _results = jsQuery['value']
            _nextLink = jsQuery.get('@odata.nextLink', None)

            return _results, _nextLink

        else:

            _err_msg = jsQuery['error']['message']

            if 'Could not find a property named' in _err_msg:
                _add_msg = 'When querying foreign key fields, do not forget to ommit "fk" part of the field, e.g. ' + \
                           '"fk_accountid" -> "_accountid". Please, refer to the documentation for more information.'

            else:
                _add_msg = ''

            logging.error(''.join([f"Could not query endpoint \"{endpoint}\". ",
                                   f"Received: {scQuery} - {_err_msg} ",
                                   _add_msg]))
            sys.exit(1)

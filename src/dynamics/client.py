import logging
import os
import sys
from kbc.client_base import HttpClientBase


BASE_URL_REFRESH = 'https://login.microsoftonline.com/common/oauth2/token'


class DynamicsClientRefresh(HttpClientBase):

    def __init__(self):

        super().__init__(base_url=BASE_URL_REFRESH)

    def refreshAccessToken(self, clientId, clientSecret, resourceUrl, refreshToken):

        logging.debug("Refreshing access token.")

        headersRefresh = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'application/json'
        }

        bodyRefresh = {
            'client_id': clientId,
            'grant_type': 'refresh_token',
            'client_secret': clientSecret,
            'resource': resourceUrl,
            'refresh_token': refreshToken
        }

        reqRefresh = self.post_raw(url=self.base_url, headers=headersRefresh, data=bodyRefresh)
        scRefresh, jsRefresh = reqRefresh.status_code, reqRefresh.json()

        if scRefresh == 200:

            logging.debug("Token refreshed successfully.")
            return jsRefresh['access_token']

        else:

            logging.error(f"Could not refresh access token. Received {scRefresh} - {jsRefresh}.")
            sys.exit(1)


class DynamicsClient(HttpClientBase):

    def __init__(self, clientId, clientSecret, resourceUrl, refreshToken, apiVersion):

        self.parClientId = clientId
        self.parClientSecret = clientSecret

        self.parAccessToken = self.refreshToken(self.parClientId, self.parClientSecret,
                                                os.path.join(resourceUrl, ''), refreshToken)

        _defHeader = {
            'Authorization': f'Bearer {self.parAccessToken}',
            'Accept': 'application/json'
        }

        self.parResourceUrl = os.path.join(resourceUrl, 'api/data', apiVersion)

        super().__init__(base_url=self.parResourceUrl, default_http_header=_defHeader)
        self.getEntityMetadata()

    def refreshToken(self, clientId, clientSecret, resourceUrl, refreshToken):

        return DynamicsClientRefresh().refreshAccessToken(clientId, clientSecret, resourceUrl, refreshToken)

    def getEntityMetadata(self):

        urlMeta = os.path.join(self.base_url, 'EntityDefinitions')
        paramsMeta = {
            '$select': 'PrimaryIdAttribute,EntitySetName'
        }

        reqMeta = self.get_raw(url=urlMeta, params=paramsMeta)
        scMeta, jsMeta = reqMeta.status_code, reqMeta.json()

        if scMeta == 200:

            logging.debug("Obtained logical definitions of entities.")
            self.varApiObjects = {e['EntitySetName'].lower(): e for e in jsMeta['value']}

        else:

            logging.error("Could not obtain entity metadata for resource.")
            logging.error(f"Received: {scMeta} - {jsMeta}.")
            sys.exit(1)

    def queryData(self, endpoint, query=None):

        resultsQuery = []

        headersQuery = {
            'Prefer': 'odata.maxpagesize=5000'
        }

        urlQuery = os.path.join(self.base_url, endpoint)

        if query is not None:
            urlQuery += '?' + query

        _nextLink = True

        while _nextLink is True:

            reqQuery = self.get_raw(url=urlQuery, headers=headersQuery)
            scQuery, jsQuery = reqQuery.status_code, reqQuery.json()

            if scQuery == 200:

                resultsQuery += jsQuery['value']
                urlQuery = jsQuery.get('@odata.nextLink', None)
                _nextLink = True if urlQuery else False

            else:

                logging.error(f"Could not query endpoint {endpoint}. Received: {scQuery} - {jsQuery}.")
                sys.exit(1)

        return resultsQuery

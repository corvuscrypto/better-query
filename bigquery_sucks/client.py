"""
Base client for the API. This builds on top of the AuthorizedSession
class just to make things a bit easier.
"""
import json
import urllib3
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.credentials import Credentials
from bigquery_sucks.entities.projects import ProjectResource


class Client():
    def __init__(self, auth_dict=None):
        if not auth_dict:
            raise ValueError("requires authentication")
        del auth_dict['type']
        credentials = Credentials(None, token_uri="https://accounts.google.com/o/oauth2/token", **auth_dict)
        self._transport = AuthorizedSession(credentials)
        self.projects = ProjectResource(self)

    @staticmethod
    def from_credentials_file(filename):
        with open(filename, "rb") as credentials_file:
            return Client(json.load(credentials_file))

    def get(self, url, params=None):
        if params:
            # clean the params
            clean_params = {}
            for k, v in params.items():
                if v:
                    clean_params[k] = v
            url += "?" + urllib3.request.urlencode(clean_params)
        return self._transport.request(method="GET", url=url)

    def post(self, url, data=None):
        return self._transport.request(method="POST", url=url, data=json.dumps(data), headers={"Content-Type": "application/json"})

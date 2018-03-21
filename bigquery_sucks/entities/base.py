"""
Base Entity class for maximum convenience (and effort)
"""


class BaseResource(object):
    global_base_url = "https://www.googleapis.com/bigquery/v2"

    def __init__(self, client):
        self.client = client


class LazyLoadedModel(object):
    lazy_attributes = []

    def __init__(self, client):
        self.client = client
        self._loaded = False
        self.url = ""

    def __getattr__(self, attr_name):
        if attr_name in self.lazy_attributes and not self._loaded:
            self.load()
            self._loaded = True
        return super(LazyLoadedModel, self).__getattribute__(attr_name)

    def load(self):
        raise NotImplemented

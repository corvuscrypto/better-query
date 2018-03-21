"""
Class for Table related structures
"""
from bigquery_sucks.entities.base import BaseResource
from bigquery_sucks.entities.base import LazyLoadedModel


class TableResource(BaseResource):

    def list(self, project_id, dataset_id, max_results=None, page_token=None):
        url = self.global_base_url + "/projects/" + project_id + "/datasets/" + dataset_id + "/tables"
        params = {
            "maxResults": max_results,
            "pageToken": page_token
        }
        response = self.client.get(url, params).json()
        tables = []
        for table_data in response['tables']:
            tables.append(Table(self.client, table_data['tableReference']))
        return tables


class DatasetTableResource(BaseResource):
    def __init__(self, client, project_id, dataset_id):
        self.client = client
        self.base_url = self.global_base_url + "/projects/" + project_id + "/datasets/" + dataset_id

    def list(self, max_results=None, page_token=None):
        params = {
            "maxResults": max_results,
            "pageToken": page_token
        }
        response = self.client.get(self.base_url + "/tables", params).json()
        tables = []
        for table_data in response['tables']:
            tables.append(Table(self.client, table_data['tableReference']))
        return tables


class Table(LazyLoadedModel):

    url_template = "https://www.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"

    lazy_attributes = [
        "creation_time",
        "last_modified_time",
        "num_bytes",
        "num_long_term_bytes",
        "num_rows"
    ]

    def __init__(self, client, table_data):
        super(Table, self).__init__(client)
        self.project_id = table_data['projectId']
        self.dataset_id = table_data['datasetId']
        self.id = table_data['tableId']
        self.url = self.url_template.format(
            table_id=self.id,
            dataset_id=self.dataset_id,
            project_id=self.project_id
        )

    def load(self):
        table_data = self.client.get(self.url).json()
        self.creation_time = table_data['creationTime']
        self.last_modified_time = table_data['lastModifiedTime']
        self.num_bytes = table_data['numBytes']
        self.num_long_term_bytes = table_data['numLongTermBytes']
        self.num_rows = table_data['numRows']

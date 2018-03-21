"""
Class and model for Datasets yo
"""
from bigquery_sucks.entities.base import BaseResource
from bigquery_sucks.entities.base import LazyLoadedModel


class DatasetResource(BaseResource):

    def list(self, project_id, all=None, filter=None,
             max_results=None, page_token=None):
        url = self.global_base_url + "/projects/" + project_id + "/datasets"
        params = {
            "all": all,
            "filter": filter,
            "maxResults": max_results,
            "pageToken": page_token
        }
        self.client.get(url, params)


class ProjectDatasetResource(BaseResource):
    def __init__(self, client, project_id):
        self.client = client
        self.base_url = self.global_base_url + "/projects/" + project_id

    def list(self, all=None, filter=None, max_results=None, page_token=None):
        params = {
            "all": all,
            "filter": filter,
            "maxResults": max_results,
            "pageToken": page_token
        }
        response = self.client.get(self.base_url + "/datasets", params).json()
        datasets = []
        for dataset_data in response['datasets']:
            datasets.append(Dataset(self.client, dataset_data['datasetReference']))
        response['datasets'] = datasets
        return response


class Dataset(LazyLoadedModel):

    url_template = "https://www.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}"

    lazy_attributes = [
        "creation_time",
        "access",
        "last_modified_time"
    ]

    def __init__(self, client, dataset_data):
        super(Dataset, self).__init__(client)
        self.project_id = dataset_data['projectId']
        self.id = dataset_data['datasetId']
        self.url = self.url_template.format(
            dataset_id=self.id,
            project_id=self.project_id
        )

    def load(self):
        dataset_data = self.client.get(self.url).json()
        self.creation_time = dataset_data['creationTime']
        self.access = dataset_data['access']
        self.last_modified_time = dataset_data['lastModifiedTime']

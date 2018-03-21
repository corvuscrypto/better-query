"""
Class and methods related to BigQuery Project information.
"""
from bigquery_sucks.entities.base import BaseResource
from bigquery_sucks.entities.datasets import ProjectDatasetResource


class ProjectResource(BaseResource):
    def list(self, max_results=None, page_token=None):
        """
        List your projects
        """
        url = self.global_base_url + "/projects"
        params = {
            "maxResults": max_results,
            "pageToken": page_token
        }
        response = self.client.get(url, params).json()
        projects = []
        for project_data in response['projects']:
            projects.append(Project(self.client, project_data))
        response['projects'] = projects
        return response


class Project(object):
    """
    Project model representation of the data we get back from
    the Resource methods.

    These objects aren't designed to be instantiated by the user,
    but by the ProjectResource.

    However, this can be instantiated manually if all the info is
    provided
    """
    def __init__(self, client, project_data):
        self.client = client
        self.id = project_data['id']
        self.numeric_id = project_data['numericId']
        self.friendly_name = project_data['friendlyName']
        self.datasets = ProjectDatasetResource(client, self.numeric_id)

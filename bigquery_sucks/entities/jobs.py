"""
Classes and methods for Jobs including
query jobs
"""
from math import ceil
from threading import Thread
from queue import Queue
from bigquery_sucks.entities.base import BaseResource


class Query(BaseResource):
    def __init__(self, client, project_id):
        self.client = client
        self.url = (
            self.global_base_url + "/projects/"
            + project_id + "/queries"
        )

    def run(self, query, legacy_sql=False):
        data = {
            "query": query,
            "useLegacySql": legacy_sql,
            "maxResults": 0,
            "kind": "bigquery#queryRequest"
        }
        response = self.client.post(self.url, data=data)
        return QueryJob(self.client, response.json())


class QueryJob(BaseResource):
    def __init__(self, client, job_data):
        self.client = client
        self.id = job_data['jobReference']['jobId']
        self.project_id = job_data['jobReference']['projectId']
        self.total_rows = int(job_data['totalRows'])
        self.schema = job_data['schema']


class StopMessage(object):
    pass


class IteratorThread(Thread):
    def __init__(self, queue, client, project_id, job_id, start_index, max_results):
        super(IteratorThread, self).__init__()
        self.queue = queue
        self.client = client
        self.start_index = start_index
        self.max_results = max_results
        self.results = 0
        self.url = "https://www.googleapis.com/bigquery/v2/projects/" + project_id + "/queries/" + job_id

    def run(self):
        params = {
            "startIndex": self.start_index,
            "maxResults": min(5000, self.max_results),
        }
        res = self.client.get(self.url, params=params)
        results = res.json()
        del params['startIndex']
        while 'pageToken' in results and 'rows' in results:
            for row in results['rows']:
                self.queue.put(row)
            self.max_results -= len(results['rows'])
            params['pageToken'] = results['pageToken']
            res = self.client.get(self.url, params=params)
            results = res.json()
        if 'rows' in results:
            for row in results['rows']:
                self.queue.put(row)
        self.queue.put(StopMessage())


class MultiThreadIterator(object):
    def __init__(self, client, job, threads=2, max_queue=10000):
        self.num_rows = job.total_rows
        self.job_id = job.id
        self.queue = Queue(max_queue)
        self.started = False
        self.num_threads = threads
        rows_per_thread = ceil(job.total_rows / threads)
        for i in range(threads):
            thread = IteratorThread(
                self.queue,
                client,
                job.project_id,
                job.id,
                i * rows_per_thread,
                rows_per_thread
            )
            thread.start()

    def generator(self):
        stops = 0
        while True:
            message = self.queue.get()
            self.queue.task_done()
            if isinstance(message, StopMessage):
                stops += 1
                if stops == self.num_threads:
                    raise StopIteration
                continue
            yield message

    def __iter__(self):
        if self.started:
            raise StopIteration
        self.started = True
        return (x for x in self.generator())

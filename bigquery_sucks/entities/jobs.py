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

    def run(self, query, legacy_sql=False, threads=1):
        data = {
            "query": query,
            "useLegacySql": legacy_sql,
            "maxResults": 0,
            "kind": "bigquery#queryRequest"
        }
        response = self.client.post(self.url, data=data)
        return QueryJob(self.client, response.json(), threads)


class QueryJob(BaseResource):
    def __init__(self, client, job_data, threads=1):
        self.client = client
        self.id = job_data['jobReference']['jobId']
        self.project_id = job_data['jobReference']['projectId']
        self.total_rows = int(job_data['totalRows'])
        self.schema = job_data['schema']
        self.url = "https://www.googleapis.com/bigquery/v2/projects/" + self.project_id + "/queries/" + self.id
        self.threads = threads

    def __iter__(self):
        if self.threads > 1:
            return (x for x in MultiThreadIterator(self.client, self, self.threads))
        return (x for x in Iterator(self.client, self))


class Iterator(object):

    TRANSFORMS = {
        "STRING": str,
        "INTEGER": int,
        "FLOAT": float,
        "BOOLEAN": bool
    }

    def __init__(self, client, job, start_index=0):
        self.client = client
        self.job = job
        self.start_index = start_index
        self._load_schema()

    def _load_schema(self):
        types = []
        for field in self.job.schema['fields']:
            name = field['name']
            _type = field['type']
            types.append((name, self.TRANSFORMS[_type]))
        self.schema = types

    def row_to_dict(self, row):
        data = {}
        for i, field in enumerate(row['f']):
            value = field['v']
            name, _type = self.schema[i]
            if value is None:
                data[name] = None
            else:
                data[name] = _type(value)
        return data

    def get_pages(self):
        page_token = ""
        while page_token is not None:
            params = {
                "pageToken": page_token,
                "startIndex": self.start_index
            }
            results = self.client.get(self.job.url, params).json()
            self.start_index = None
            page_token = results.get("pageToken")
            yield results.get("rows", [])

    def __iter__(self):
        for page in self.get_pages():
            for row in page:
                row = self.row_to_dict(row)
                yield row


class StopMessage(object):
    pass


class IteratorThread(Thread):
    def __init__(self, queue, client, job, start_index, max_results):
        self.queue = queue
        self.results_left = max_results
        self.start_index = start_index
        self.iterator = Iterator(client, job, start_index)
        super(IteratorThread, self).__init__()

    def run(self):
        for row in self.iterator:
            self.queue.put(row)
            self.results_left -= 1
            if self.results_left == 0:
                break
        self.queue.put(StopMessage())


class MultiThreadIterator(object):
    def __init__(self, client, job, threads=2, queue_size=10000):
        self.started = False
        self.num_rows = job.total_rows
        self.queue = Queue(queue_size)
        self.num_threads = threads
        rows_per_thread = ceil(job.total_rows / threads)
        self.threads = []
        for i in range(threads):
            thread = IteratorThread(
                self.queue,
                client.copy(),
                job,
                i * rows_per_thread,
                rows_per_thread
            )
            self.threads.append(thread)
        for t in self.threads:
            t.start()

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

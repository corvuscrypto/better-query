# BigQuery Sucks

It really does. It has some of the worst documentation I've seen, the worst auto-generated code, and the worst client interfaces.
You deserve better, even if your company has decided you have to use BigQuery in the first place. If that's the case, I strongly suggest
trying to convince them to avoid it and move to something more... practical.

## Motivation

The client in python is pretty poor. It barely gets the job done and you often have to use a lot of intermediate models to represent a metastate
in order to use all of the features. This is **shit**. On top of that, parallelizing retrieval of results is damn near impossible without manually
splitting up the queries manually, which does not scale well dynamically. This is actually the main motivation for this. If I only implement one thing, it will be the ability to parallelize retrieval of a query result.

## Usage

The first thing you'll need is a `Client`.

```python
from bigquery_sucks.client import Client


client = Client.from_credentials_file(
    "/path/to/application_default_credentials.json"
)

```

Next, surely you want to list the projects associated with your account

```python
projects = client.projects.list()
for project in projects:
    print(project.friendly_name)
```

Then maybe you want to list the datasets for a particular project?

```python
datasets = projects[0].datasets.list()
for dataset in datasets:
    print(dataset.id)
```

Or if you know the API endpoint for a resource, you can call it directly.

```python
client.get("https://www.googleapis.com/bigquery/v2/projects")
```

There will be documentation on each part of this lib at some point. Just not yet

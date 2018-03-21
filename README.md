# BigQuery Sucks

It really does. It has some of the worst documentation I've seen, the worst auto-generated code, and the worst client interfaces.
You deserve better, even if your company has decided you have to use BigQuery in the first place. If that's the case, I strongly suggest
trying to convince them to avoid it and move to something more... practical.

## Motivation

The client in python is pretty poor. It barely gets the job done and you often have to use a lot of intermediate models to represent a metastate
in order to use all of the features. This is **shit**. On top of that, parallelizing retrieval of results is damn near impossible without manually
splitting up the queries manually, which does not scale well dynamically. This is actually the main motivation for this. If I only implement one thing, it will be the ability to parallelize retrieval of a query result.

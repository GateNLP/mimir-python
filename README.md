# mimir-python

A client library for working with Mímir search in Python.

Supports the Mímir API as described in [the documentation for mimir](https://gate.ac.uk/mimir/doc/mimir-guide.pdf).

Also adds context helpers to deal with initialising and removing queries, and iterators to allow for more pythonic use. 

```python
from mimir_helpers import MimirHelper

# Configuring the endpoint. Don't forget the trailing slash.
helper = MimirHelper("http://mymimirendpoint.example.com/search/") 

# Issue a query manually.
queryId = helper.postQuery("{Token}")
helper.wait(queryId) # Wait until the search is done.
print helper.documentsCount(queryId)
print helper.documentId(queryId, rank=0)
helper.close(queryId) # Politely tell the server we're done with this query.

# Issue a query manually using the context manager
with helper.query("{Token}") as resultSet:
	print resultSet.documentsCount()
	print resultSet.documentId(rank=0)

# Iterate over the results from a query.
with helper.query("{Token}") as resultSet:
	for result in resultSet.results():
		print result.text
		print result.documentId
		print result.hits
		print result.metadata
		print result.tokens
```

Depends on python requests library.

Have fun!

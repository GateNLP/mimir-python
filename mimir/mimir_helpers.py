import requests, time
from urlparse import urljoin
import xml.etree.ElementTree as ET
from contextlib import contextmanager

NS = {"mimir": "http://gate.ac.uk/ns/mimir"}
class MimirException(Exception):
	pass

class MimirMetadata(object): 
	def __init__(self, documentTitle, documentURI, metadata):
		self.documentTitle = documentTitle
		self.documentURI = documentURI
		self.metadata = metadata

class MimirDocumentHit(object):
	def __init__(self, documentId, termPosition, length):
		self.documentId = documentId
		self.termPosition = termPosition
		self.length = length

class MimirDocumentToken(object):
	def __init__(self, text, position = -1, isSpace = False):
		self.position = position
		self.text = text
		self.isSpace = isSpace

class MimirResult(object):
	def __init__(self, metadata, documentId, tokens, hits):
		self.metadata = metadata
		self.documentId = documentId
		self.tokens = tokens
		self.text = u"".join(token.text for token in tokens)
		self.hits = hits

class MimirHelper(object):
	def __init__(self, endpoint):
		self.endpoint = endpoint

	def __queryMimir(self, path, **params):
		"""
			Runs a mimir query on the given path with the given parameters
		
			Will do the error handling in the case of a mimir error.
			
			@returns XML Element for the data field of the response

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
					response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response

		"""
		result = requests.get(urljoin(self.endpoint, path),
			params=params)

		root = ET.fromstring(result.text.encode('utf-8'))

		state = root.find("mimir:state", NS).text

		if state == "ERROR":
			message = root.find("mimir:error", NS).text
			raise MimirException(message)
		else:
			return root.find("mimir:data", NS)

	def wait(self, queryId): 
		"""Waits for the indicated query to finish.

			Should be called before trying to get hits.

			@returns Returned queryId

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
					response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		return self.__queryMimir("documentsCountSync", queryId = queryId)

	def close(self, queryId): 
		"""Releases the given queryId

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
					response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		return self.__queryMimir("close", queryId = queryId)

	@contextmanager
	def query(self, query): 
		"""Posts a query in a contextmanager that closes the query when done"""
		queryId = self.postQuery(query)
		self.wait(queryId)
		try:
			yield MimirResultSet(self, queryId)
		finally:
			self.close(queryId)


	def postQuery(self, query):
		"""
			Starts a new query of the given value, and returns the queryId. 

			queryId is also stored in the object instance for ease of use.

			@returns Returned queryId

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
					response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		result = self.__queryMimir("postQuery", queryString = query)
		return result.find("mimir:queryId", NS).text
		
	def documentsCurrentCount(self, queryId):
		"""
			Asks Mimir how many results there are available.

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		result = self.__queryMimir("documentsCurrentCount", queryId = queryId)
		return long(result.find("mimir:value", NS).text)

	def documentsCount(self, queryId):
		"""
			Asks Mimir how many results there are in total. -1 if not completed.

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		result = self.__queryMimir("documentsCount", queryId = queryId)
		return long(result.find("mimir:value", NS).text)

	def documentMetadata(self, queryId, rank, fieldNames = []):
		"""
			Returns the metadata for the given document, including any additional fieldNames

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		if fieldNames != []:
			fieldNames = ",".join(fieldNames)
			result = self.__queryMimir("documentMetadata", queryId = queryId, rank=rank, fieldNames = fieldNames)
		else:
			result = self.__queryMimir("documentMetadata", queryId = queryId, rank=rank)

		uri = result.find("mimir:documentURI", NS).text
		title = result.find("mimir:documentTitle", NS).text
		metadata = result.findall("mimir:metadataField", NS)

		metadata = {e.attrib["name"]: e.attrib["value"] for e in metadata}

		return MimirMetadata(title, uri, metadata)

	def documentId(self, queryId, rank):
		"""
			Returns the id for the given document

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		result = self.__queryMimir("documentId", queryId = queryId, rank=rank)
		return long(result.find("mimir:value", NS).text)

	def documentHits(self, queryId, rank):
		"""
			Returns the hits for the given document

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		result = self.__queryMimir("documentHits", queryId = queryId, rank=rank)

		hits = []

		for hit in result.find("mimir:hits", NS).findall("mimir:hit", NS):
			hits.append(MimirDocumentHit(hit.attrib["documentId"],
										 hit.attrib["termPosition"],
										 hit.attrib["length"]))

		return hits

	def documentTextTokens(self, queryId, rank, termPosition = 0, length = None):
		"""
			Returns the text for the given document as series of MimirDocumentTokens

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		if length != None:
			result = self.__queryMimir("documentText", queryId = queryId, 
				rank=rank, 
				termPosition = termPosition,
				length=length)
		else:
			result = self.__queryMimir("documentText", queryId = queryId, 
				rank=rank, 
				termPosition = termPosition)

		tokens = []

		for tag in result.getchildren():
			if tag.tag == "text":
				tokens.append(MimirDocumentToken(tag.text,
										 position = tag.attr["position"]))
			else:
				tokens.append(MimirDocumentToken(tag.text, isSpace=True))

		return tokens

	def documentText(self, queryId, rank, termPosition = 0, length = None):
		"""
			Returns the text for the given document as a string

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		return u"".join(t.text for t in self.documentTextTokens(queryId, rank, termPosition, length))

	def renderDocument(self, queryId, rank):
		"""
			Returns the HTML for the result text of the given document as a string

			@throws RequestException if there was a problem with the request (eg the document or query couldn't be found)
		"""
		result = requests.get(urljoin(self.endpoint, "renderDocument"),
			params={"queryId": queryId, "rank": rank})

		return result.text

	def renderDocumentById(self, documentId):
		"""
			Returns the HTML for the entire document by Id

			@throws RequestException if there was a problem with the request (eg the document or query couldn't be found)
		"""
		result = requests.get(urljoin(self.endpoint, "renderDocument"),
			params={"documentId": documentId})

		return result.text

	def metadata(self, query, fieldNames=[]):
		"""
			Returns an iterable for the query which yields the selected metadata.

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""	
		with self.query(query) as resultSet:
			for result in resultSet.metadata(fieldNames):
				yield result


	def ids(self, query):
		"""
			Returns an iterable for the query which yields the document IDs.

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""	
		with self.query(query) as resultSet:
			for result in resultSet.ids():
				yield result

	def results(self, query, metadataFieldNames = []):
		"""
			Returns an iterable for the query which yields the complete results objects.

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""	
		with self.query(query) as resultSet:
			for result in resultSet.results(metadataFieldNames):
				yield result
				

class MimirResultSet(object):
	def __init__(self, mimirHelper, queryId):
		self.mimirHelper = mimirHelper
		self.queryId = queryId

	def close(self): 
		"""Releases the query

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
					response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		return self.mimirHelper.close(self.queryId)

		
	def documentsCurrentCount(self):
		"""
			Asks Mimir how many results there are available.

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		return self.mimirHelper.documentsCurrentCount(self.queryId)

	def documentsCount(self):
		"""
			Asks Mimir how many results there are in total. -1 if not completed.

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		return self.mimirHelper.documentsCount(self.queryId)

	def documentMetadata(self, rank, fieldNames = []):
		"""
			Returns the metadata for the given document, including any additional fieldNames

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		return self.mimirHelper.documentMetadata(self.queryId, rank, fieldNames)

	def documentId(self, rank):
		"""
			Returns the id for the given document

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		return self.mimirHelper.documentId(self.queryId, rank)

	def documentHits(self, rank):
		"""
			Returns the hits for the given document

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		return self.mimirHelper.documentHits(self.queryId, rank)

	def documentTextTokens(self, rank, termPosition = 0, length = None):
		"""
			Returns the text for the given document as series of MimirDocumentTokens

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		return self.mimirHelper.documentTextTokens(self.queryId, rank, termPosition, length)

	def documentText(self, rank, termPosition = 0, length = None):
		"""
			Returns the text for the given document as a string

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""
		return self.mimirHelper.documentText(self.queryId, rank, termPosition, length)

	def renderDocument(self, rank):
		"""
			Returns the HTML for the result text of the given document as a string

			@throws RequestException if there was a problem with the request (eg the document or query couldn't be found)
		"""
		return self.mimirHelper.renderDocument(self.queryId, rank)


	def metadata(self, fieldNames=[]):
		"""
			Returns an iterable for the query which yields the selected metadata.

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""	
		for rank in range(0, self.documentsCount()):
			yield self.documentMetadata(rank, fieldNames)


	def ids(self):
		"""
			Returns an iterable for the query which yields the document IDs.

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""	
		for rank in range(0, self.documentsCount()):
			yield self.documentId(rank)

	def results(self, metadataFieldNames = []):
		"""
			Returns an iterable for the query which yields the complete results objects.

			@throws RequestException if there was a problem with the request
			@throws MimirException if there was a problem with the mimir query or 
				response could not be read
			@throws xml.etree.ElementTree.ParseError if parse was not possible on response
		"""	
		for rank in range(0, self.documentsCount()):
			yield MimirResult(  self.documentMetadata(rank, metadataFieldNames), 
								self.documentId(rank),
								self.documentTextTokens(rank),
								self.documentHits(rank))

	def __iter__(self):
		for result in self.results():
			yield result
import unittest
from mimir_helpers import *
TEST_ENDPOINT = "http://gateservice9.dcs.shef.ac.uk:8086/angus/mimir/brexit-run4-all-fixed/search/"

class TestMimirHelpers(unittest.TestCase):
	def setUp(self):
		self.mimir = MimirHelper(TEST_ENDPOINT)

	def testPostQueryValid(self):
		queryId = self.mimir.postQuery("{UserID}")
		self.assertIsInstance(queryId, str)
		self.assertNotEqual(len(queryId), 0)

	def testPostQueryInvalid(self):
		with self.assertRaises(MimirException) as cm:
			self.mimir.postQuery("{UserID")

		self.assertEqual(cm.exception.message, 'Encountered "<EOF>" at line 1, column 7.\n'+\
			'Was expecting one of:\n    "}" ...\n    <tok> ...\n    ')

	def testDocumentsCurrentCount(self):
		queryId = self.mimir.postQuery("{UserID}")
		count = self.mimir.documentsCurrentCount(queryId)

		self.assertIsInstance(count, long)
		self.assertGreater(count, 0)

	def testMetaDataNoFields(self):
		queryId = self.mimir.postQuery("{UserID}")
		self.mimir.wait(queryId)
		metadata = self.mimir.documentMetadata(queryId, "1")

		self.assertIsInstance(metadata, MimirMetadata)

		self.assertIsInstance(metadata.documentTitle, str)
		self.assertGreater(len(metadata.documentTitle), 0)

		self.assertIsInstance(metadata.documentURI, str)
		self.assertGreater(len(metadata.documentURI), 0)

	def testMetaDataField(self):
		queryId = self.mimir.postQuery("{UserID}")
		self.mimir.wait(queryId)
		metadata = self.mimir.documentMetadata(queryId, "1", fieldNames=["author"])

		self.assertIsInstance(metadata, MimirMetadata)

		self.assertIsInstance(metadata.documentTitle, str)
		self.assertGreater(len(metadata.documentTitle), 0)

		self.assertIsInstance(metadata.documentURI, str)
		self.assertGreater(len(metadata.documentURI), 0)

		self.assertIsInstance(metadata.metadata, dict)
		self.assertEqual(len(metadata.metadata), 1)

		self.assertTrue("author" in metadata.metadata)
		self.assertIsInstance(metadata.metadata["author"], str)

	def testQuery(self):
		with self.mimir.query("{UserID}") as resultSet:
			count = resultSet.documentsCurrentCount()

			self.assertIsInstance(count, long)
			self.assertGreater(count, 0)

	def testMetaDataIterator(self):
		seenCount = 0

		for metadata in self.mimir.metadata("{UserID}", fieldNames=["author"]):
			self.assertIsInstance(metadata, MimirMetadata)

			self.assertIsInstance(metadata.documentTitle, str)
			self.assertGreater(len(metadata.documentTitle), 0)

			self.assertIsInstance(metadata.documentURI, str)
			self.assertGreater(len(metadata.documentURI), 0)

			self.assertIsInstance(metadata.metadata, dict)
			self.assertEqual(len(metadata.metadata), 1)

			self.assertTrue("author" in metadata.metadata)
			self.assertIsInstance(metadata.metadata["author"], str)

			seenCount += 1
			if seenCount == 100:
				break # Don't iterate through the entire lot because there could be loads.

	def testDocumentHits(self):
		with self.mimir.query("{Hashtag}") as resultSet:
			hits = resultSet.documentHits(0)

			self.assertGreater(len(hits), 0)
			for hit in hits:
				self.assertIsInstance(hit, MimirDocumentHit)


	def testDocumentTextTokens(self):
		with self.mimir.query("{Hashtag}") as resultSet:
			tokens = resultSet.documentTextTokens(0)

			self.assertGreater(len(tokens), 0)
			for token in tokens:
				self.assertIsInstance(token, MimirDocumentToken)

	def testDocumentText(self):
		with self.mimir.query("{Hashtag}") as resultSet:
			text = resultSet.documentText(0)
			self.assertGreater(len(text), 0)
			self.assertIsInstance(text, unicode)

	def testDocumentRender(self):
		documentId = 0
		with self.mimir.query("{Hashtag}") as resultSet:
			textWithAnnotations = resultSet.renderDocument(0)
			documentId = resultSet.documentId(0)
			self.assertGreater(len(textWithAnnotations), 0)
			self.assertIsInstance(textWithAnnotations, unicode)
			text = resultSet.documentText(0)


		textById = self.mimir.renderDocumentById(documentId)
		self.assertGreater(len(text), 0)
		self.assertIsInstance(text, unicode)
		self.assertEqual(text, textById)

	def testIterateResultsSet(self):
		with self.mimir.query("{Hashtag}") as resultSet:
			count = 0
			for result in resultSet:
				count += 1
				self.assertIsInstance(result, MimirResult)
				if count == 10:
					break
			self.assertEqual(count, 10)

if __name__ == '__main__':
    unittest.main()
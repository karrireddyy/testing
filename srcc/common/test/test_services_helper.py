import unittest
from common.services_helper import dictionary_to_query_string

class TestServicesHelper(unittest.TestCase):

	def test_dictionary_to_query_string(self):
		query_string = dictionary_to_query_string({"Name": "temp.txt", "Body":"xyz"})		
		self.assertEquals("body=xyz&name=temp.txt", query_string)

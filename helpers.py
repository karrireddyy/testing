from common.config_manager import discover_service
import requests
from common.collections import OCR_GENERATED_MODELS_COLLECTION
from common.dbadapter import get_list_by_query


def get_page_delimiter():
	return u"~~~~~~~~~~~~~~~~!!!!!!!~~~~~~~~~~~~~~~~"


def gtedit_extraction_to_text(extraction_output):
	"""
	:param extraction_output:
	Using `gtedit extract` puts all lines in the document into the stdout. It gets
	piped into this method by the PDF processor and trainer and returned.
	Extraction_output typically refers to a single page of a single document.
	:return:
	"""
	page_text = ""
	for line in extraction_output.split("\n"):
		text_item = line.split("\t")
		if len(text_item) == 2:
			# The first item is the line's file name, which isn't super important right now.
			page_text += "%s\n" % text_item[1]
	return page_text


def get_indexed_pages_for_file_id(consul_server, file_id):
	ret = discover_service(consul_server, "elastic-search")
	if len(ret) != 0:
		url = "http://%s:%s/%s" % (ret[0]["Address"], ret[0]["ServicePort"], file_id)
		response = requests.get(url)
		return response
	return None


def reindex_body_for_file_id(consul_server, post_body):
	ret = discover_service(consul_server, "elastic-search")
	if len(ret) != 0:
		url = "http://%s:%s" % (ret[0]["Address"], ret[0]["ServicePort"])
		response = requests.post(url, json=post_body)
		return response
	return {}


def get_system_models():
	system_models = get_list_by_query({
		"InProgress": False,
		"CompletedSuccessfully": True,
		"Active": True,
		"Latest": True
	}, OCR_GENERATED_MODELS_COLLECTION)
	return system_models

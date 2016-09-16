from common.config_manager import discover_service
from common.helper import get_consul_server
import urllib
from common.dbadapter import get_logged_in_user_id

import requests
from flask import request
import json
from common.log_writer import *

logger = get_logger("services-helper")

def lookup_es_service():
	return discover_service(get_consul_server(), "elastic-search")

def lookup_user_service():
	ret = discover_service(get_consul_server(), "user")
	service_addr= ret[0]["Address"]
	service_port = ret[0]["ServicePort"]

	return "http://" + service_addr + ":" + str(service_port) + "/create"

def lookup_organization_service():
	ret = discover_service(get_consul_server(), "organization")
	service_addr= ret[0]["Address"]
	service_port = ret[0]["ServicePort"]
	
	return "http://" + service_addr + ":" + str(service_port)

def lookup_organization_link_service():
	ret = discover_service(get_consul_server(), "link")
	service_addr= ret[0]["Address"]
	service_port = ret[0]["ServicePort"]
	
	return "http://" + service_addr + ":" + str(service_port) + "/create"

def lookup_engagement_service(id):
	ret = discover_service(get_consul_server(), "engagement")
	service_addr= ret[0]["Address"]
	service_port = ret[0]["ServicePort"]

	return "http://" + service_addr + ":" + str(service_port)+"/"+id
	
def lookup_project_service(id):
	ret = discover_service(get_consul_server(), "project")
	service_addr= ret[0]["Address"]
	service_port = ret[0]["ServicePort"]

	return "http://" + service_addr + ":" + str(service_port)+"/"+id

def lookup_folder_service(id):
	ret = discover_service(get_consul_server(), "folder")
	service_addr= ret[0]["Address"]
	service_port = ret[0]["ServicePort"]

	return "http://" + service_addr + ":" + str(service_port)+"/"+id

def query_es_service(query_params):
	info(logger, "query_es_service...")

	es_service = lookup_es_service()

	if es_service == None or len(es_service) <= 0:
		return {"error": True, "msg":"elastic search service not available."}, 404

	shost = es_service[0]["Address"]
	sport = str(es_service[0]["ServicePort"])
	s_server = "http://" + shost + ":" + sport + "/?"+dictionary_to_query_string(query_params)

	info(logger, s_server)

	r = requests.get(s_server)

	return r.json(), 200

def dictionary_to_query_string(dictionary):	
	return urllib.urlencode(dict((k.lower(), v) for k, v in dictionary.iteritems()))

def lookup_tag_service():
	ret = discover_service(get_consul_server(), "tag")
	service_addr= ret[0]["Address"]
	service_port = ret[0]["ServicePort"]

	return "http://" + service_addr + ":" + str(service_port)

def get_service_header():
	request_headers = {}
	for k, v in request.headers.items():
		request_headers[k] = v

	final_header = {"User-Agent": request_headers["User-Agent"]}

	if "Content-Type" in request_headers:
		final_header["Content-Type"] = request_headers["Content-Type"]
	else:
		final_header["Content-Type"] = "application/json"

	final_header["User-Id"] = get_logged_in_user_id()	

	return final_header

def update_org_tags(tags, personal_tag):
	tag_service = lookup_tag_service()

	if tag_service == None or len(tag_service) <= 0:
		return {"error": True, "msg":"tag service not available."}, 404	
	
	final_header = get_service_header()
	final_header["Content-Type"] = "application/json"
	requests.post(tag_service, data=json.dumps({"Tags": tags, "PersonalTag": personal_tag}), headers=final_header)

def lookup_pdf_converter_service(from_ext):
	ret = discover_service(get_consul_server(), "unoconv")
	service_addr= ret[0]["Address"]
	service_port = ret[0]["ServicePort"]

	return "http://" + service_addr + ":" + str(service_port)+"/unoconv/"+from_ext+"/pdf"

def lookup_file_service():
	ret = discover_service(get_consul_server(), "file")
	service_addr= ret[0]["Address"]
	service_port = ret[0]["ServicePort"]

	return "http://" + service_addr + ":" + str(service_port)
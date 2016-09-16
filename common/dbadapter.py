from pymongo import MongoClient
import os
import config_manager
import helper
import numbers
import collections
from flask import request

from common.log_writer import *

try:
	consul_server = os.environ["CONSUL_SERVER"]
except Exception, ex:
	consul_server = "127.0.0.1"
	print "No consul server available."

local_mode = False

try:
	is_local = os.environ["IS_LOCAL"]
	if is_local == "1":
		local_mode = True
except Exception, ex:
	local_mode = False

logger = get_logger("dbadapter")

def get_db_conn(collection_name):
	if consul_server == "127.0.0.1" and local_mode == False:
		return None
	else:
		if local_mode == False:
			ret = config_manager.discover_service(consul_server, "mongo")
			if ret.__class__.__name__ not in ('list', 'touple'):
				return None
			if len(ret) == 0:
				return None

			mongodb_host = ret[0]["Address"]
			mongodb_port = int(ret[0]["ServicePort"])
			config_arr = config_manager.get_config(consul_server, ["mongodb_name"])
		else:
			mongodb_host = "127.0.0.1"
			mongodb_port = 27017
			config_arr = []
			config_arr.append({"mongodb_name": "mydb"})
		try:
			client = MongoClient(mongodb_host, mongodb_port,
			                     serverSelectionTimeoutMS=1500)
			db_us = client[config_arr[0]["mongodb_name"]]
			collection = db_us[collection_name]
			return collection
		except Exception, ex:
			print ex
			return None


def create(item, changed_by_id, changed_on_utc, collection_name, version=True, allow_active=True):
	item["EntityID"] = ""
	item["Version"] = "00000000-0000-0000-0000-000000000000"
	return save(item, changed_by_id, changed_on_utc, collection_name, version=version, allow_active=allow_active)


def save(item, changed_by_id, changed_on_utc, collection_name, version=True, allow_active=True):
	# if not helper.validate_uuid(changed_by_id):
	# 	return None
	if not helper.validate_datetime(changed_on_utc, ""):
		return None
	if "EntityID" not in item:
		return None
	if "Version" not in item:
		return None

	try:
		myitem = item
		collection = get_db_conn(collection_name)
		if version == True:
			if myitem["EntityID"] == "":
				myitem["EntityID"] = helper.get_uuid()
			else:
				collection.update_many({"EntityID": myitem["EntityID"]}, {"$set": {"Latest": False}})
			myitem["PreviousVersion"] = myitem["Version"]
			myitem["Version"] = helper.get_uuid()
			if allow_active:
				myitem["Active"] = True
			myitem["Latest"] = True
			# if "Active" not in myitem:
			# 	myitem["Active"] = True
			# if "Latest" not in myitem:
			# 	myitem["Latest"] = True
			myitem["ChangedByID"] = changed_by_id
			myitem["ChangedOn"] = str(changed_on_utc)

			if collection == None:
				debug(logger, "Collection none, not inserting a new record...")
				return None
			debug(logger, "inserting a new record...")
			res = collection.insert_one(myitem)
			row = collection.find_one({"EntityID": myitem["EntityID"], "Active": True, "Latest": True})

			row.pop("_id")

			return row
		else:
			collection = get_db_conn(collection_name)
			if collection is None:
				return None
			collection.update({"EntityID": myitem["EntityID"], "Latest": True}, {"$set": item})
			row = collection.find_one({"EntityID": myitem["EntityID"], "Active": True, "Latest": True})
			row.pop("_id")
			return row
	except Exception, ex:
		print str(ex)
		return None


def get_by_id(entity_id, collection_name, version_id=""):
	# if not helper.validate_uuid(entity_id):
	# 	return None
	if version_id != "":
		return None

	try:
		collection = get_db_conn(collection_name)
		if version_id == "":
			row = collection.find_one({"EntityID": entity_id, "Latest": True})
			row.pop("_id")
			return row
		else:
			row = collection.find_one({"EntityID": entity_id, "Version": version_id})
			row.pop("_id")
			return row
	except Exception, ex:
		print str(ex)
		return None


def delete(entity_id, changed_by_id, changed_on_utc, collection_name, version=True):
	# if not helper.validate_uuid(entity_id):
	# 	return False

	try:
		if version == True:
			item = get_by_id(entity_id, collection_name)
			# item.pop("_id")
			item["Active"] = False
			x = save(item, changed_by_id, changed_on_utc, collection_name, True, False)
			if x == None:  # After delete there is no such object like this
				return True
			else:
				return False
		else:
			if not helper.validate_uuid(changed_by_id):
				return False
			if not helper.validate_datetime(changed_on_utc, ""):
				return False
			collection = get_db_conn(collection_name)
			res = collection.update_one({"EntityID": entity_id, "Latest": True}, {"$set": {"Active": False,
			                                                                               "ChangedByID": changed_by_id,
			                                                                               "ChangedOn": str(
				                                                                               changed_on_utc)}})
			if res.matched_count == res.modified_count:
				return True
			else:
				return False
	except Exception, ex:
		print ex
		return False


def get_list(collection_name, limit=0, skip=0, active=True):
	if not isinstance(limit, numbers.Integral):
		return None
	if not isinstance(skip, numbers.Integral):
		return None

	if active:
		query = {"Active": True, "Latest": True}
	else:
		query = {"Active": False}

	return get_list_by_query(query, collection_name, limit, skip)


def delete_many_by_query(query, changed_by_id, changed_on_utc, collection_name):
	try:
		collection = get_db_conn(collection_name)
		res = collection.update_many(query, {
			"$set": {
				"Active": False,
				"ChangedByID": changed_by_id,
				"ChangedOn": str(changed_on_utc)
			}
		})
		return res
	except Exception, ex:
		print str(ex)
		return None


def get_list_by_query(query, collection_name, limit=0, skip=0):
	if not isinstance(limit, numbers.Integral):
		return None
	if not isinstance(skip, numbers.Integral):
		return None

	try:
		collection = get_db_conn(collection_name)
		res = collection.find(query, None, skip, limit)
		return res
	except Exception, ex:
		print str(ex)
		return None


def get_logged_in_user_id():
	request_headers = {}
	for k, v in request.headers.items():
		request_headers[k] = v

	return request.headers["User-ID"]


def get_logged_in_user_organization():
	user_id = get_logged_in_user_id()
	last_viewed_organization = get_list_by_query({"User": user_id, "Latest": True},
	                                             collections.LAST_VIEWED_ORGANIZATION).sort("ChangedOn", -1)

	org = ""
	x = list(last_viewed_organization)
	if len(x) == 0:
		lvo = ""
	else:
		lvo = x[0]["Organization"]
		link = None
	if lvo != "":
		org = get_by_id(lvo, collections.ORGANIZATION_COLLECTION)

	return org

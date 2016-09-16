import requests
import json

import logging
import sys
from random import randint

roles = {"OA": "Organization Admin", "OM": "Manager", "U": "User", "A": "Analyst", "AS": "Associate",
         "SA": "System Admin"}


def test(value):
	print value


def get_key_value(server, key):
	if server == "127.0.0.1":
		return None
	else:
		consul_url = "http://" + server + ":8500/v1/kv/" + key
		r = requests.get(consul_url)
		if r.status_code == 404:
			return None
		else:
			data = r.json()
			try:
				return data[0]["Value"].decode("base64")
			except:
				return None


def get_config(server, keys):
	if server == "127.0.0.1":
		return []
	else:
		res = []
		res_dict = {}
		for key in keys:
			consul_url = "http://" + server + ":8500/v1/kv/" + key
			r = requests.get(consul_url)
			if r.status_code == 404:
				res_dict[key] = ""
			else:
				data = r.json()
				try:
					res_dict[key] = data[0]["Value"].decode("base64")
				except:
					return []
		res.append(res_dict)
		##TODO check if keys have value, raise error otherwise
		# if not valid_config(res):			
		# 	raise ValueError('Can not get config values')

		return res


def valid_config(config_arr):
	if len(config_arr) == 0 or config_arr[0]["secret_key"] == "" or config_arr[0]["secret_key_id"] == "" \
			or config_arr[0]["bucket_name"] == "":
		return False
	else:
		return True


def discover_service(server, service):
	try:
		consul_url = "http://" + server + ":8500/v1/catalog/service/" + service
		r = requests.get(consul_url)
		m = r.json()
		selected = randint(0, (len(m) - 1))
		ret = []
		ret.append(m[selected])
		return ret
	except:
		return []


def get_roles(role_code):
	if role_code not in roles:
		return ""
	else:
		return roles[role_code]


def is_role(role_name):
	for role, role_n in roles.items():
		if role_n == role_name:
			return True
	return False


def get_logging_handler(consul_server):
	config_arr = get_config(consul_server, ["log_level"])
	formatter = logging.Formatter(
		"[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s")
	handler = logging.StreamHandler(sys.stdout)

	if len(config_arr) > 0 and config_arr[0] is not None and config_arr[0]["log_level"] is not None:
		log_level = config_arr[0]["log_level"]
	else:
		log_level = "INFO"

	if log_level == "DEBUG":
		handler.setLevel(logging.DEBUG)

	if log_level == "INFO":
		handler.setLevel(logging.INFO)

	if log_level == "WARNING":
		handler.setLevel(logging.WARNING)

	if log_level == "ERROR":
		handler.setLevel(logging.ERROR)

	if log_level == "CRITICAL":
		handler.setLevel(logging.CRITICAL)

	handler.setFormatter(formatter)

	return handler


def is_admin(role_name):
	for role, role_n in roles.items():
		if role_name == role_n and role == "OA":
			return True
	return False


def is_manager(role_name):
	for role, role_n in roles.items():
		if role_name == role_n and role == "OM":
			return True
	return False


def is_user(role_name):
	for role, role_n in roles.items():
		if role_name == role_n and role == "U":
			return True
	return False


def is_analyst(role_name):
	for role, role_n in roles.items():
		if role_name == role_n and role == "A":
			return True
	return False


def has_org_role(role_name):

	return is_role(role_name)

def is_local_or_dev_environment(consul_server):
	local_or_dev_environment = False
	config_arr = get_config(consul_server, ["environment"])

	if len(config_arr) > 0 and config_arr[0] is not None and config_arr[0]["environment"] is not None:
		if config_arr[0]["environment"] == "local" or config_arr[0]["environment"] == "dev":
			local_or_dev_environment = True

	return local_or_dev_environment

import uuid
from uuid import UUID
import arrow
import hashlib
import base64
import random
import os
import errno
import config_manager
import requests


def mkdir_p(path):
	try:
		os.makedirs(path)
	except OSError as exc:  # Python >2.5
		if exc.errno == errno.EEXIST and os.path.isdir(path):
			pass
		else:
			raise


def get_consul_server():
	try:
		consul_server = os.environ["CONSUL_SERVER"]
	except Exception, ex:
		consul_server = "127.0.0.1"
		print "No consul server available."
	return consul_server


def get_uuid(return_type="hex"):
	m = uuid.uuid4()
	if return_type == "hex":
		return str(m.hex)
	else:
		return str(m)


def validate_uuid(uuid_string, comp_type="hex"):
	try:
		val = UUID(uuid_string, version=4)
	except ValueError, ex:
		return False

	if comp_type == "hex":
		return str(val.hex) == uuid_string
	else:
		return str(val.hex) == uuid_string.replace('-', '')


def validate_datetime(datetime_str, validation_format='YYYY-MM-DD HH:mm:ss'):
	if validation_format == "":
		try:
			arrow.get(datetime_str)
			return True
		except Exception, ex:
			return False
	else:
		try:
			arrow.get(datetime_str, validation_format)
			return True
		except Exception, ex:
			return False


def get_void_uuid():
	return "00000000-0000-0000-0000-000000000000"


def get_indexed_pages_for_file_id(consul_server, file_id):
	ret = config_manager.discover_service(consul_server, "elastic-search")
	if len(ret) != 0:
		url = "http://%s:%s/%s" % (ret[0]["Address"], ret[0]["ServicePort"], file_id)
		response = requests.get(url)
		return response
	return None


def get_encrypted_str(str, method="sha256", hexdigest=True):
	try:
		if method == "sha224":
			if hexdigest:
				return hashlib.sha224(str).hexdigest()
			else:
				return hashlib.sha224(str).digest()
		if method == "sha256":
			if hexdigest:
				return hashlib.sha256(str).hexdigest()
			else:
				return hashlib.sha256(str).hexdigest()
	except Exception, ex:
		return None


def base64_encode(str):
	try:
		return base64.b64encode(str)
	except Exception, ex:
		return None


def base64_decode(str):
	try:
		return base64.b64decode(str)
	except Exception, ex:
		return None


def generate_random_pwd():
	alphabet = "abcdefghijklmnopqrstuvwxyz"
	upperalphabet = alphabet.upper()
	pw_len = 8
	pwlist = []

	for i in range(pw_len // 3):
		pwlist.append(alphabet[random.randrange(len(alphabet))])
		pwlist.append(upperalphabet[random.randrange(len(upperalphabet))])
		pwlist.append(str(random.randrange(10)))
	for i in range(pw_len - len(pwlist)):
		pwlist.append(alphabet[random.randrange(len(alphabet))])

	random.shuffle(pwlist)
	pwstring = "".join(pwlist)

	return pwstring


def is_cedarwood_user(user, local_or_dev_environment):
	is_cedarwood_user_flag = False
	if user["Email"] == "jaygrieves@gmail.com":
		is_cedarwood_user_flag = True
	elif local_or_dev_environment == True and is_application_developer(user["Email"]):
		is_cedarwood_user_flag = True
	else:
		email_domain = user["Email"].split("@")[1]
		if email_domain == "cedarwoodconsulting.com":
			is_cedarwood_user_flag = True

	return is_cedarwood_user_flag


def is_application_developer(email):
	developer_emails = ["njabisetti@aksharasolutions.com",
	                    "srujana@aksharasolutions.com",
	                    "avelagandula@aksharasolutions.com",
	                    "chris.cohoat@gmail.com"]

	return email in developer_emails

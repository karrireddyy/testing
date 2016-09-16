from common.log_writer import *
from common.config_manager import discover_service
import requests
from boto.s3.key import Key
import uuid
import os
import shutil

default_logger = get_logger("content-extraction-generic-logger")


class BaseExtractionProcessor(object):
	def __init__(self, consul_server, bucket, file_name, extension=None, logger=None):
		"""
		The extension can be passed in and override a simple extension
		check based on file_name, if a mimetype has been guessed upstream
		or a file has been detected as one file type masquerading as another.
		"""
		if not extension:
			extension = file_name.split(".")[-1]
		self.consul_server = consul_server
		self.bucket = bucket
		self.file_name = file_name
		self.extension = extension
		self.content = ""
		self.metadata = {}
		tika_service_components = discover_service(self.consul_server, "tika-server")
		if len(tika_service_components) != 0:
			self.tikaServerEndpoint = "http://%s:%s" % (tika_service_components[0]["Address"],
			                                            tika_service_components[0]["ServicePort"])
		else:
			raise Exception("Unable to discover the `tika-server`")
		self.logger = logger if logger else default_logger
		self.downloaded_file = None
		self.files_to_cleanup = []

	def download_file(self):
		s3_key = Key(self.bucket)
		s3_key.key = self.file_name
		f = open('/tmp/file1.txt','w')
                f.write(s3_key.key)
                f.close()


		file_key = self.bucket.get_key(self.file_name)


		info(self.logger, "File key: %s" % file_key.metadata)
		self.metadata = dict(file_key.metadata)

		output_filename = "%s.%s" % (uuid.uuid4(), self.extension)
		f = open('/tmp/file.txt','w')
		f.write(output_filename) 
		f.close()
		resulting_filename = os.path.join("/opt/content_extraction", output_filename)
		info(self.logger, "Saving file to %s" % resulting_filename)
		try:
			s3_key.get_contents_to_filename(resulting_filename)
		except:
			error(self.logger, "Unable to get s3_key to local file.")
			return None
		if os.path.isfile(resulting_filename):
			info(self.logger, "Successfully transferred file from S3 to local disk.")
			self.downloaded_file = resulting_filename
			self.files_to_cleanup.append(self.downloaded_file)
			return resulting_filename
		else:
			error(self.logger, "Operations are completed, but no file on disk.")
			return None

	def download_system_model(self, model_s3_key, model_destination_path):
		# TODO: Get the base directory of the destination path and remove all old models
		# that are not the default English model.
		s3_key = Key(self.bucket)
		s3_key.key = model_s3_key
		s3_key.get_contents_to_filename(model_destination_path)
		return os.path.isfile(model_destination_path)

	def index_content(self):
		debug(self.logger, "Sending data to ElasticSearch")
		try:
			ret = discover_service(self.consul_server, "elastic-search")
			if len(ret) != 0:
				send_data = {}
				for key, val in self.metadata.items():
					send_data[key] = val
				send_data["body_txt"] = self.content

				post_server = "http://" + ret[0]["Address"] + ":" + str(ret[0]["ServicePort"])
				r = requests.post(post_server, json=send_data)
				if r.status_code == 201:
					return True
				else:
					error(self.logger, "Error sending data to ElasticSearch: %s" % r.status_code)
					return False
		except Exception, ex:
			error(self.logger, "Error sending data to ElasticSearch: %s" % ex)
			return False

	def cleanup(self):
		print("NOT CLEANED")
		
		'''for file_to_cleanup in self.files_to_cleanup:
			if os.path.isfile(file_to_cleanup):
				os.remove(file_to_cleanup)
			elif os.path.isdir(file_to_cleanup):
				shutil.rmtree(file_to_cleanup)'''

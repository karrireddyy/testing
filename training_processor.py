from common.content_extraction.base_processor import BaseExtractionProcessor
from common.log_writer import *
import os
import traceback
import glob
from common.helper import mkdir_p, get_void_uuid
import arrow
from common.collections import OCR_GROUND_TRUTH_COLLECTION, OCR_TRAINING_COLLECTION, OCR_GENERATED_MODELS_COLLECTION
from pdf_to_images import PdfToImages
import subprocess
from common.dbadapter import get_by_id, get_list_by_query, delete_many_by_query, create, save
import uuid
import codecs
from helpers import gtedit_extraction_to_text, get_page_delimiter, get_indexed_pages_for_file_id, \
	reindex_body_for_file_id, get_system_models
import json
from common.config_manager import get_config
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import shutil
from common.queue import publish_command_to_exchange
import glob
from common.pusher_util import *
import common.pusher_events as pusher_events

# A good default is 1000 lines. The fewer the lines the faster the training and more limited the model
RNN_LINES_TO_TRAIN = 1000


class TrainingProcessor(object):
	def __init__(self, logger, consul_server, ground_truth_entity_id, updated_data, edited_images, original_file_id):
		self.logger = logger
		self.consul_server = consul_server
		self.ground_truth_entity_id = ground_truth_entity_id
		self.updated_data = updated_data
		self.edited_images = edited_images
		self.extraction_output = None
		self.original_file_id=original_file_id

		# The following environment variable must be set for the subprocess commands to work properly
		os.environ["PYTHONIOENCODING"] = "utf8"

		# Setup s3 connection information
		s3_config = get_config(self.consul_server, ["secret_key", "secret_key_id", "bucket_name"])
		self.s3_connection = S3Connection(s3_config[0]["secret_key"], s3_config[0]["secret_key_id"])
		self.s3_bucket_name = s3_config[0]["bucket_name"]
		self.s3_bucket = self.s3_connection.get_bucket(self.s3_bucket_name)

		# Create a directory to extract text and images from
		self.working_directory = "/opt/content_extraction/%s" % uuid.uuid4()
		mkdir_p(self.working_directory)

	def extract_content(self):
		ground_truth_item = get_by_id(self.ground_truth_entity_id, OCR_GROUND_TRUTH_COLLECTION)
		uploaded_file_id = ground_truth_item["FileID"]
		ground_truth_page = int(
			ground_truth_item["sub_directory"].split("__")[1])  # sub_directory looks like page__%04d

		existing_training_items = get_list_by_query({
			"FileID": self.ground_truth_entity_id,
			"Active": True,
			"Latest": True
		}, OCR_TRAINING_COLLECTION)

		# Need the file's metadata in order to properly index in elasticsearch. This will be the
		# base JSON body that we post to the search service for this file.

		file_post_data = self.get_file_metadata(ground_truth_item["FileName"])

		# Next, we save the revised HTML file into the working directory

		relative_filename = "correction.html"
		resulting_filename = os.path.join(self.working_directory, relative_filename)
		with codecs.open(resulting_filename, "w", "utf-8") as text_file:
			text_file.write(self.updated_data)

		# After saving the file into the working directory we convert it to pngs and text files

		extraction_steps = " ".join(
			["ocropus-gtedit", "extract", "-O", relative_filename])
		print "Performing extraction with `%s` in directory %s" % (extraction_steps, self.working_directory)
		ground_truth_process = subprocess.Popen(extraction_steps,
		                                        cwd=self.working_directory, close_fds=True,
		                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
		                                        universal_newlines=True, shell=True)
		self.extraction_output, extraction_errors = ground_truth_process.communicate()
		if extraction_errors:
			info(self.logger, "Have extraction errors. Proceeding all the same: %s" % extraction_errors)

		if self.extraction_output:

			revision_number = (existing_training_items.count() + 1)

			existing_pages_response = get_indexed_pages_for_file_id(self.consul_server, uploaded_file_id)
			if not existing_pages_response:
				error(self.logger, "No pages have been indexed for this file. Something has gone wrong.")
				return

			# The indexed document body is delimited by a special set of characters that are unique to this service.
			# Since we are extracting only a single page in this message we need to retrieve all other pages and
			# re-index the whole body with the new page in place of the old page.

			existing_pages = existing_pages_response.json()["data"]["body_txt"].split(get_page_delimiter())

			existing_pages[ground_truth_page - 1] = gtedit_extraction_to_text(
				self.extraction_output)  # TODO: Check for change in pages?
			file_post_data["body_txt"] = get_page_delimiter().join(existing_pages)
			reindex_body_for_file_id(self.consul_server, file_post_data)

			# Sync the new revision in S3 with all extracted text/images/corrected HTML

			extraction_s3_key = "%s/revision_%04d" % (ground_truth_item["s3_key"], revision_number)
			extraction_s3_bucket = ground_truth_item["s3_bucket"]
			print extraction_s3_bucket
			s3_sync_steps = ["aws", "s3", "sync", ".", "s3://%s/%s" % (extraction_s3_bucket, extraction_s3_key)]
			
			extraction_s3_keyrevision = "%s" % (ground_truth_item["s3_key"])
		  	os.system("chmod +777 /tmp")	
			rnumber=str(revision_number)
			

			f11 = open("/opt/revision.txt", 'w')
			f11.write(rnumber)
			f11.close()
                   	


                      
	

			revision_numbercmd = "aws s3 cp /opt/revision.txt s3://%s/%s/revision.txt"%(extraction_s3_bucket, extraction_s3_keyrevision)
			os.system(revision_numbercmd)
			
			extraction_s3_key1 = "%s/pdfname.txt" % (ground_truth_item["s3_key"])
			os.system("aws s3 cp s3://%s/%s /opt/pdfname.txt"%(extraction_s3_bucket, extraction_s3_key1))
			pdfname = open("/opt/pdfname.txt").read()
			os.system("aws s3 cp s3://%s/%s /opt/" %(extraction_s3_bucket, pdfname))
			print "Uploading to S3 with `%s`" % " ".join(s3_sync_steps)
			s3_sync_process = subprocess.Popen(s3_sync_steps,
			                                   cwd=self.working_directory, close_fds=True,
			                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
			                                   universal_newlines=True)
			s3_sync_output, s3_sync_error = s3_sync_process.communicate()
			os.system("aws s3 cp s3://%s/%s/correction.html /opt/"  % (extraction_s3_bucket, extraction_s3_key))
			os.system("python /app/replace/replace.py")
			
			path_totalpages = "%s" % (ground_truth_item["s3_key"])
			path_totalpages = path_totalpages[:-1]
			path_totalpages = path_totalpages + '1'
			totalpages_cmd = "aws s3 cp s3://%s/%s/totalpages.txt /opt/totalpages.txt"% (extraction_s3_bucket, path_totalpages)
			os.system(totalpages_cmd)
			totalpages = int(open("/opt/totalpages.txt").read())
			print totalpages

			grab_changespath = "%s" % (ground_truth_item["s3_key"])
			grab_changespath = grab_changespath[:-4]
			for i in range(1,totalpages+1):
				i1 = str(i).zfill(4)
				get_revision = "aws s3 cp  s3://%s/%s%s/revision.txt /tmp/revision.txt"%(extraction_s3_bucket, grab_changespath,i1)
				os.system(get_revision)
				rev_number = int(open("/tmp/revision.txt").read())
	
				if(rev_number==9989):
					get_hocr = "aws s3 cp s3://%s/%s%s/correction_original.html /tmp/test2/some_%s.hocr"%(extraction_s3_bucket, grab_changespath,i1,i-1)
					print get_hocr
					os.system(get_hocr)
				else:
					rev_number = str(rev_number).zfill(4)
					get_hocr1 = "aws s3 cp s3://%s/%s%s/revision_%s/correction.html /tmp/test2/some_%s.hocr"%(extraction_s3_bucket, grab_changespath,i1,rev_number,i-1)
					os.system(get_hocr1)
			print rev_number

			
			os.system("python /app/laterreplace/replace.py")
			command7 = "/app/laterhocr/test.sh %s" %(pdfname)
			os.system(command7)
			os.system("aws s3 cp /tmp/test2/some_0.pdf s3://%s/%s" %(extraction_s3_bucket, pdfname))
			print "Done with S3 sync."
			if s3_sync_error:
				print "S3 Errors: %s" % s3_sync_error

			# Add the revision to mongo so that a front-end user knows about the update

			create({
				"FileID": self.ground_truth_entity_id,
				"RevisionNumber": revision_number,
				"s3_key": extraction_s3_key,
				"s3_bucket": extraction_s3_bucket
			}, get_void_uuid(), arrow.utcnow(), OCR_TRAINING_COLLECTION)

			send_pusher_doc_message(pusher_events.DOCUMENT_EVENT, {"Name": "document_changed", "DocumentID": self.original_file_id, "Date": str(arrow.utcnow())})

			# Finally, take the latest revision and only copy the images we care about

			for edited_image in self.edited_images.keys():
				for file_to_copy in [".bin.png", ".gt.txt", ]:
					existing_ground_truth_text_key = Key(self.s3_connection.get_bucket(self.s3_bucket_name))
					resulting_filename = edited_image.replace(".bin.png", file_to_copy)
					existing_ground_truth_text_key.key = "%s/%s" % (extraction_s3_key, resulting_filename)
					existing_ground_truth_text_key.copy(existing_ground_truth_text_key.bucket.name,
					                                    "%s/ground_truth/%s" % (ground_truth_item["s3_key"],
					                                                            resulting_filename))

			return True

		return False

	def begin_training(self):
		# Trigger training. For now only a single model generation can be ran at a time because it's so expensive
		in_progress_models = get_list_by_query({
			"InProgress": True,
			"Active": True,
			"Latest": True
		}, OCR_GENERATED_MODELS_COLLECTION)
		if not self.extraction_output:
			warn(self.logger, "Extraction output is empty...skipping training.")
			return
		elif in_progress_models.count() > 0:
			# TODO: Check the ChangedOn property in the latest in progress model and make sure
			# it isn't hung up and in need of cancelling. A manual command for removing in progress items:
			# db.ocr_generated_models.remove({"InProgress": true, "Active": true, "Latest": true})
			warn(self.logger, "Training in progress...skipping training trigger.")
			return

		training_item = create({
			"InProgress": True
		}, get_void_uuid(), arrow.utcnow(), OCR_GENERATED_MODELS_COLLECTION)

		# Models are currently on a system wide basis
		system_models = get_system_models()
		system_model_count = system_models.count()
		new_model_name = "model-%04d" % (system_model_count + 1)

		if system_model_count == 0:
			model_to_load = "en-default.pyrnn.gz"
			lines_before_stopping = 100000 + RNN_LINES_TO_TRAIN
		else:
			try:
				latest_system_model = system_models[system_model_count - 1]
				lines_before_stopping = latest_system_model["LineStartCounter"] + RNN_LINES_TO_TRAIN
				downloaded_model_name = "model_latest.gz"
				resulting_filename = os.path.join(self.working_directory, downloaded_model_name)
				s3_key_to_download = Key(self.s3_bucket)
				s3_key_to_download.key = "%s/model_latest.gz" % latest_system_model["s3_key"].split("/")[0]
				s3_key_to_download.get_contents_to_filename(resulting_filename)
				if os.path.isfile(resulting_filename):
					info(self.logger, "Successfully downloaded system model %s" % downloaded_model_name)
					model_to_load = downloaded_model_name
				else:
					model_to_load = None
			except:
				info(self.logger, "Unable to download model_latest.gz for training. Defaulting to en-default.")
				model_to_load = "en-default.pyrnn.gz"
				lines_before_stopping = 100000 + RNN_LINES_TO_TRAIN

		if not model_to_load:

			training_item["CompletedSuccessfully"] = False

		else:

			training_steps = " ".join(["ocropus-rtrain", "--load", model_to_load, "-o", new_model_name,
			                           "-F", "%s" % RNN_LINES_TO_TRAIN, "-N", "%s" % lines_before_stopping,
			                           "*.bin.png"])
			info(self.logger, "Performing training with `%s`. Likely will take a while!" % training_steps)
			training_process = subprocess.Popen(training_steps,
			                                    cwd=self.working_directory, close_fds=True,
			                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
			                                    universal_newlines=True, shell=True)
			training_output, training_errors = training_process.communicate()
			info(self.logger, "Training has been completed successfully!")
			if training_errors:
				info(self.logger, "Have training errors. Proceeding all the same: %s" % training_errors)
			else:
				info(self.logger, "Successfully trained a new model named %s" % new_model_name)

			# We are using glob because the output filename has an extra unknown number of characters tacked on.
			versioned_model_name = glob.glob("%s/%s*" % (self.working_directory, new_model_name))[0].split("/")[-1]
			latest_model_name = "model_latest.gz"

			versioned_s3_key = "ocr_generated_models/%s" % versioned_model_name
			latest_s3_key = "ocr_generated_models/%s" % latest_model_name

			# First, copy the local versioned model to S3 for permanent resting
			s3_sync_steps = ["aws", "s3", "cp", versioned_model_name, "s3://%s/%s" % (self.s3_bucket_name, versioned_s3_key)]
			info(self.logger, "Uploading versioned training to S3 with `%s`" % " ".join(s3_sync_steps))
			s3_copy_process = subprocess.Popen(s3_sync_steps,
			                                   cwd=self.working_directory, close_fds=True,
			                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
			                                   universal_newlines=True)
			s3_copy_output, s3_copy_error = s3_copy_process.communicate()

			print "Done with S3 copy."
			if s3_copy_error:
				error(self.logger, "S3 Errors: %s" % s3_copy_error)
			else:
				info(self.logger, "Finished with S3 upload.")

			# Second, overwrite the model_latest.gz file by copying in-place in S3
			s3_latest_sync_steps = ["aws", "s3", "cp",
			                        "s3://%s/%s" % (self.s3_bucket_name, versioned_s3_key),
			                        "s3://%s/%s" % (self.s3_bucket_name, latest_s3_key)]
			info(self.logger, "Copying latest training in S3 with `%s`" % " ".join(s3_latest_sync_steps))
			s3_copy_process = subprocess.Popen(s3_latest_sync_steps,
			                                   cwd=self.working_directory, close_fds=True,
			                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
			                                   universal_newlines=True)
			s3_latest_copy_output, s3_latest_copy_error = s3_copy_process.communicate()

			print "Done with S3 copy."
			if s3_latest_copy_error:
				error(self.logger, "S3 Errors: %s" % s3_copy_error)
			else:
				info(self.logger, "Finished with S3 copy.")

			# Move the file into the local ocropus shared directory so that future predictions don't have
			# to redownload before using this new model
			shutil.move("%s/%s" % (self.working_directory, versioned_model_name),
			            "/usr/local/share/ocropus/%s" % latest_model_name)

			publish_command_to_exchange("content-extraction-pdf-new-model", json.dumps({
				"s3_key": latest_s3_key,
				"s3_bucket": self.s3_bucket_name
			}))

			training_item["s3_key"] = versioned_s3_key
			training_item["Name"] = new_model_name
			training_item["CompletedSuccessfully"] = True
			# Following lines extracts the finishing line count ... for example:
			#   "ocr_generated_models/model-0001-00100010.pyrnn.gz" results in 100010
			training_item["LineStartCounter"] = int(versioned_s3_key.split("/")[-1].split(".")[0].split("-")[-1])

		training_item["InProgress"] = False
		save(training_item, get_void_uuid(), arrow.utcnow(), OCR_GENERATED_MODELS_COLLECTION)

	def cleanup(self):
		try:
			shutil.rmtree(self.working_directory)
		except:
			info(self.logger, "Cleanup of directory %s failed. May need to manually cleanup." % self.working_directory)

	def get_file_metadata(self, file_name):
		s3_key = Key(self.s3_bucket)
		s3_key.key = file_name
		file_key = self.s3_bucket.get_key(file_name)
		return dict(file_key.metadata)

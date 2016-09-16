from common.content_extraction.base_processor import BaseExtractionProcessor
from common.log_writer import *
import os
import traceback
import glob
from common.helper import mkdir_p, get_void_uuid
from common.dbadapter import create, get_by_id, save
import arrow
from common.collections import OCR_GROUND_TRUTH_COLLECTION, OCR_INITIAL_WORK_COLLECTION, FILE_COLLECTION
from pdf_to_images import PdfToImages
import subprocess
from helpers import gtedit_extraction_to_text, get_page_delimiter, get_system_models
from tika import parser
from common.pusher_util import *
import common.pusher_events as pusher_events


class PdfExtractionProcessor(BaseExtractionProcessor):
	def __init__(self, consul_server, bucket, file_name, extension=None, logger=None, worker_count=2):
		self.page_delimiter = get_page_delimiter()
		self.worker_count = str(worker_count)

		super(PdfExtractionProcessor, self).__init__(consul_server, bucket, file_name, extension=extension,
		                                             logger=logger)

		# The following environment variable must be set for the subprocess commands to work properly
		os.environ["PYTHONIOENCODING"] = "utf8"

		system_models = get_system_models()
		system_model_count = system_models.count()

		if system_model_count == 0:
			self.model_to_load = "en-default.pyrnn.gz"
		else:
			# TODO: Verify that this model exists in /usr/local/share/ocropus. If it doesn't,
			# download the model from the ocr_generated_models/X key (s3_key 2 lines up) and sync
			# up the /usr/local/share/ocropus share directory.
			system_model_s3_key = "%s/model_latest.gz" % system_models[system_model_count - 1]["s3_key"].split("/")[0]
			self.model_to_load = system_model_s3_key.split("/")[-1]
			# Check to see if this model exists in the local directory. If it doesn't, download it and cleanup
			# older models from the directory (to avoid ->inf hard drive usage)
			local_model_location = os.path.join("/usr/local/share/ocropus", self.model_to_load)
			if not os.path.isfile(local_model_location):
				try:
					self.download_system_model(system_model_s3_key, local_model_location)
				except:
					# Download failures fall back to using the default system model. Most likely scenario
					# is if there are models in mongo but model_latest does not yet exist in S3 (or has been deleted)
					info(self.logger, "Unable to download model_latest.gz for extraction. Defaulting to en-default.")
					self.model_to_load = "en-default.pyrnn.gz"

	def parse_content(self, model_work_bucket, file_entity_id):

		def send_document_changed_pusher_event(doc_id):
			send_pusher_doc_message(pusher_events.DOCUMENT_EVENT, {"Name": "document_changed", "DocumentID": doc_id, "Date": str(arrow.utcnow())})

		def send_status_update(update):
			status_update = {
				"FileID": file_entity_id,
				"FileName": self.file_name,
				"message": update
			}
			create(status_update, get_void_uuid(), arrow.utcnow(), OCR_INITIAL_WORK_COLLECTION)
			send_document_changed_pusher_event(file_entity_id)

		def send_ground_truth_update(sub_directory, s3_bucket, s3_key):
			ground_truth_data = {
				"FileID": file_entity_id,
				"FileName": self.file_name,
				"sub_directory": sub_directory,
				"s3_bucket": s3_bucket,
				"s3_key": s3_key
			}
			create(ground_truth_data, get_void_uuid(), arrow.utcnow(), OCR_GROUND_TRUTH_COLLECTION)

		def update_ocr_flag(flag=False):
			file_entry = get_by_id(file_entity_id, FILE_COLLECTION)
			file_entry["OCR"] = flag
			save(file_entry, get_void_uuid(), arrow.utcnow(), FILE_COLLECTION)

		if self.download_file():

			parsed = parser.from_file(self.downloaded_file, serverEndpoint=self.tikaServerEndpoint)
			parsed_content = parsed.get("content", "")
			if parsed_content and len(parsed_content) > 20000:
				self.content = parsed_content.strip()
				info(self.logger, "Successfully parsed text PDF for %s. Skipping OCR." % self.downloaded_file)
				update_ocr_flag(flag=False)
				return self.index_content()
			else:
				update_ocr_flag(flag=True)

			# Index before parsing because OCR and content extraction can take a bit
			self.index_content()

			self.files_to_cleanup.append(self.downloaded_file)

			try:

				downloaded_file = self.downloaded_file
				local_directory = os.path.dirname(downloaded_file)
				local_filename = os.path.basename(downloaded_file)
				file_without_extension = local_filename.split(".")[0]

				model_work_directory = local_filename.replace(".pdf", "_model_work")

				info(self.logger, "DOWNLOADED to %s" % local_directory)

				# Start the work
				model_directory = os.path.join(local_directory, model_work_directory)
				info(self.logger, "Model directory is %s" % model_directory)
				self.files_to_cleanup.append(model_directory)

				mkdir_p(model_directory)
				send_status_update("made_directories")

				# First, convert PDF to series of images in subdirectory
				send_status_update("starting_image_creation")
				image_maker = PdfToImages({})
				img_dpi, glob_img_filename = image_maker.make_img_from_pdf(downloaded_file,
				                                                           subdirectory=model_work_directory)

				# Second, for all images perform binarization to fix the rotation and coloring (ocropus-nlbin)

				send_status_update("starting_binarization")
				binarization_steps = ["ocropus-nlbin", "-Q", self.worker_count, "%s*.jpg" % file_without_extension]
				print "Performing binarization with `%s`" % " ".join(binarization_steps)
				binarization_process = subprocess.Popen(binarization_steps,
				                                        cwd=model_directory, close_fds=True,
				                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
				                                        universal_newlines=True)
				binarization_output, binarization_err = binarization_process.communicate()
				print "Done with binarization"
				print "Binarization Errors: %s" % binarization_err
				if binarization_err:
					return send_status_update("Problem with binarization: %s" % binarization_err)

				# Third, extract the lines from the binarization (this is the page layout analysis) (ocropus-gpageseg)
				send_status_update("starting_line_extraction")
				line_extraction_steps = ["ocropus-gpageseg", "-n", "-Q", self.worker_count, "*.bin.png"]
				print "Performing line extraction with `%s`" % " ".join(line_extraction_steps)
				line_extraction_process = subprocess.Popen(line_extraction_steps,
				                                           cwd=model_directory, close_fds=True,
				                                           stdout=subprocess.PIPE, stderr=subprocess.PIPE,
				                                           universal_newlines=True)
				line_extraction_output, line_extraction_error = line_extraction_process.communicate()

				print "Done with line extraction"
				print "Line Extraction Errors: %s" % line_extraction_error
				if line_extraction_error:
					return send_status_update("Problem with line extraction: %s" % line_extraction_error)

				# Fourth, convert lines to text columns using the (currently available) model. (ocropus-rpred)

				send_status_update("starting_predictions")
				text_column_steps = ["/app/ocropus-rpred-skip-errors.py", "-Q", self.worker_count, "-n",
				                     "-m", self.model_to_load, "%s*/*.bin.png" % file_without_extension]
				print "Performing text column parsing with `%s`" % " ".join(text_column_steps)
				text_column_process = subprocess.Popen(text_column_steps,
				                                       cwd=model_directory, close_fds=True,
				                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE,
				                                       universal_newlines=True)
				text_column_output, text_column_error = text_column_process.communicate()
				print "Done with text column parsing"
				print "Text column errors: %s" % text_column_error
				if text_column_error:
					print "Had text column errors. Skipping for now."

				send_status_update("starting_ground_truth_data")
				for created_directory in sorted([x for x in
				                                 glob.glob(
					                                 os.path.join(model_directory, "%s*" % file_without_extension)) if
				                                 os.path.isdir(x)]):
					sub_directory = created_directory.split("/")[-1].replace("%s" % file_without_extension, "page_")
					send_status_update("starting_ground_truth_for_%s" % sub_directory)
					# Fifth and final, generate ground truth data HTML file (ocropus-gtedit)
					# Due to some weirdness with ocropus-gtedit we need to run a shell to properly change into the directory
					ground_truth_steps = " ".join(
						["ocropus-gtedit", "html", "./*.bin.png", "-o", "correction_original.html"])
					print "Performing ground truth with `%s`" % ground_truth_steps
					print "In directory: %s" % created_directory
					ground_truth_process = subprocess.Popen(ground_truth_steps,
					                                        cwd=created_directory, close_fds=True,
					                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
					                                        universal_newlines=True, shell=True)
					ground_truth_output, ground_truth_error = ground_truth_process.communicate()
					print "Done with ground truth"
					if ground_truth_error:
						return send_status_update("Problem with ground truth: %s" % ground_truth_error)

					# The initial version before training needs extraction(ocropus-gtedit)
					extraction_steps = " ".join(
						["ocropus-gtedit", "extract", "-O", "correction_original.html"])
					print "Performing extraction with `%s`" % extraction_steps
					print "In directory: %s" % created_directory
					extraction_process = subprocess.Popen(extraction_steps,
					                                      cwd=created_directory, close_fds=True,
					                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE,
					                                      universal_newlines=True, shell=True)
					extraction_output, extraction_error = extraction_process.communicate()
					print "Done with extraction"
					if extraction_error:
						print "Extraction errors: %s" % extraction_error
					if extraction_output:
						self.content += "%s%s" % (gtedit_extraction_to_text(extraction_output), self.page_delimiter)



					os.system("/app/hocr/test.sh")
					print ">>>>> Uploading HOCRed file to S3 <<<<<<"
					files1=open("/tmp/file1.txt").read()

					os.system("aws s3 cp /tmp/test/some_0.pdf s3://%s/%s" %(model_work_bucket,files1))	
					# Finally, upload to S3
					send_status_update("starting_s3_upload_for_%s" % sub_directory)
					s3_key = "model-results/%s/%s" % (file_without_extension, sub_directory)
					s3_sync_steps = ["aws", "s3", "sync", ".",
					                 "s3://%s/%s" % (model_work_bucket, s3_key)]
					print "Uploading to S3 with `%s`" % " ".join(s3_sync_steps)
					s3_sync_process = subprocess.Popen(s3_sync_steps,
					                                   cwd=created_directory, close_fds=True,
					                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
					                                   universal_newlines=True)
					s3_sync_output, s3_sync_error = s3_sync_process.communicate()
					pageno = s3_key[-4:]
					localpage =  int(pageno) - 1
					totalpages = str(pageno)
					page1address = s3_key[:-10]
					
					f10 = open("/tmp/totalpages.txt", 'w')
					f10.write(totalpages)
					f10.close()
					pagescommand = "aws s3 cp /tmp/totalpages.txt s3://%s/%spage__0001/totalpages.txt"  % (model_work_bucket, page1address)
					os.system(pagescommand)
					command2 = "python /app/append/test.py %s"%localpage
					os.system(command2)
					command = "aws s3 cp /tmp/test/correct_%s.html s3://%s/%s/correction_original.html" % (localpage, model_work_bucket, s3_key)
					os.system(command)
					command1 = "aws s3 cp /tmp/file1.txt s3://%s/%s/pdfname.txt" % (model_work_bucket, s3_key)
					os.system(command1)
					
					os.system("echo 9989 > /opt/revision.txt")
					revisioncmd = "aws s3 cp /opt/revision.txt s3://%s/%s/revision.txt"% (model_work_bucket, s3_key)
					os.system(revisioncmd)	
					print s3_key
					print "Done with S3 sync"
					print "S3 Errors: %s" % s3_sync_error
					send_ground_truth_update(sub_directory, model_work_bucket, s3_key)

				send_status_update("finished")
			except:
				traceback.print_exc()
				return send_status_update("Something went wrong")

		info(self.logger, "Finishing parsing content")
		return None
		
	# To update the model
	# 1. Take in the HTML text after a user edits a correction_original.html or revision_x.html file
	# 2. Download it somewhere with name correction.html and extract the png and new truth data with:
	#       env PYTHONIOENCODING=UTF-8 ocropus-gtedit extract correction.html
	# 3. Train the new model (replace en-default.pyrnn.gz with newest model name if one exists) with:
	#       env PYTHONIOENCODING=UTF-8 ocropus-rtrain --load en-default.pyrnn.gz -o revised-model -F 100 *.bin.png
	# The first last_trial value is 100000 and -N of ~100300 allows 3 * 100 lines of training per page

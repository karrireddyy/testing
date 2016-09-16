import sys
import os
from common import config_manager
from common.helper import get_consul_server, get_void_uuid
import json
from boto.s3.connection import S3Connection
from extraction_processor import PdfExtractionProcessor
from training_processor import TrainingProcessor
from common.config_manager import get_config
from common.collections import OCR_GROUND_TRUTH_COLLECTION, OCR_INITIAL_WORK_COLLECTION, OCR_TRAINING_COLLECTION
from common.dbadapter import get_by_id, get_list_by_query, delete_many_by_query, create
import arrow
from training_timer import TrainingTimer

import string
import random
import pika
import multiprocessing
import traceback

from common.log_writer import *
from boto.s3.key import Key

logger = get_logger("content-extraction-pdf")


# The next two methods are shared nearly identically with queue.py in common.
# The change is the blocked_connection_timeout which needs to be tested with legacy Pika,
# as the content_extraction_pdf service uses Pika from master cause long-running tasks


def _get_rabbit_connection_info(queue_key):
	"""
	Establishes a blocking RabbitMQ connection.
	TODO: Make this into a helper class or singleton
	Returns:
		error_msg, {queue_name, connection}
	"""

	if consul_server == "127.0.0.1":
		return "Consul server is set to 127.0.0.1", None
	look_for_service_name = "docker-rabbitmq-5672"
	found_service = config_manager.discover_service(consul_server, look_for_service_name)
	if found_service.__class__.__name__ not in ("list", "tuple"):
		return "Service class not in expected format", None
	if len(found_service) == 0:
		return "No services found for `%s`" % look_for_service_name, None

	rabbitmq_host = found_service[0]["Address"]
	rabbitmq_port = int(found_service[0]["ServicePort"])

	config_arr = config_manager.get_config(consul_server, [queue_key, "rabbitmq_user", "rabbitmq_pass"])
	rabbit_username = config_arr[0]["rabbitmq_user"]
	rabbit_password = config_arr[0]["rabbitmq_pass"]

	try:
		credentials = pika.PlainCredentials(rabbit_username, rabbit_password)
		parameters = pika.ConnectionParameters(rabbitmq_host, rabbitmq_port, "/", credentials,
		                                       heartbeat=0, blocked_connection_timeout=None)
		connection = pika.BlockingConnection(parameters)
		return None, connection
	except Exception, ex:
		return traceback.print_exc(), None


def create_blocking_listener_for_queue(logger, queue, action):
	error_message, connection = _get_rabbit_connection_info(queue)
	if error_message:
		warn(logger, "Could not establish rabbit connection: %s" % error_message)
		return
	elif not connection:
		warn(logger, "No rabbit connection was returned")
		return

	try:
		channel = connection.channel()
		channel.basic_qos(prefetch_count=1)
		channel.queue_declare(queue=queue, durable=True)
		channel.basic_consume(action, queue=queue, no_ack=False)
		channel.start_consuming()
	except Exception, ex:
		warn(logger, "Fatal error creating blocking listener: %s" % traceback.print_exc())
		return

	return True


def create_shared_listener_for_queue(logger, queue, action):
	error_message, connection = _get_rabbit_connection_info(queue)
	if error_message:
		warn(logger, "Could not establish rabbit connection: %s" % error_message)
		return
	elif not connection:
		warn(logger, "No rabbit connection was returned")
		return

	try:
		channel = connection.channel()
		channel.exchange_declare(exchange=queue, exchange_type="fanout", auto_delete=True)
		result = channel.queue_declare(exclusive=True)
		queue_name = result.method.queue
		channel.queue_bind(exchange=queue, queue=queue_name)
		channel.basic_consume(action, queue=queue_name, no_ack=True)
		channel.start_consuming()
	except Exception, ex:
		warn(logger, "Fatal error creating shared listener: %s" % traceback.print_exc())
		return

	return True


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
	return "".join(random.choice(chars) for _ in range(size)) + ".txt"


consul_server = get_consul_server()

config_arr = config_manager.get_config(consul_server, ["content_extraction_pdf_worker_count"])
if not config_arr or not config_arr[0]["content_extraction_pdf_worker_count"]:
	worker_count = 3  # Default worker count of 3
else:
	worker_count = int(config_arr[0]["content_extraction_pdf_worker_count"])

training_items = {}


def set_aws_env_variables():
	# The AWS CLI is used for syncing directories after upload and on training. It does not take
	# command line arguments so OS-wide environment variables need to be used.
	s3_config = get_config(consul_server, ["secret_key", "secret_key_id", "bucket_name"])
	os.environ["AWS_ACCESS_KEY_ID"] = s3_config[0]["secret_key"]
	os.environ["AWS_SECRET_ACCESS_KEY"] = s3_config[0]["secret_key_id"]


def extraction_action(ch, method, properties, body):
	info(logger, "########## Processing pdf content extraction message...#########")
	set_aws_env_variables()
	body = json.loads(body)

	s3_config = get_config(consul_server, ["secret_key", "secret_key_id", "bucket_name"])
	conn = S3Connection(s3_config[0]["secret_key"], s3_config[0]["secret_key_id"])
	bucket = conn.get_bucket(s3_config[0]["bucket_name"])

	file_key = bucket.get_key(body["file_name"])
	metadata = dict(file_key.metadata)
	extension = metadata["extension"]
	entity_id = metadata["entity_id"]

	delete_many_by_query({
		"FileID": entity_id
	}, get_void_uuid(), arrow.utcnow(), OCR_GROUND_TRUTH_COLLECTION)

	delete_many_by_query({
		"FileID": entity_id
	}, get_void_uuid(), arrow.utcnow(), OCR_INITIAL_WORK_COLLECTION)

	processor = PdfExtractionProcessor(consul_server, bucket, body["file_name"], extension, worker_count=worker_count)
	info(logger, "Processor has been initiated. Beginning content parse.")

	# Passing the bucket name in case the model work happens in a separate bucket eventually
	try:
		processor.parse_content(s3_config[0]["bucket_name"], entity_id)
	except:
		error(logger, "Content could not be parsed. ")
		return

	try:
		processor.index_content()
	except:
		error(logger, "Content was parsed, but indexing in Elasticsearch failed.")
		return

	try:
		processor.cleanup()
	except:
		error(logger, "Unable to cleanup local files. Continuing on either way (may need to manually cleanup /opt")

	ch.basic_ack(delivery_tag=method.delivery_tag)
	info(logger, "######### Finished processing pdf content extraction message #########")


def training_action(ch, method, properties, body):
	info(logger, "########## Processing pdf content extraction (training) message...#########")
	set_aws_env_variables()
	body = json.loads(body)

	ground_truth_entity_id = body["ground_truth_data_entity_id"]
	updated_data = body["updated_data"]
	edited_images = body["edited_images"]
	original_file_id = body["original_file_id"]

	if ground_truth_entity_id in training_items:
		info(logger, "Training scheduled...cancelling that timer and will kick off another option")
		training_items[ground_truth_entity_id].stop()
		del training_items[ground_truth_entity_id]

	training_processor = TrainingProcessor(logger, consul_server, ground_truth_entity_id, updated_data, edited_images, original_file_id)
	training_processor.extract_content()

	info(logger, "Training for page with ID %s will begin in 60 seconds" % ground_truth_entity_id)
	training_items[ground_truth_entity_id] = TrainingTimer(training_processor, delay_seconds=60)
	training_items[ground_truth_entity_id].start()

	ch.basic_ack(delivery_tag=method.delivery_tag)
	info(logger, "######### Finished processing pdf content extraction (training) message #########")


def new_model_action(ch, method, properties, body):
	info(logger, "########## Processing new model action message...#########")

	body = json.loads(body)
	system_model_s3_key = body["s3_key"]
	model_to_load = system_model_s3_key.split("/")[-1]
	local_model_location = os.path.join("/usr/local/share/ocropus", model_to_load)

	# TODO: possibly use a checksum check to only download this model if it's new.
	info(logger, "Downloading updated model from S3...")

	s3_config = get_config(consul_server, ["secret_key", "secret_key_id", "bucket_name"])
	conn = S3Connection(s3_config[0]["secret_key"], s3_config[0]["secret_key_id"])
	bucket = conn.get_bucket(body["s3_bucket"])

	s3_key = Key(bucket)
	s3_key.key = system_model_s3_key
	s3_key.get_contents_to_filename(local_model_location)
	if os.path.isfile(local_model_location):
		info(logger, "Successfully downloaded model to local path.")
	else:
		info(logger, "Unable to download latest model!")

	info(logger, "######### Finished processing pdf content extraction (model action) message #########")


if __name__ == "__main__":

	reload(sys)
	sys.setdefaultencoding("utf-8")

	for index in range(worker_count):
		extraction_worker = multiprocessing.Process(target=create_blocking_listener_for_queue,
		                                            args=(logger, "content-extraction-pdf", extraction_action))
		extraction_worker.start()
		if os.environ.get("ALLOW_RETRAIN") == "true":
			info(logger, "Container launched with retraining flag. Will be an OCR worker and retrain models.")
			training_worker = multiprocessing.Process(target=create_blocking_listener_for_queue,
			                                          args=(logger, "content-extraction-pdf-training", training_action))
			training_worker.start()
		else:
			info(logger, "Container launched without retraining flag. Will simply be an OCR worker.")
		new_model_worker = multiprocessing.Process(target=create_shared_listener_for_queue,
		                                           args=(logger, "content-extraction-pdf-new-model", new_model_action))
		new_model_worker.start()

import os
import pika
import config_manager
from log_writer import *
import traceback

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
	if found_service.__class__.__name__ not in ('list', 'tuple'):
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
		parameters = pika.ConnectionParameters(rabbitmq_host, rabbitmq_port, '/', credentials)
		connection = pika.BlockingConnection(parameters)
		return None, connection
	except Exception, ex:
		return traceback.print_exc(), None


def publish_command_to_queue(queue_key, message):
	# TODO: Pass `logger` as first argument, just as the listener method below does.
	error_message, connection = _get_rabbit_connection_info(queue_key)
	if error_message:
		print "Could not establish rabbit connection: %s" % error_message
		return
	elif not connection:
		print "No rabbit connection was returned"
		return

	try:
		channel = connection.channel()
		channel.queue_declare(queue=queue_key, durable=True)
		channel.basic_publish(exchange="", routing_key=queue_key, body=message, properties=pika.BasicProperties(
			delivery_mode=2,  # make message persistent
		))
		connection.close()
	except Exception, ex:
		print "Fatal error: %s" % traceback.print_exc()
		return

	return True


def publish_command_to_exchange(exchange_key, message):
	error_message, connection = _get_rabbit_connection_info(exchange_key)
	if error_message:
		print "Could not establish rabbit connection: %s" % error_message
		return
	elif not connection:
		print "No rabbit connection was returned"
		return

	try:
		channel = connection.channel()
		channel.exchange_declare(exchange=exchange_key, exchange_type='fanout', auto_delete=True)
		channel.basic_publish(exchange=exchange_key, routing_key='', body=message)
		connection.close()
	except Exception, ex:
		print "Fatal error: %s" % traceback.print_exc()
		return

	return True

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
		warn(logger, "Fatal error: %s" % traceback.print_exc())
		return

	return True


def create_blocking_listener_for_service(logger, service_name, action):
	return create_blocking_listener_for_queue(logger, service_name + "_queue", action)


def publish_command_to_service(service_name, msg):
	return publish_command_to_queue(service_name + "_queue", msg)

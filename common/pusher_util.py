import pusher
from log_writer import *
from dbadapter import get_logged_in_user_organization
import config_manager
from helper import get_consul_server

logger = get_logger("pusher-util")

consul_server = get_consul_server()
config_arr = config_manager.get_config(consul_server, ["pusher_key", "pusher_app_id", "pusher_secret"])

pusher_client = pusher.Pusher(
  app_id=config_arr[0]["pusher_app_id"],
  key=config_arr[0]["pusher_key"],
  secret=config_arr[0]["pusher_secret"],
  ssl=False
)

def send_pusher_message(event, data, organization_id=None):
	try:		
		if organization_id !=None:
			channel = "cedarwood_"+organization_id
		else:
			channel = "cedarwood_"+get_logged_in_user_organization()["EntityID"]
			
		pusher_client.trigger(channel, event, data)
		debug(logger,"on channel:"+channel+" event:"+ event + " sent...")
	except Exception, e:
		error(logger, e)

def send_pusher_doc_message(event, data):
	try:		
		channel = "cedarwood_DOCUMENT_"+data["DocumentID"]		
			
		pusher_client.trigger(channel, event, data)
		debug(logger,"on channel:"+channel+" event:"+ event + " sent...")
	except Exception, e:
		error(logger, e)

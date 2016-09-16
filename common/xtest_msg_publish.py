import unittest
import os
import queue
import json

##TODO: these look like functional tests, should be named the same.
class TestMSGPublish(unittest.TestCase):

	def test_publish_msg(self):
		ret = queue.publish_command_to_service("unittest", "Test Msg")
		self.assertTrue(ret)


	def test_publish_msg_html(self):
		msg = "<p>This is my new email</p><br ?> And it is a test"
		ret = queue.publish_command_to_service("unittest", msg)
		self.assertTrue(ret)


	def test_publish_msg_json(self):
		command = {"flag": "to-text", "action": "send-email", 
			"body": "<p>This is my new email</p><br ?> And it is a test",
			"to": "test@example.com",
			"from": "abc@example.com",
			"subject": "New Mail"}
		ret = queue.publish_command_to_service("mail", json.dumps(command))
		self.assertTrue(ret)		


if __name__ == "__main__":
	unittest.main()
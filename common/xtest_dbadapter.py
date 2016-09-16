import unittest
import os
import dbadapter
import helper
import arrow

##TODO: these look like functional tests, should be named the same.
class TestDBAdapater(unittest.TestCase):

	def test_connection(self):
		self.assertIsNotNone(dbadapter.get_db_conn("mycollection"))

	def test_save_with_version(self):
		EntityId = helper.get_uuid()
		item = {"EntityID": EntityId, "value": 12, "Version": helper.get_void_uuid(), "Active": True, "Latest": True}
		ret = dbadapter.save(item, helper.get_uuid(), arrow.utcnow(), "mycollection")
		self.assertIsNotNone(ret)
		self.assertEqual(ret["EntityID"], EntityId)

		ret.pop("_id")
		ret["value"] = 53
		version = ret["Version"]
		ret["Active"] = True
		ret["Latest"] = True
		ret1 = dbadapter.save(ret, helper.get_uuid(), arrow.utcnow(), "mycollection")
		self.assertIsNotNone(ret1)
		self.assertEqual(ret1["EntityID"], EntityId)
		self.assertEqual(ret1["value"], 53)
		self.assertEqual(ret1["PreviousVersion"], version)


	def test_save_without_version(self):
		EntityId = helper.get_uuid()
		item = {"EntityID": EntityId, "value": 12, "Version": helper.get_void_uuid(),  "Active": True, "Latest": True}
		ret = dbadapter.save(item, helper.get_uuid(), arrow.utcnow(), "mycollection")
		self.assertIsNotNone(ret)
		self.assertEqual(ret["EntityID"], EntityId)

		ret.pop("_id")
		ret["value"] = 53
		version = ret["Version"]
		PreviousVersion = ret["PreviousVersion"]
		ret1 = dbadapter.save(ret, helper.get_uuid(), arrow.utcnow(), "mycollection", False)
		self.assertIsNotNone(ret1)
		self.assertEqual(ret1["EntityID"], EntityId)
		self.assertEqual(ret1["value"], 53)
		self.assertEqual(ret1["Version"], version)
		self.assertEqual(ret1["PreviousVersion"], PreviousVersion)


	def test_get_by_id(self):
		EntityId = helper.get_uuid()
		item = {"EntityID": EntityId, "value": 12, "Version": helper.get_void_uuid(), "Active": True, "Latest": True}
		ret = dbadapter.save(item, helper.get_uuid(), arrow.utcnow(), "mycollection")
		self.assertIsNotNone(ret)
		self.assertEqual(ret["EntityID"], EntityId)

		new_item = dbadapter.get_by_id(EntityId, "mycollection")
		self.assertIsNotNone(new_item)
		self.assertEqual(new_item["EntityID"], EntityId)
		self.assertEqual(new_item["value"], 12)


	def test_get_by_id_and_version(self):
		EntityId = helper.get_uuid()
		item = {"EntityID": EntityId, "value": 12, "Version": helper.get_void_uuid(), "Active": True, "Latest": True}
		ret = dbadapter.save(item, helper.get_uuid(), arrow.utcnow(), "mycollection")
		self.assertIsNotNone(ret)
		self.assertEqual(ret["EntityID"], EntityId)

		version1 = ret["Version"]
		ret.pop("_id")
		ret["value"] = 53
		ret["Active"] = True
		ret["Latest"] = True
		ret1 = dbadapter.save(ret, helper.get_uuid(), arrow.utcnow(), "mycollection")
		self.assertIsNotNone(ret1)
		self.assertEqual(ret1["EntityID"], EntityId)
		version2 = ret1["Version"]

		new_item = dbadapter.get_by_id(EntityId, "mycollection", version1)
		self.assertIsNotNone(new_item)
		self.assertEqual(new_item["value"], 12)

		new_item2 = dbadapter.get_by_id(EntityId, "mycollection", version2)
		self.assertIsNotNone(new_item2)
		self.assertEqual(new_item2["value"], 53)


	def test_delete_with_version(self):
		EntityId = helper.get_uuid()
		item = {"EntityID": EntityId, "value": 12, "Version": helper.get_void_uuid(), "Active": True, "Latest": True}
		ret = dbadapter.save(item, helper.get_uuid(), arrow.utcnow(), "mycollection")
		self.assertIsNotNone(ret)
		self.assertEqual(ret["EntityID"], EntityId)

		ret = dbadapter.delete(EntityId, helper.get_uuid(), arrow.utcnow(), "mycollection")
		self.assertTrue(ret)


	def test_delete_without_version(self):
		EntityId = helper.get_uuid()
		item = {"EntityID": EntityId, "value": 12, "Version": helper.get_void_uuid(), "Active": True, "Latest": True}
		ret = dbadapter.save(item, helper.get_uuid(), arrow.utcnow(), "mycollection")
		self.assertIsNotNone(ret)
		self.assertEqual(ret["EntityID"], EntityId)

		ret = dbadapter.delete(EntityId, helper.get_uuid(), arrow.utcnow(), "mycollection", False)
		self.assertTrue(ret)


	def test_get_list_without_limit_and_skip(self):
		for i in range(0,10):
			EntityId = helper.get_uuid()
			item = {"EntityID": EntityId, "value": i, "Version": helper.get_void_uuid(), "Active": True, "Latest": True}
			ret = dbadapter.save(item, helper.get_uuid(), arrow.utcnow(), "mycollection")
			self.assertIsNotNone(ret)

		res = dbadapter.get_list("mycollection")
		i = 0
		for doc in res:
			self.assertIsNotNone(doc)


	def test_get_list_with_limit_and_skip(self):
		for i in range(0,10):
			EntityId = helper.get_uuid()
			item = {"EntityID": EntityId, "value": i, "Version": helper.get_void_uuid(), "Active": True, "Latest": True}
			ret = dbadapter.save(item, helper.get_uuid(), arrow.utcnow(), "mycollection")
			self.assertIsNotNone(ret)

		res = dbadapter.get_list("mycollection", 5, 5)
		i = 0
		for doc in res:
			self.assertIsNotNone(doc)


	def test_get_query(self):
		user_name = "test25@test.com"
		password = "ecd71870d1963316a97e3ac3408c9835ad8cf0f3c1bc703527c30265534f75ae"
		res = dbadapter.get_list_by_query({"Email": user_name,
									"HashedPassword": password, "Active": True}, "user")
		for doc in res:
			print doc


if __name__ == "__main__":
	unittest.main()
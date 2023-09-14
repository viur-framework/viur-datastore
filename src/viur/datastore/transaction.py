import json
import pprint

from viur.datastore import is_viur_datastore_request_ok
from viur.datastore.transport import authenticatedRequest, projectID
from viur.datastore.types import currentTransaction


class Transaction:
	def __init__(self):
		print("init")

	def __enter__(self):
		current_transaction_context = currentTransaction.get()

		print("Entering the context...")
		if current_transaction_context:
			raise "We are in a Transaction"
		else:
			txnOptions = {}
		postData = {
			"transactionOptions": {
				"readWrite": txnOptions
			}
		}
		req = authenticatedRequest(
			url="https://datastore.googleapis.com/v1/projects/%s:beginTransaction" % projectID,
			data=json.dumps(postData).encode("UTF-8"),
		)
		if is_viur_datastore_request_ok(req):
			transaction_key = json.loads(req.content)["transaction"]
			try:
				currentTxn = {"key": transaction_key, "mutations": [], "affectedEntities": []}
				currentTransaction.set(currentTxn)
				print(f"current {transaction_key=}")
			except:
				currentTransaction.set(None)


	def __exit__(self, exc_type, exc_val, exc_tb):
		print("Leaving the context...")
		print(exc_type, exc_val, exc_tb, sep="\n")
		if exc_tb is None:
			current_transaction = currentTransaction.get()
			if current_transaction["mutations"]:
				# Commit TXN
				postData = {
					"mode": "TRANSACTIONAL",  #
					"transaction": current_transaction["key"],
					"mutations": current_transaction["mutations"]
				}
				req = authenticatedRequest(
					url="https://datastore.googleapis.com/v1/projects/%s:commit" % projectID,
					data=json.dumps(postData).encode("UTF-8"),
				)
				is_viur_datastore_request_ok(req)
				pprint.pprint(req.content)

		currentTransaction.set(None)

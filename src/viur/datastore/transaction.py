import json
from time import sleep

import logging
import pprint

from viur.datastore.errors import AbortedError, CollisionError, NoMutationResultsError, ViurDatastoreError
from viur.datastore import is_viur_datastore_request_ok
from viur.datastore.transport import authenticatedRequest, projectID
from viur.datastore.types import currentTransaction


class Transaction:
	def __init__(self, allow_overriding=False):
		self.transaction_key = None
		self.exponential_backoff = 1
		self.allow_overriding = allow_overriding
		self.transaction_init_data = {}

	def __enter__(self):
		current_transaction_context = currentTransaction.get()
		transaction_options = {}
		if current_transaction_context:
			if not self.allow_overriding:
				raise RecursionError("Cannot call a transaction inside a transaction!")
			transaction_options["previousTransaction"] = current_transaction_context["key"]

		self.transaction_init_data = {
			"transactionOptions": {
				"readWrite": transaction_options
			}
		}

		self.set_key()
		current_transaction = {"key": self.transaction_key, "mutations": [], "affectedEntities": []}
		currentTransaction.set(current_transaction)

	def __exit__(self, exc_type, exc_val, exc_tb):
		if exc_tb is None:
			self.commit()
		else:
			self.rollback()
		currentTransaction.set(None)

	def rollback(self):
		authenticatedRequest(
			url=f"https://datastore.googleapis.com/v1/projects/{projectID}:rollback",
			data=json.dumps({
				"transaction": self.transaction_key
			}).encode("UTF-8"),
		)

	def commit(self):
		current_transaction = currentTransaction.get()
		if current_transaction["mutations"]:
			# Commit TXN
			commit_data = {
				"mode": "TRANSACTIONAL",  #
				"transaction": self.transaction_key,
				"mutations": current_transaction["mutations"]
			}
			req = authenticatedRequest(
				url=f"https://datastore.googleapis.com/v1/projects/{projectID}:commit",
				data=json.dumps(commit_data).encode("UTF-8"),
			)
			try:
				is_viur_datastore_request_ok(req)
			except (CollisionError, AbortedError) as err:  # Got a collision or is aborted; retry the entire transaction
				sleep_time = 2 ** self.exponential_backoff
				self.exponential_backoff += 1
				logging.error(f"We got an error in a transaction we try again in {sleep_time} seconds")
				sleep(sleep_time)
				if self.exponential_backoff < 4:
					print(f"try set new transaction key")
					self.set_key()  # get new transaction key
					return self.commit()
				else:
					# If we made it here, all tries are exhausted
					raise CollisionError("All retries are exhausted for this transaction")
			# TODO Mabye do thix in pyx with simdjson
			transaction_result = req.json()
			if not (mutation_results := transaction_result.get("mutationResults")):
				raise NoMutationResultsError("No mutation-results received")
			if not len(mutation_results) == abs(len(current_transaction["affectedEntities"])):
				raise ViurDatastoreError("Invalid number of mutation-results received")
			for i, entity in enumerate(mutation_results):
				try:
					affected_entity = current_transaction["affectedEntities"][i]
					if entity.get("key"):
						if not affected_entity:
							raise ViurDatastoreError("Received an unexpected key-update")
						affected_entity.key = entity["key"]
						affected_entity.version = entity.version
				except IndexError:
					raise ViurDatastoreError("Received an unexpected key-update")
		else:
			self.rollback()

	def set_key(self):
		response = authenticatedRequest(
			url=f"https://datastore.googleapis.com/v1/projects/{projectID}:beginTransaction",
			data=json.dumps(self.transaction_init_data).encode("UTF-8"),
		)
		if is_viur_datastore_request_ok(response):
			transaction_key = response.json()["transaction"]
			self.transaction_key = transaction_key

#!/usr/bin/env python

import base64
import boto3
import botocore
import os
import pprint
import sys
import threading

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "..")
import RunAndMonitorEc2Inst


sqs_region = "us-east-1"
q_name_jr = "acorn-jobs-requested"
msg_body = "acorn-exp-req"


def main(argv):
	try:
		bc = boto3.client("sqs", region_name = sqs_region)
		sqs = boto3.resource("sqs", region_name = sqs_region)
		#DeleteQ(bc)
		q = GetQ(bc, sqs)

		DeqReq(q)

		# TODO: rename the file. poll and process job request messages and job
		# completed messages
		# TODO: implement job completed msg processing.

	except KeyboardInterrupt as e:
		Cons.P("Got a keyboard interrupt. Stopping ...")
		AllWaitTimers.ReqStop()


def DeleteQ(bc):
	with Cons.MT("Deleting queue ..."):
		response = bc.delete_queue(
				QueueUrl="https://queue.amazonaws.com/998754746880/acorn-exps"
				)
		Cons.P(pprint.pformat(response, indent=2))


# Get the queue. Create one if not exists.
def GetQ(bc, sqs):
	with Cons.MT("Getting the queue ..."):
		try:
			queue = sqs.get_queue_by_name(
					QueueName = q_name_jr,
					# QueueOwnerAWSAccountId='string'
					)
			#Cons.P(pprint.pformat(vars(queue), indent=2))
			#{ '_url': 'https://queue.amazonaws.com/998754746880/acorn-exps',
			#		  'meta': ResourceMeta('sqs', identifiers=[u'url'])}
			return queue
		except botocore.exceptions.ClientError as e:
			#Cons.P(pprint.pformat(e, indent=2))
			#Cons.P(pprint.pformat(vars(e), indent=2))
			if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
				pass
			else:
				raise e

		Cons.P("The queue doesn't exists. Creating one ...")
		response = bc.create_queue(QueueName = q_name_jr)
		# Default message retention period is 4 days.

		return sqs.get_queue_by_name(QueueName = q_name_jr)


def EnqReq(q):
	with Cons.MT("Enq a message ..."):
		q.send_message(MessageBody=msg_body, MessageAttributes={
			"rep_model": {"StringValue": "full", "DataType": "String"},
			"exchange_acorn_metadata": {"StringValue": "true", "DataType": "String"},
			})


def DeqReq(q):
	with Cons.MT("Deq messages ..."):
		for i in range(10):
			messages = None
			with WaitTimer():
				while True:
					try:
						messages = None
						messages = q.receive_messages(
								#AttributeNames=[
								#	'Policy'|'VisibilityTimeout'|'MaximumMessageSize'|'MessageRetentionPeriod'|'ApproximateNumberOfMessages'|'ApproximateNumberOfMessagesNotVisible'|'CreatedTimestamp'|'LastModifiedTimestamp'|'QueueArn'|'ApproximateNumberOfMessagesDelayed'|'DelaySeconds'|'ReceiveMessageWaitTimeSeconds'|'RedrivePolicy',
								#	],
								MessageAttributeNames=["All"],
								MaxNumberOfMessages=1,

								# Should be bigger than one experiment duration so that another
								# of the same experiment doesn't get picked up while one is
								# running.
								VisibilityTimeout=3600,

								WaitTimeSeconds=5
								)
					except botocore.exceptions.EndpointConnectionError as e:
						# Could not connect to the endpoint URL: "https://queue.amazonaws.com/"
						Cons.P("%s. Retrying ..." % e)
						time.sleep(2)
					if (messages is not None) and (len(messages)) > 0:
						break

			for m in messages:
				if m.body != msg_body:
					raise RuntimeError("Unexpected. m.body=[%s]" % m.body)

				if m.receipt_handle is None:
					raise RuntimeError("Unexpected")

				if m.message_attributes is None:
					raise RuntimeError("Unexpected")

				params = {}
				for k, v in m.message_attributes.iteritems():
					if v["DataType"] != "String":
						raise RuntimeError("Unexpected")
					v1 = v["StringValue"]
					params[k] = v1
					#Cons.P("  %s: %s" % (k, v1))

				params["sqs_url"] = base64.b64encode(q._url)
				params["sqs_message_receipt_handle"] = m.receipt_handle

				# TODO: May want some admission control here, like one based on how
				# many free instance slots are available.

				Cons.P("Starting an experiment with the parameters %s"
						% ", ".join(['%s=%s' % (k, v) for (k, v) in params.items()]))

				regions_all = [
						"us-east-1"
						, "us-west-1"
						, "us-west-2"
						, "eu-west-1"
						, "eu-central-1"
						, "ap-southeast-1b"
						, "ap-southeast-2"

						# Seoul. Terminates by itself. Turns out they don't have c3 instance types.
						#, "ap-northeast-2"

						, "ap-northeast-1"
						, "sa-east-1"
						]
				ec2_type = "c3.4xlarge"
				# cluster_name for executing the init script.
				params["init_script"] = "acorn-server"

				# Cassandra cluster name. It's ok for multiple clusters to have the same
				# cluster_name for Cassandra. It's ok for multiple clusters to have the same
				# name as long as they don't see each other through the gossip protocol.
				params["cass_cluster_name"] = "acorn"

				RunAndMonitorEc2Inst.Run(regions = regions_all, ec2_type = ec2_type, tags = params)

				# Sleep a bit so that each cluster has a unique ID, which is made of
				# current datetime
				time.sleep(1.5)


class AllWaitTimers:
	timers = []

	@staticmethod
	def Add(wt):
		AllWaitTimers.timers.append(wt)

	@staticmethod
	def ReqStop():
		for wt in AllWaitTimers.timers:
			wt.ReqStop()


class WaitTimer:
	def __enter__(self):
		AllWaitTimers.Add(self)
		self.stop_requested = False
		self.cv = threading.Condition()
		self.t = threading.Thread(target=self.Timer)
		self.t.start()

	def Timer(self):
		self.wait_time = 0
		while self.stop_requested == False:
			with self.cv:
				self.cv.wait(1.0)
			if self.stop_requested == True:
				break

			# Clear current line
			sys.stdout.write(chr(27) + "[2K")
			# Move the cursor to column 1
			sys.stdout.write(chr(27) + "[1G")

			Cons.Pnnl("Waiting for a reply %s" % (self.wait_time + 1))
			self.wait_time += 1

	def __exit__(self, type, value, traceback):
		self.ReqStop()

	def ReqStop(self):
		if self.wait_time > 0:
			print ""

		self.stop_requested = True
		with self.cv:
			self.cv.notifyAll()
		if self.t != None:
			self.t.join()


if __name__ == "__main__":
	sys.exit(main(sys.argv))

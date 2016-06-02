#!/usr/bin/env python

import boto3
import botocore
import os
import pprint
import sys
import threading

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Cons
import Util


sqs_region = "us-east-1"
q_name = "acorn-exps"


def main(argv):
	try:
		bc = boto3.client("sqs", region_name = sqs_region)
		sqs = boto3.resource("sqs", region_name = sqs_region)
		#DeleteQ(bc)
		q = GetQ(bc, sqs)

		DeqReq(q)
	except KeyboardInterrupt as e:
		Cons.P("Got a keyboard interrupt. Stopping ...")
		WaitTimer.ReqStop()


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
					QueueName = q_name,
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
		response = bc.create_queue(QueueName = q_name)
		# Default message retention period is 4 days.

		return sqs.get_queue_by_name(QueueName = q_name)


def EnqReq(q):
	with Cons.MT("Enq a message ..."):
		q.send_message(MessageBody="acorn-exp", MessageAttributes={
			"rep_model": {"StringValue": "full", "DataType": "String"},
			"exchange_acorn_metadata": {"StringValue": "true", "DataType": "String"},
			})


def DeqReq(q):
	with Cons.MT("Deq messages ..."):
		for i in range(10):
			messages = None
			with WaitTimer():
				while True:
					messages = q.receive_messages(
							#AttributeNames=[
							#	'Policy'|'VisibilityTimeout'|'MaximumMessageSize'|'MessageRetentionPeriod'|'ApproximateNumberOfMessages'|'ApproximateNumberOfMessagesNotVisible'|'CreatedTimestamp'|'LastModifiedTimestamp'|'QueueArn'|'ApproximateNumberOfMessagesDelayed'|'DelaySeconds'|'ReceiveMessageWaitTimeSeconds'|'RedrivePolicy',
							#	],
							MessageAttributeNames=["All"],
							MaxNumberOfMessages=1,
							VisibilityTimeout=10,
							WaitTimeSeconds=5
							)
					if len(messages) > 0:
						break

			for m in messages:
				# Get the custom author m attribute if it was set
				author_text = ''
				if m.message_attributes is None:
					raise RuntimeError("Unexpected")

				#rep_model = m.message_attributes.get('rep_model').get('StringValue')
				#Cons.P("[%s] [%s]" % (m.body, rep_model))

				Cons.P("%d m.body=[%s]" % (i, m.body))
				for k, v in m.message_attributes.iteritems():
					if v["DataType"] != "String":
						raise RuntimeError("Unexpected")
					Cons.P("  %s: %s" % (k, v["StringValue"]))

				# TODO: Start the experiment with the parameters

				# TODO: Let the queue know that the m is processed
				#m.delete()


# It can be stopped without the object handle
class WaitTimer:
	stop_requested = False
	cv = threading.Condition()
	t = None

	def __enter__(self):
		WaitTimer.t = threading.Thread(target=self.Timer)
		WaitTimer.t.start()

	def Timer(self):
		self.wait_time = 0
		while WaitTimer.stop_requested == False:
			with WaitTimer.cv:
				WaitTimer.cv.wait(1.0)
			if WaitTimer.stop_requested == True:
				break
			self.wait_time += 1

			# Clear current line
			sys.stdout.write(chr(27) + "[2K")
			# Move the cursor to column 1
			sys.stdout.write(chr(27) + "[1G")

			Cons.Pnnl("Waiting for a reply %s" % self.wait_time)

	def __exit__(self, type, value, traceback):
		if self.wait_time > 0:
			print " ... got one"
		WaitTimer.stop_requested = True
		with WaitTimer.cv:
			WaitTimer.cv.notifyAll()
		WaitTimer.t.join()

	@staticmethod
	def ReqStop():
		WaitTimer.stop_requested = True
		with WaitTimer.cv:
			WaitTimer.cv.notifyAll()
		if WaitTimer.t != None:
			WaitTimer.t.join()


if __name__ == "__main__":
	sys.exit(main(sys.argv))

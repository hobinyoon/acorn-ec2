#!/usr/bin/env python

import boto3
import botocore
import os
import pprint
import sys

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Cons
import Util


sqs_region = "us-east-1"
q_name_jr = "acorn-jobs-requested"
msg_body = "acorn-exp-req"

def main(argv):
	bc = boto3.client("sqs", region_name = sqs_region)
	sqs = boto3.resource("sqs", region_name = sqs_region)
	#DeleteQ(bc)
	q = GetQ(bc, sqs)

	EnqReq(q)


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
	# Measure xDC traffic of object replication and metadata
	_EnqReq(q, {
		"acorn_options.full_replication": "true"
		, "acorn-youtube.replication_type": "partial"
		, "fn_youtube_reqs": "tweets-010"
		, "max_requests": "5000"
		, "simulation_time_dur_in_ms": "10000"
		})
	#_EnqReq(q, {
	#	"acorn_options.full_replication": "false"
	#	, "acorn-youtube.replication_type": "partial"
	#	})


def _EnqReq(q, attrs):
	with Cons.MT("Enq a message ..."):
		msg_attrs = {}
		for k, v in attrs.iteritems():
			msg_attrs[k] = {"StringValue": v, "DataType": "String"}
		q.send_message(MessageBody=msg_body, MessageAttributes={msg_attrs})


if __name__ == "__main__":
	sys.exit(main(sys.argv))

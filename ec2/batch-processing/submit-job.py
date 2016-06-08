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
	q = GetQ(bc, sqs)

	# Measure xDC traffic of object replication and metadata
	MeasureMetadataXdcTraffic(q)


# Get the queue. Create one if not exists.
def GetQ(bc, sqs):
	with Cons.MT("Getting the queue ..."):
		queue = sqs.get_queue_by_name(
				QueueName = q_name_jr,
				# QueueOwnerAWSAccountId='string'
				)
		#Cons.P(pprint.pformat(vars(queue), indent=2))
		#{ '_url': 'https://queue.amazonaws.com/998754746880/acorn-exps',
		#		  'meta': ResourceMeta('sqs', identifiers=[u'url'])}
		return queue


def MeasureMetadataXdcTraffic(q):
	#regions = [
	#		"us-east-1"
	#		, "us-west-1"
	#		, "us-west-2"
	#		, "eu-west-1"
	#		, "eu-central-1"
	#		, "ap-southeast-1b"
	#		, "ap-southeast-2"

	#		# Seoul. Terminates by itself. Turns out they don't have c3 instance types.
	#		#, "ap-northeast-2"

	#		, "ap-northeast-1"
	#		, "sa-east-1"
	#		]
	regions = [
			"us-east-1"
			, "us-west-1"
			]

	Cons.P("regions: %s" % ",".join(regions))

	req_attrs = {
			"regions": ",".join(regions)

			# Partial replication metadata is exchanged
			, "acorn-youtube.replication_type": "partial"

			# Objects are fully replicated
			, "acorn_options.full_replication": "true"

			, "acorn-youtube.fn_youtube_reqs": "tweets-010"
			, "acorn-youtube.max_requests": "5000"
			, "acorn-youtube.simulation_time_dur_in_ms": "10000"
			}
	_EnqReq(q, req_attrs)

	# Full replication, of course without any acorn metadata exchange
	req_attrs["acorn-youtube.replication_type"] = "full"
	req_attrs["acorn_options.use_attr_user"] = "false"
	req_attrs["acorn_options.use_attr_topic"] = "false"
	_EnqReq(q, req_attrs)


def _EnqReq(q, attrs):
	with Cons.MT("Enq a message ..."):
		msg_attrs = {}
		for k, v in attrs.iteritems():
			msg_attrs[k] = {"StringValue": v, "DataType": "String"}
		q.send_message(MessageBody=msg_body, MessageAttributes=msg_attrs)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

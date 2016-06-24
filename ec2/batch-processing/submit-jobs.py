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

	#SingleDevNode(q)

	ByYoutubeWorkloadOfDifferentSizes(q)

	#ByRepModels(q)

	# To dig why some requests are running behind
	#MeasureClientOverhead(q)

	# Measure xDC traffic of object replication and metadata
	#MeasureMetadataXdcTraffic(q)


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


_regions_all = [
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
_regions_1 = [
		"us-east-1"
		]
_regions_2 = [
		"us-east-1"
		, "us-west-1"
		]

_regions_8 = [
		"us-east-1"
		, "us-west-1"
		, "us-west-2"
		, "eu-west-1"
		# capacity-oversubscribed
		#, "eu-central-1"
		, "ap-southeast-1b"
		, "ap-southeast-2"
		, "ap-northeast-1"
		, "sa-east-1"
		]
_regions_1 = [
		"us-east-1"
		]
_regions_2 = [
		"us-east-1"
		, "us-west-1"
		]


def SingleDevNode(q):
	# UT
	req_attrs = {
			"init-script": "acorn-dev"
			, "regions": ",".join(_regions_1)
			}
	_EnqReq(q, req_attrs)


def ByYoutubeWorkloadOfDifferentSizes(q):
	# UT
	req_attrs = {
			"init-script": "acorn-server"
			, "regions": ",".join(_regions_8)

			# Partial replication metadata is exchanged
			, "acorn-youtube.replication_type": "partial"

			, "acorn-youtube.fn_youtube_reqs": "tweets-010"

			# Default is 10240
			#, "acorn-youtube.youtube_extra_data_size": "10240"

			# Default is -1 (request all)
			#, "acorn-youtube.max_requests": "-1"

			# Default is 1800000
			#, "acorn-youtube.simulation_time_dur_in_ms": "1800000"

			# Default is true, true
			#, "acorn_options.use_attr_user": "true"
			#, "acorn_options.use_attr_topic": "true"
			}

	# Full replication, of course without any acorn metadata exchange
	req_attrs["acorn-youtube.replication_type"] = "full"
	req_attrs["acorn_options.use_attr_user"] = "false"
	req_attrs["acorn_options.use_attr_topic"] = "false"

	#for wl in ["tweets-010", "tweets-017", "tweets-054", "tweets-076", "tweets-100"]:
	for wl in ["tweets-010", "tweets-017", "tweets-054", "tweets-076"]:
		req_attrs["acorn-youtube.fn_youtube_reqs"] = wl
		_EnqReq(q, req_attrs)


def ByRepModels(q):
	# UT
	req_attrs = {
			"init-script": "acorn-server"
			, "regions": ",".join(_regions_all)

			# Partial replication metadata is exchanged
			, "acorn-youtube.replication_type": "partial"

			, "acorn-youtube.fn_youtube_reqs": "tweets-010"

			# Default is 10240
			#, "acorn-youtube.youtube_extra_data_size": "10240"

			# Default is -1 (request all)
			#, "acorn-youtube.max_requests": "-1"
			, "acorn-youtube.max_requests": "100000"

			# Default is 1800000
			#, "acorn-youtube.simulation_time_dur_in_ms": "1800000"
			, "acorn-youtube.simulation_time_dur_in_ms": "10000"

			# Default is true, true
			, "acorn_options.use_attr_user": "true"
			, "acorn_options.use_attr_topic": "true"
			}
	_EnqReq(q, req_attrs)

#	# T
#	req_attrs["acorn_options.use_attr_user"] = "false"
#	req_attrs["acorn_options.use_attr_topic"] = "true"
#	_EnqReq(q, req_attrs)
#
#	# U
#	req_attrs["acorn_options.use_attr_user"] = "true"
#	req_attrs["acorn_options.use_attr_topic"] = "false"
#	_EnqReq(q, req_attrs)
#
#	# NA
#	req_attrs["acorn_options.use_attr_user"] = "false"
#	req_attrs["acorn_options.use_attr_topic"] = "false"
#	_EnqReq(q, req_attrs)
#
#	# Full
#	req_attrs["acorn-youtube.replication_type"] = "full"
#	req_attrs["acorn_options.use_attr_user"] = "false"
#	req_attrs["acorn_options.use_attr_topic"] = "false"
#	_EnqReq(q, req_attrs)


def MeasureClientOverhead(q):
	# Maximum 5%. Most of the time negligible.
	req_attrs = {
			# Swap the coordinates of us-east-1 and eu-west-1 to see how much
			# overhead is there in eu-west-1
			"regions": ",".join(["us-east-1"])
			, "acorn-youtube.fn_youtube_reqs": "tweets-100"
			, "acorn-youtube.youtube_extra_data_size": "512"

			# Request all
			, "acorn-youtube.max_requests": "-1"

			, "acorn-youtube.simulation_time_dur_in_ms": "1800000"
			}
	_EnqReq(q, req_attrs)


def MeasureMetadataXdcTraffic(q):
	Cons.P("regions: %s" % ",".join(_regions_all))

	req_attrs = {
			"regions": ",".join(_regions_all)

			# Partial replication metadata is exchanged
			, "acorn-youtube.replication_type": "partial"

			# Objects are fully replicated
			, "acorn_options.full_replication": "true"

			, "acorn-youtube.fn_youtube_reqs": "tweets-010"

			, "acorn-youtube.youtube_extra_data_size": "10240"

			# Request all
			, "acorn-youtube.max_requests": "-1"

			, "acorn-youtube.simulation_time_dur_in_ms": "1800000"
			}
	_EnqReq(q, req_attrs)

	# Full replication, of course without any acorn metadata exchange
	req_attrs["acorn-youtube.replication_type"] = "full"
	req_attrs["acorn_options.use_attr_user"] = "false"
	req_attrs["acorn_options.use_attr_topic"] = "false"
	_EnqReq(q, req_attrs)


def MeasureMetadataXdcTrafficSmallScale(q):
	Cons.P("regions: %s" % ",".join(_regions_all))

	req_attrs = {
			"_regions_all": ",".join(_regions_all)

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

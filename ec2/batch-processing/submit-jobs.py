#!/usr/bin/env python

import boto3
import botocore
import json
import os
import pprint
import sys

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Cons

sys.path.insert(0, "%s/.." % os.path.dirname(__file__))
import Ec2Region


sqs_region = "us-east-1"
q_name_jr = "acorn-jobs-requested"
msg_body = "acorn-exp-req"

def main(argv):
	bc = boto3.client("sqs", region_name = sqs_region)
	sqs = boto3.resource("sqs", region_name = sqs_region)
	q = GetQ(bc, sqs)

	SingleDevNode(q)

	# Test when the full replication (Unmodified Cassandra) saturates the cluster
	# by varying request density (by changing the simulation time)
	#FullRepClusterSaturationTest(q)

	#for i in range(1):
	#	ByYoutubeWorkloadOfDifferentSizes(q)

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


def SingleDevNode(q):
	req_attrs = {
			"init_script": "acorn-dev"
			# Default is "acorn-server"
			, "ami_name": "tweets-db"
			, "region_spot_req": {
				"us-east-1": {"inst_type": "r3.xlarge", "max_price": 1.0}
				#, "us-west-2": {"inst_type": "r3.xlarge", "max_price": 1.0}

				# For the Tweet crawler, MySQL node
				#"us-east-1": {"inst_type": "c3.4xlarge", "max_price": 1.0}
				}
			}
	_EnqReq(q, req_attrs)


# Pricing can be specified per datacenter too later when needed.
_11_region_spot_req = {
		"ap-northeast-1": {"inst_type": "r3.xlarge", "max_price": 3.0}
		, "ap-northeast-2": {"inst_type": "r3.xlarge", "max_price": 3.0}

		, "ap-south-1": {"inst_type": "i2.xlarge", "max_price": 3.0}
		# Keep getting killed due to the limited capacity
		#, "ap-south-1": {"inst_type": "r3.xlarge", "max_price": 3.0}
		#, "ap-south-1": {"inst_type": "r3.2xlarge", "max_price": 3.0}

		, "ap-southeast-1": {"inst_type": "r3.xlarge", "max_price": 3.0}
		, "ap-southeast-2": {"inst_type": "r3.xlarge", "max_price": 3.0}

		# r3.xlarge is oversubscribed and expensive. strange.
		, "eu-central-1": {"inst_type": "r3.2xlarge", "max_price": 3.0}

		, "eu-west-1": {"inst_type": "r3.xlarge", "max_price": 3.0}

		# Sao Paulo doesn't have r3.xlarge
		, "sa-east-1": {"inst_type": "c3.2xlarge", "max_price": 3.0}

		, "us-east-1": {"inst_type": "r3.xlarge", "max_price": 3.0}
		, "us-west-1": {"inst_type": "r3.xlarge", "max_price": 3.0}
		, "us-west-2": {"inst_type": "r3.xlarge", "max_price": 3.0}
		}


def FullRepClusterSaturationTest(q):
	req_attrs = {
			"init_script": "acorn-server"
			, "region_spot_req": _11_region_spot_req

			# Default is 30 mins, 1800 secs.
			#, "acorn-youtube.simulation_time_dur_in_ms": "1800000"

			# Default is true, true
			#, "acorn_options.use_attr_user": "true"
			#, "acorn_options.use_attr_topic": "true"
			}

	# Full replication, of course without any acorn metadata exchange
	req_attrs["acorn-youtube.replication_type"] = "full"
	req_attrs["acorn_options.use_attr_user"] = "false"
	req_attrs["acorn_options.use_attr_topic"] = "false"

	#for i in range(1100000, 2400000, 100000):
	for i in [1100000]:
		req_attrs["acorn-youtube.simulation_time_dur_in_ms"] = str(i)
		_EnqReq(q, req_attrs)


def ByYoutubeWorkloadOfDifferentSizes(q):
	req_attrs = {
			"init_script": "acorn-server"
			, "region_spot_req": _11_region_spot_req

			# Partial replication metadata is exchanged
			, "acorn-youtube.replication_type": "partial"

			, "acorn-youtube.fn_youtube_reqs": "tweets-010"

			# Default is 10240
			#, "acorn-youtube.youtube_extra_data_size": "10240"

			# Default is -1 (request all)
			#, "acorn-youtube.max_requests": "-1"

			# Default is 30 mins, 1800 secs.
			#, "acorn-youtube.simulation_time_dur_in_ms": "1800000"

			# Default is true, true
			#, "acorn_options.use_attr_user": "true"
			#, "acorn_options.use_attr_topic": "true"
			}
	for wl in ["tweets-010", "tweets-017", "tweets-054", "tweets-076", "tweets-100"]:
		req_attrs["acorn-youtube.fn_youtube_reqs"] = wl
		_EnqReq(q, req_attrs)

	# Full replication, of course without any acorn metadata exchange
	req_attrs["acorn-youtube.replication_type"] = "full"
	req_attrs["acorn_options.use_attr_user"] = "false"
	req_attrs["acorn_options.use_attr_topic"] = "false"

	for wl in ["tweets-010", "tweets-017", "tweets-054", "tweets-076", "tweets-100"]:
		req_attrs["acorn-youtube.fn_youtube_reqs"] = wl
		_EnqReq(q, req_attrs)


def ByRepModels(q):
	# UT
	req_attrs = {
			"init_script": "acorn-server"
			, "regions": Ec2Region.All()

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
			"regions": ["us-east-1"]
			, "acorn-youtube.fn_youtube_reqs": "tweets-100"
			, "acorn-youtube.youtube_extra_data_size": "512"

			# Request all
			, "acorn-youtube.max_requests": "-1"

			, "acorn-youtube.simulation_time_dur_in_ms": "1800000"
			}
	_EnqReq(q, req_attrs)


def MeasureMetadataXdcTraffic(q):
	Cons.P("regions: %s" % ",".join(Ec2Region.All()))

	req_attrs = {
			"regions": Ec2Region.All()

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
	Cons.P("regions: %s" % ",".join(Ec2Region.All()))

	req_attrs = {
			"regions": Ec2Region.All()

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
	with Cons.MT("Enq a request: "):
		attrs = attrs.copy()
		Cons.P(pprint.pformat(attrs))

		jc_params = {}
		for k in attrs.keys():
			if k in ["region_spot_req", "ami_name"]:
				jc_params[k] = attrs[k]
				del attrs[k]
		#Cons.P(json.dumps(jc_params))

		msg_attrs = {}
		for k, v in attrs.iteritems():
			msg_attrs[k] = {"StringValue": v, "DataType": "String"}
		msg_attrs["job_controller_params"] = {"StringValue": json.dumps(jc_params), "DataType": "String"}

		q.send_message(MessageBody=msg_body, MessageAttributes=msg_attrs)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

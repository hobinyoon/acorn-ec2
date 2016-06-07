#!/usr/bin/env python

import datetime
import imp
import os
import Queue
import sys
import time

sys.path.insert(0, "..")
import RunAndMonitorEc2Inst

import ConsMt
import JobCompletionQ
import JobReqQ
import InstMonitor


# TODO: sigint not working

def main(argv):
	try:
		PollJrJcMsgs()

		# TODO: poll and process job request messages and job completed messages
		# TODO: implement job completed msg processing.

	except KeyboardInterrupt as e:
		ConsMt.P("Got a keyboard interrupt. Stopping ...")
		InstMonitor.StopAll()


_q = Queue.Queue()

def PollJrJcMsgs():
	JobReqQ.PollBackground(_q)
	JobCompletionQ.PollBackground(_q)

	while True:
		# TODO: Let InstMonitor stop immediately on exit
		with InstMonitor.IM():
			# Blocked waiting until a request is available
			r = _q.get(True)
		if isinstance(r, JobReq):
			ProcessJobReq(r)
		elif isinstance(r, JobCompleted):
			ProcessJobCompletion(r)
		else:
			raise RuntimeError("Unexpected type %s" % type(r))


def ProcessJobReq(jr):
	# TODO
	#	ConsMt.P("Start serving ...")
	#	ConsMt.P("")

	# TODO: May want some admission control here, like one based on how many
	# free instance slots are available.

	ConsMt.P("Processing a job request msg. tags:")
	for k, v in sorted(jr.tags.iteritems()):
		ConsMt.P("  %s:%s" % (k, v))

	# TODO: make it a part of the job request
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

	ec2_type = "c3.4xlarge"

	# Pass these as the init script parameters. Decided not to use EC2 tag
	# for these, due to its limitations.
	#   http://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/allocation-tag-restrictions.html
	jr_sqs_url = q._url
	jr_sqs_msg_receipt_handle = jr.msg.receipt_handle
	init_script = "acorn-server"

	# Cassandra cluster name. It's ok for multiple clusters to have the same
	# cluster_name for Cassandra. It's ok for multiple clusters to have the
	# same name as long as they don't see each other through the gossip
	# protocol.  It's even okay to use the default one: test-cluster
	#tags["cass_cluster_name"] = "acorn"

	RunAndMonitorEc2Inst.Run(
			regions = regions
			, ec2_type = ec2_type
			, tags = jr.tags
			, jr_sqs_url = jr_sqs_url
			, jr_sqs_msg_receipt_handle = jr_sqs_msg_receipt_handle
			, init_script = init_script)
	print ""

	# Sleep a bit so that each cluster has a unique ID, which is made of
	# current datetime
	time.sleep(1.5)


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

def ProcessJobCompletion(jc):
	job_id = jc.tags["job_id"]
	ConsMt.P("Processing a job completion msg. job_id:%s" % job_id)

	fn_module = "%s/../term-insts.py" % os.path.dirname(__file__)
	mod_name,file_ext = os.path.splitext(os.path.split(fn_module)[-1])
	if file_ext.lower() != '.py':
		raise RuntimeError("Unexpected file_ext: %s" % file_ext)
	py_mod = imp.load_source(mod_name, fn_module)
	getattr(py_mod, "main")([fn_module, "job_id:%s" % job_id])

	JobCompletionQ.DeleteMsg(jc)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

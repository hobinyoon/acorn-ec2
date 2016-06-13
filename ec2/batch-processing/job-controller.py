#!/usr/bin/env python

import datetime
import imp
import os
import pprint
import Queue
import sys
import time

sys.path.insert(0, "..")
import ReqSpotAndMonitor
import RunAndMonitorEc2Inst

import ConsMt
import JobCompletionQ
import JobReqQ
import InstMonitor
import S3


def main(argv):
	try:
		ConsMt.P("Starting ...")
		PollJrJcMsgs()
	except KeyboardInterrupt as e:
		ConsMt.P("\n%s Got a keyboard interrupt. Stopping ..." % time.strftime("%y%m%d-%H%M%S"))
	except Exception as e:
		ConsMt.P("\n%s Got an exception: %s. Stopping ..." % (time.strftime("%y%m%d-%H%M%S"), e))
	JobReqQ.DeleteQ()


_req_q = Queue.Queue()

def PollJrJcMsgs():
	JobReqQ.PollBackground(_req_q)
	JobCompletionQ.PollBackground(_req_q)

	while True:
		with InstMonitor.IM():
			# Blocked waiting until a request is available
			#
			# Interruptable get
			#   http://stackoverflow.com/questions/212797/keyboard-interruptable-blocking-queue-in-python
			while True:
				try:
					req = _req_q.get(timeout=100000)
					break
				except Queue.Empty:
					pass

		if isinstance(req, JobReqQ.JobReq):
			ProcessJobReq(req)
		elif isinstance(req, JobCompletionQ.JobCompleted):
			ProcessJobCompletion(req)
		else:
			raise RuntimeError("Unexpected type %s" % type(req))


def ProcessJobReq(jr):
	# TODO: May want some admission control here, like one based on how many
	# free instance slots are available.

	ConsMt.P("\n%s Got a job request msg. attrs:" % time.strftime("%y%m%d-%H%M%S"))
	for k, v in sorted(jr.attrs.iteritems()):
		ConsMt.P("  %s:%s" % (k, v))

	ec2_type = "c3.4xlarge"

	# Pass these as the init script parameters. Decided not to use EC2 tag
	# for these, due to its limitations.
	#   http://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/allocation-tag-restrictions.html
	jr_sqs_url = jr.msg.queue_url
	jr_sqs_msg_receipt_handle = jr.msg.receipt_handle
	init_script = "acorn-server"

	regions = jr.attrs["regions"].split(",")
	# Delete "regions" from the dict to avoid messing up the parameter parsing
	# caused by the commas
	jr.attrs.pop("regions", None)

	# Cassandra cluster name. It's ok for multiple clusters to have the same
	# cluster_name for Cassandra. It's ok for multiple clusters to have the
	# same name as long as they don't see each other through the gossip
	# protocol.  It's even okay to use the default one: test-cluster
	#tags["cass_cluster_name"] = "acorn"

	if False:
		# On-demand instances are too expensive.
		RunAndMonitorEc2Inst.Run(
				regions = regions
				, ec2_type = ec2_type
				, tags = jr.attrs
				, jr_sqs_url = jr_sqs_url
				, jr_sqs_msg_receipt_handle = jr_sqs_msg_receipt_handle
				, init_script = init_script)
	else:
		ReqSpotAndMonitor.Run(
				regions = regions
				, ec2_type = ec2_type
				, tags = jr.attrs
				, jr_sqs_url = jr_sqs_url
				, jr_sqs_msg_receipt_handle = jr_sqs_msg_receipt_handle
				, init_script = init_script
				, price = 1.0
				)

	# No need to sleep here. Launching a cluster takes like 30 secs.  Used to
	# sleep a bit so that each cluster has a unique ID, which is made of current
	# datetime
	#time.sleep(1.5)


def ProcessJobCompletion(jc):
	job_id = jc.attrs["job_id"]
	ConsMt.P("\n%s Got a job completion msg. job_id:%s" % (time.strftime("%y%m%d-%H%M%S"), job_id))

	fn_module = "%s/../term-insts.py" % os.path.dirname(__file__)
	mod_name,file_ext = os.path.splitext(os.path.split(fn_module)[-1])
	if file_ext.lower() != '.py':
		raise RuntimeError("Unexpected file_ext: %s" % file_ext)
	py_mod = imp.load_source(mod_name, fn_module)
	getattr(py_mod, "main")([fn_module, "job_id:%s" % job_id])

	JobReqQ.DeleteMsg(jc.attrs["job_req_msg_recript_handle"])

	JobCompletionQ.DeleteMsg(jc)
	S3.Sync()


if __name__ == "__main__":
	sys.exit(main(sys.argv))

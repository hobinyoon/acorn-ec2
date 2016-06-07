#!/usr/bin/env python

import base64
import boto3
import botocore
import datetime
import os
import pprint
import sys
import threading
import time

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "..")
import RunAndMonitorEc2Inst


_bc = None
_sqs = None
sqs_region = "us-east-1"
q_name_jr = "acorn-jobs-requested"
msg_body = "acorn-exp-req"


def main(argv):
	try:
		global _bc, _sqs
		_bc = boto3.client("sqs", region_name = sqs_region)
		_sqs = boto3.resource("sqs", region_name = sqs_region)

		q = GetQ()
		DeqReq(q)

		# TODO: poll and process job request messages and job completed messages
		# TODO: implement job completed msg processing.

	except KeyboardInterrupt as e:
		ConsP("Got a keyboard interrupt. Stopping ...")
		AllWaitTimers.ReqStop()


# Get the queue. Create one if not exists.
def GetQ():
	with Cons.MT("Getting the queue ..."):
		try:
			queue = _sqs.get_queue_by_name(
					QueueName = q_name_jr,
					# QueueOwnerAWSAccountId='string'
					)
			#ConsP(pprint.pformat(vars(queue), indent=2))
			#{ '_url': 'https://queue.amazonaws.com/998754746880/acorn-exps',
			#		  'meta': ResourceMeta('sqs', identifiers=[u'url'])}
			return queue
		except botocore.exceptions.ClientError as e:
			#ConsP(pprint.pformat(e, indent=2))
			#ConsP(pprint.pformat(vars(e), indent=2))
			if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
				pass
			else:
				raise e

		ConsPnnl("The queue doesn't exists. Creating one ")
		while True:
			response = None
			try:
				response = _bc.create_queue(QueueName = q_name_jr)
				# Default message retention period is 4 days.
				print ""
				break
			except botocore.exceptions.ClientError as e:
				# When calling the CreateQueue operation: You must wait 60 seconds after
				# deleting a queue before you can create another with the same name.
				# It doesn't give me how much more you need to wait. Polling until succeed.
				if e.response["Error"]["Code"] == "AWS.SimpleQueueService.QueueDeletedRecently":
					sys.stdout.write(".")
					sys.stdout.flush()
					time.sleep(2)
				else:
					raise e

		return _sqs.get_queue_by_name(QueueName = q_name_jr)


def DeqReq(q):
	ConsP("Start serving ...")
	ConsP("")
	while True:
		messages = None
		with InstMonitor():
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
					ConsP("%s. Retrying ..." % e)
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

			tags = {}
			for k, v in m.message_attributes.iteritems():
				if v["DataType"] != "String":
					raise RuntimeError("Unexpected")
				v1 = v["StringValue"]
				tags[k] = v1
				#ConsP("  %s: %s" % (k, v1))

			# TODO: May want some admission control here, like one based on how many
			# free instance slots are available.

			ConsP("Got an experiment request. tags:")
			for k, v in sorted(tags.iteritems()):
				ConsP("  %s:%s" % (k, v))

			# TODO
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
			jr_sqs_msg_receipt_handle = m.receipt_handle
			init_script = "acorn-server"

			# Cassandra cluster name. It's ok for multiple clusters to have the same
			# cluster_name for Cassandra. It's ok for multiple clusters to have the
			# same name as long as they don't see each other through the gossip
			# protocol.  It's even okay to use the default one: test-cluster
			#tags["cass_cluster_name"] = "acorn"

			RunAndMonitorEc2Inst.Run(
					regions = regions
					, ec2_type = ec2_type
					, tags = tags
					, jr_sqs_url = jr_sqs_url
					, jr_sqs_msg_receipt_handle = jr_sqs_msg_receipt_handle
					, init_script = init_script)
			print ""

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


_fmt_desc_inst = "%13s %-15s %10s %15s %13s"

class InstMonitor:
	def __enter__(self):
		AllWaitTimers.Add(self)
		self.stop_requested = False
		self.cv = threading.Condition()
		self.t = threading.Thread(target=self.DescInst)
		self.t.start()

	def DescInst(self):
		self.bt = time.time()
		self.lines_printed = 0
		while self.stop_requested == False:
			bt = time.time()

			self._DescInst()

			if self.stop_requested:
				break

			wait_time = 10 - (time.time() - bt)
			if wait_time > 0:
				with self.cv:
					self.cv.wait(wait_time)

	def _DescInst(self):
		sys.stdout.write("Describing instances:")
		sys.stdout.flush()
		self.lines_printed += 1

		dis = []
		regions_all = [
				"us-east-1"
				, "us-west-1"
				, "us-west-2"
				, "eu-west-1"
				, "eu-central-1"
				, "ap-southeast-1"
				, "ap-southeast-2"
				, "ap-northeast-2"
				, "ap-northeast-1"
				, "sa-east-1"
				]
		for r in regions_all:
			dis.append(DescInstPerRegion(r))

		self.threads = []
		for di in dis:
			t = threading.Thread(target=di.Run)
			self.threads.append(t)
			t.start()

		for t in self.threads:
			t.join()
		print ""

		# Clear previous lines
		for i in range(self.lines_printed):
			# Clear current line
			sys.stdout.write(chr(27) + "[2K")
			# Move the cursor up
			sys.stdout.write(chr(27) + "[1A")
			# Move the cursor to column 1
			sys.stdout.write(chr(27) + "[1G")
		# Clear current line
		sys.stdout.write(chr(27) + "[2K")
		self.lines_printed = 0

		num_insts = 0
		for di in dis:
			num_insts += di.NumInsts()
		if num_insts == 0:
			ConsP("No instances found.")
			self.lines_printed += 1
			return

		header = Util.BuildHeader(_fmt_desc_inst,
			"job_id"
			" Placement:AvailabilityZone"
			" InstanceId"
			" PublicIpAddress"
			" State:Name"
			)
		ConsP(header)
		self.lines_printed += len(header.split("\n"))

		results = []
		for di in dis:
			results += di.GetResults()
		for r in sorted(results):
			ConsP(r)
			self.lines_printed += 1

		ConsP("")
		ConsP("Time since the last job request: %s" % (str(datetime.timedelta(seconds=(time.time() - self.bt)))))
		ConsP("")
		self.lines_printed += 3

	def __exit__(self, type, value, traceback):
		self.ReqStop()

	def ReqStop(self):
		self.stop_requested = True
		with self.cv:
			self.cv.notifyAll()
		if self.t != None:
			self.t.join()
			# It doesn't kill the running threads immediately, which is fine. There
			# is only like 1 - 2 secs of delay.


class DescInstPerRegion:
	def __init__(self, region):
		self.region = region
		self.key_error = None

	def Run(self):
		try:
			boto_client = boto3.session.Session().client("ec2", region_name=self.region)
			self.response = boto_client.describe_instances()
		except KeyError as e:
			#ConsP("region=%s KeyError=[%s]" % (self.region, e))
			self.key_error = e

		sys_stdout_write(" %s" % self.region)

	def NumInsts(self):
		if self.key_error is not None:
			return 0
		num = 0
		for r in self.response["Reservations"]:
			for r1 in r["Instances"]:
				num += 1
		return num

	def GetInstDesc(self):
		ids = []
		if self.key_error is not None:
			return ids
		for r in self.response["Reservations"]:
			ids += r["Instances"]
		return ids


	def GetResults(self):
		if self.key_error is not None:
			return ["region=%s KeyError=[%s]" % (self.region, self.key_error)]

		#ConsP(pprint.pformat(self.response, indent=2, width=100))
		results = []
		for r in self.response["Reservations"]:
			for r1 in r["Instances"]:
				if _Value(_Value(r1, "State"), "Name") == "terminated":
					continue

				tags = {}
				if "Tags" in r1:
					for t in r1["Tags"]:
						tags[t["Key"]] = t["Value"]

				results.append(_fmt_desc_inst % (
					tags.get("job_id")
					, _Value(_Value(r1, "Placement"), "AvailabilityZone")
					, _Value(r1, "InstanceId")
					, _Value(r1, "PublicIpAddress")
					, _Value(_Value(r1, "State"), "Name")
					))
		return results


def _Value(dict_, key):
	if key == "":
		return ""

	if key in dict_:
		return dict_[key]
	else:
		return ""


_print_lock = threading.Lock()

def ConsP(msg):
	with _print_lock:
		Cons.P(msg)

def ConsPnnl(msg):
	with _print_lock:
		Cons.Pnnl(msg)

def sys_stdout_write(msg):
	with _print_lock:
		sys.stdout.write(msg)
		sys.stdout.flush()


if __name__ == "__main__":
	sys.exit(main(sys.argv))

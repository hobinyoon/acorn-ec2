#!/usr/bin/env python

import boto3
import os
import pprint
import sys
import threading

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util


_fmt = "%-15s %10s %13s %13s"


def RunTermInst():
	regions = [
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

	threads = []

	sys.stdout.write("terminating all running instances:")
	sys.stdout.flush()

	tis = []
	for r in regions:
		tis.append(TermInst(r))

	for ti in tis:
		t = threading.Thread(target=ti.Run)
		threads.append(t)
		t.start()

	for t in threads:
		t.join()
	print ""
	print ""

	ConsP(Util.BuildHeader(_fmt,
		"Region"
		" InstanceId"
		" PrevState"
		" CurrState"
		))

	for ti in tis:
		ti.PrintResult()


class TermInst:
	def __init__(self, region):
		self.region = region

	def Run(self):
		boto_client = boto3.session.Session().client("ec2", region_name=self.region)
		response = boto_client.describe_instances()
		#ConsP(pprint.pformat(response, indent=2, width=100))

		# "running" instances
		self.inst_ids = []

		for r in response["Reservations"]:
			for r1 in r["Instances"]:
				if "Name" in r1["State"]:
					if r1["State"]["Name"] == "running":
						self.inst_ids.append(r1["InstanceId"])
		#ConsP("There are %d \"running\" instances." % len(self.inst_ids))
		#ConsP(pprint.pformat(self.inst_ids, indent=2, width=100))

		if len(self.inst_ids) == 0:
			sys_stdout_write(" %s" % self.region)
			return

		self.response = boto_client.terminate_instances(
				InstanceIds = self.inst_ids
				)
		sys_stdout_write(" %s" % self.region)

	def PrintResult(self):
		if len(self.inst_ids) == 0:
			return

		#ConsP(pprint.pformat(self.response, indent=2, width=100))
		for ti in self.response["TerminatingInstances"]:
			ConsP(_fmt % (
				self.region
				, ti["InstanceId"]
				, ti["PreviousState"]["Name"]
				, ti["CurrentState"]["Name"]
				))


def _Value(dict_, key):
	if key == "":
		return ""

	if key in dict_:
		return dict_[key]
	else:
		return ""


_print_lock = threading.Lock()

# Serialization is not needed in this file. Leave it for now.
def ConsP(msg):
	with _print_lock:
		Cons.P(msg)


def sys_stdout_write(msg):
	with _print_lock:
		sys.stdout.write(msg)
		sys.stdout.flush()


def TestTermInst():
	boto_client = boto3.session.Session().client("ec2", region_name="us-east-1")
	response = boto_client.describe_instances()
	#ConsP(pprint.pformat(response, indent=2, width=100))

	# "running" instances
	inst_ids = []

	for r in response["Reservations"]:
		for r1 in r["Instances"]:
			if "Name" in r1["State"]:
				if r1["State"]["Name"] == "running":
					inst_ids.append(r1["InstanceId"])

	ConsP("There are %d \"running\" instances." % len(inst_ids))
	#ConsP(pprint.pformat(inst_ids, indent=2, width=100))

	response = boto_client.terminate_instances(
			DryRun=True,
			InstanceIds = inst_ids
			)
	ConsP(pprint.pformat(response, indent=2, width=100))


def main(argv):
	ConsP("Implement by tags")
	sys.exit(1)

	#TestTermInst()
	RunTermInst()


if __name__ == "__main__":
	sys.exit(main(sys.argv))

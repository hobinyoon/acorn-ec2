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


def RunTermInst(tags):
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

	sys.stdout.write("Terminating running instances:")
	sys.stdout.flush()

	tis = []
	for r in regions:
		tis.append(TermInst(r, tags))

	for ti in tis:
		t = threading.Thread(target=ti.Run)
		t.daemon = True
		threads.append(t)
		t.start()

	for t in threads:
		t.join()
	print ""

	Cons.P(Util.BuildHeader(_fmt,
		"Region"
		" InstanceId"
		" PrevState"
		" CurrState"
		))

	for ti in tis:
		ti.PrintResult()


class TermInst:
	def __init__(self, region, tags):
		self.region = region
		self.tags = tags

	def Run(self):
		boto_client = boto3.session.Session().client("ec2", region_name=self.region)

		response = None
		if self.tags is None:
			response = boto_client.describe_instances()
		else:
			filters = []
			for k, v in self.tags.iteritems():
				d = {}
				d["Name"] = ("tag:%s" % k)
				d["Values"] = [v]
				filters.append(d)
			response = boto_client.describe_instances(Filters = filters)
		#Cons.P(pprint.pformat(response, indent=2, width=100))

		# "running" instances
		self.inst_ids = []

		for r in response["Reservations"]:
			for r1 in r["Instances"]:
				if "Name" in r1["State"]:
					if r1["State"]["Name"] == "running":
						self.inst_ids.append(r1["InstanceId"])
		#Cons.P("There are %d \"running\" instances." % len(self.inst_ids))
		#Cons.P(pprint.pformat(self.inst_ids, indent=2, width=100))

		if len(self.inst_ids) == 0:
			Cons.sys_stdout_write(" %s" % self.region)
			return

		self.response = boto_client.terminate_instances(InstanceIds = self.inst_ids)
		Cons.sys_stdout_write(" %s" % self.region)

	def PrintResult(self):
		if len(self.inst_ids) == 0:
			return

		#Cons.P(pprint.pformat(self.response, indent=2, width=100))
		for ti in self.response["TerminatingInstances"]:
			Cons.P(_fmt % (
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


def main(argv):
	if len(argv) < 2:
		print "Usage: %s (all or tags in key:value pairs)" % argv[0]
		sys.exit(1)

	tags = None
	if argv[1] != "all":
		tags = {}
		for i in range(1, len(argv)):
			t = argv[i].split(":")
			if len(t) != 2:
				raise RuntimeError("Unexpected. argv[%d]=[%s]" % (i, argv[i]))
			tags[t[0]] = t[1]

	RunTermInst(tags)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

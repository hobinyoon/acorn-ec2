#!/usr/bin/env python

import boto3
import os
import pprint
import sys
import threading

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util


def GetByTags(tags):
	threads = []

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

	dis = []
	for r in regions:
		dis.append(DescInst(r, tags))

	for di in dis:
		t = threading.Thread(target=di.Run)
		threads.append(t)
		t.start()

	for t in threads:
		t.join()

	ips = []
	for di in dis:
		ip = di.GetIp()
		if ip == None:
			continue
		ips.append(ip)
	return ips


class DescInst:
	def __init__(self, region, tags):
		self.region = region
		self.tags = tags

	def Run(self):
		boto_client = boto3.session.Session().client("ec2", region_name=self.region)

		filters = []
		for k, v in self.tags.iteritems():
		 d = {}
		 d["Name"] = ("tag:%s" % k)
		 d["Values"] = [v]
		 filters.append(d)
		self.response = boto_client.describe_instances(Filters = filters)

		self.response = boto_client.describe_instances(
				Filters = filters
				)

	def GetIp(self):
		#Cons.P(pprint.pformat(self.response, indent=2, width=100))
		#Cons.P(pprint.pformat(self.response["Reservations"], indent=2, width=100))
		pub_ips = []
		for r in self.response["Reservations"]:
			if len(r["Instances"]) == 0:
				continue
			if len(r["Instances"]) > 1:
				raise RuntimeError("Unexpected. %d instances in region %s" % (len(r["Instances"]), self.region))
			inst = r["Instances"][0]
			if inst["State"]["Name"] != "running":
				continue
			pub_ips.append(inst["PublicIpAddress"])
		if len(pub_ips) == 0:
			return None
		if len(pub_ips) != 1:
			raise RuntimeError("Unexpected. pub_ips=[%s]" % (" ".join(pub_ips)))
		return pub_ips[0]


_my_pub_ip = None

def GetMyPubIp():
	global _my_pub_ip
	if _my_pub_ip == None:
		_my_pub_ip = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/public-ipv4"
				, print_cmd = False, print_result = False)
	return _my_pub_ip

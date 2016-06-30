#!/usr/bin/env python

import boto3
import os
import pprint
import sys
import threading

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util

import Ec2Region


def main(argv):
	with Cons.MTnnl("Checking:"):
		checks = []
		for r in Ec2Region.All():
			checks.append(Check(r))

		threads = []
		for c in checks:
			t = threading.Thread(target=c.Run)
			threads.append(t)
			t.start()

		for t in threads:
			t.join()
		print ""

		for c in checks:
			Cons.P("%-14s %2d" % (c.region, c.max_inst))


class Check:
	def __init__(self, region):
		self.region = region

	def Run(self):
		bc = boto3.session.Session().client("ec2", region_name = self.region)
		response = bc.describe_account_attributes(
				AttributeNames=[
					"max-instances",
					]
				)
		#Cons.P(pprint.pformat(response, indent=2))
		for r in response["AccountAttributes"]:
			if r["AttributeName"] != "max-instances":
				continue
			if len(r["AttributeValues"]) != 1:
				raise RuntimeError("len(r[\"AttributeValues\"])=%d" % len(r["AttributeValues"]))
			self.max_inst = int(r["AttributeValues"][0]["AttributeValue"])
			#Cons.P(self.max_inst)

		Cons.sys_stdout_write(" %s" % self.region)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

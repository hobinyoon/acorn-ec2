#!/usr/bin/env python

import boto3
import os
import pprint
import sys
import threading

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util


def main(argv):
	with Cons.MTnnl("Checking:"):
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

		checks = []
		for r in regions_all:
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
		#ConsP(pprint.pformat(response, indent=2))
		for r in response["AccountAttributes"]:
			if r["AttributeName"] != "max-instances":
				continue
			if len(r["AttributeValues"]) != 1:
				raise RuntimeError("len(r[\"AttributeValues\"])=%d" % len(r["AttributeValues"]))
			self.max_inst = int(r["AttributeValues"][0]["AttributeValue"])
			#ConsP(self.max_inst)

		with _print_lock:
			sys.stdout.write(" %s" % self.region)
			sys.stdout.flush()


_print_lock = threading.Lock()

def ConsP(msg):
	with _print_lock:
		Cons.P(msg)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

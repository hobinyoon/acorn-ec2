#!/usr/bin/env python

import boto3
import os
import sys

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util


def _DescInst():
	boto_client = boto3.client("ec2")
	response = boto_client.describe_instances(
			Filters = [{
				'Name': 'tag:Name',
				'Values': ['acorn-server']
				}]
			)
	#Cons.P(pprint.pformat(response, indent=2, width=100))

	fmt = "%10s %9s %13s %10s %15s %15s %10s"
	Cons.P(Util.BuildHeader(fmt,
		"InstanceId"
		" InstanceType"
		" LaunchTime"
		" Placement:AvailabilityZone"
		" PrivateIpAddress"
		" PublicIpAddress"
		" State:Name"))

	for r in response["Reservations"]:
		for r1 in r["Instances"]:
			Cons.P(fmt % (
				_Value(r1, "InstanceId")
				, _Value(r1, "InstanceType")
				, _Value(r1, "LaunchTime").strftime("%y%m%d-%H%M%S")
				, _Value(_Value(r1, "Placement"), "AvailabilityZone")
				, _Value(r1, "PrivateIpAddress")
				, _Value(r1, "PublicIpAddress")
				, _Value(_Value(r1, "State"), "Name")
				))


def _Value(dict_, key):
	if key == "":
		return ""

	if key in dict_:
		return dict_[key]
	else:
		return ""


def main(argv):
	_DescInst()


if __name__ == "__main__":
	sys.exit(main(sys.argv))
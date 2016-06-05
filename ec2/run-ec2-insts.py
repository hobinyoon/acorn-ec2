#!/usr/bin/env python

import sys

import RunAndMonitorEc2Inst


def main(argv):
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

	if len(argv) < 3:
		print "Usage: %s [acorn_exp_param]+ [region]+" % argv[0]
		print "  acorn_exp_param examples: rep_model:full exchange_acorn_metadata:true"
		print "  region: all or some of %s" % " ".join(regions_all)
		sys.exit(1)

	# Key-value delimiter is :. SQS message receipt handle seems to be
	# base64-encoded, which can contain the char =. So, = cannot be used as a
	# delimiter for the key-value pairs.
	# http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/ImportantIdentifiers.html

	regions = []
	params = {}
	for i in range(1, len(argv)):
		a = argv[i]
		t = a.split(":")
		if len(t) == 1:
			if t == "all":
				regions = regions_all
			else:
				regions.append(t)
		elif len(t) == 2:
			params[t[0]] = t[1]
		else:
			raise RuntimeError("Unexpected argv=[%s]" % " ".join(argv))

	# cluster_name for executing the init script.
	params["init_script"] = "acorn-server"

	# Cassandra cluster name. It's ok for multiple clusters to have the same
	# cluster_name for Cassandra. It's ok for multiple clusters to have the same
	# name as long as they don't see each other through the gossip protocol.
	params["cass_cluster_name"] = "acorn"

	# EC2 instance types
	#
	# 4 vCPUs, 7.5 Gib RAM, EBS only, $0.209 per Hour
	# "The specified instance type can only be used in a VPC. A subnet ID or network interface ID is required to carry out the request."
	# ec2_type = "c4.xlarge"
	#
	# 4 vCPUs, 7.5 Gib RAM, 2 x 40 SSD, $0.21 per Hour
	# ec2_type = "c3.xlarge"
	#
	# For fast development
	ec2_type = "c3.4xlarge"

	RunAndMonitorEc2Inst.Run(regions = regions, ec2_type = ec2_type, tags = params)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

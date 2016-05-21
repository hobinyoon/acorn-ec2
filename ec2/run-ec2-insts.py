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

	if len(argv) == 1:
		print "Usage: %s [region]+" % argv[0]
		print "  region: all or some of %s" % " ".join(regions_all)
		sys.exit(1)

	# Note: Not sure if I want to parameterize the cluster name too. It can be
	# generated dynamically.

	regions = []
	if argv[1] == "all":
		regions = regions_all
	else:
		for i in range(len(argv)):
			if i == 0:
				continue
			regions.append(argv[i])

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

	RunAndMonitorEc2Inst.Run(
			regions = regions
			, tag_name = "acorn-server"
			, ec2_type = ec2_type)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

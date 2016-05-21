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

	# Note: I may want to use option parsing utility.

	# acorn_exp_param seems hacky. Can be generalized to a dict of key value later.
	if len(argv) < 2:
		print "Usage: %s acorn_exp_param [region]+" % argv[0]
		print "  acorn_exp_param examples: u, t, ut, na, full"
		print "  region: all or some of %s" % " ".join(regions_all)
		sys.exit(1)

	# Note: In the future, I may want to generate the cluster name on the fly.

	acorn_exp_param = argv[1]

	regions = []
	if argv[2] == "all":
		regions = regions_all
	else:
		for i in range(len(argv)):
			if i < 2:
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
			, ec2_type = ec2_type
			# TODO: The first key used to be just "Name". replace all.
			, tags = {"cluster_name": "acorn-server"

				# Per cluster parameter. Combined with the cluster_name, this is used
				# for identifying a cluster. Seems hacky, but okay for now.
				, "acorn_exp_param": acorn_exp_param}
			)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

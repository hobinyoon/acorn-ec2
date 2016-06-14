#!/usr/bin/env python

import os
import sys

sys.path.insert(0, "%s/../util/python" % os.path.dirname(__file__))
import Util


def main(argv):
	target_regions = [
			"us-west-1"
			, "us-west-2"
			, "eu-west-1"
			, "eu-central-1"
			, "ap-southeast-1"
			, "ap-southeast-2"
			, "ap-northeast-1"
			, "sa-east-1"
			]

	for r in target_regions:
		cmd = "aws ec2 copy-image" \
				" --source-image-id ami-f051949d" \
				" --source-region us-east-1" \
				" --region %s" \
				" --name acorn-server-160613-1826" % r
		Util.RunSubp(cmd)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

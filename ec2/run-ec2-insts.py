#!/usr/bin/env python

import sys

import RunAndMonitorEc2Inst


def main(argv):
	RunAndMonitorEc2Inst.Run(regions = ["us-east-1", "us-west-1"], tag_name = "acorn-server")


if __name__ == "__main__":
	sys.exit(main(sys.argv))

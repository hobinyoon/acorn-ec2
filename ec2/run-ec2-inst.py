#!/usr/bin/env python

import sys

import RunAndMonitorEc2Inst


def main(argv):
	RunAndMonitorEc2Inst.Run()


if __name__ == "__main__":
	sys.exit(main(sys.argv))

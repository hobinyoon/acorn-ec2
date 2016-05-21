#!/usr/bin/env python

import sys

import DescInst


def main(argv):
	if len(argv) == 1:
		DescInst.Run()
	elif len(argv) == 2:
		DescInst.Run(argv[1])
	else:
		# Note: It's okay for now, but what if you have other projects than acorn?
		print "Usage: %s [acorn_exp_param]" % argv[0]


if __name__ == "__main__":
	sys.exit(main(sys.argv))

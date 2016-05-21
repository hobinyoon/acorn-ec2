#!/usr/bin/env python

import sys

import DescInst


def main(argv):
	if len(argv) != 2:
		# Note: It's okay for now, but what if you have other projects than acorn?
		print "Usage: %s [acorn_exp_param]" % argv[0]
		sys.exit(1)

	DescInst.Run(argv[1])


if __name__ == "__main__":
	sys.exit(main(sys.argv))

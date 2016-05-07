#!/usr/bin/env python

import sys

import DescInst


def main(argv):
	if len(argv) == 1:
		DescInst.Run()
	elif len(argv) == 2:
		DescInst.Run(argv[1])
	else:
		print "Usage: %s [tag_name]" % argv[0]


if __name__ == "__main__":
	sys.exit(main(sys.argv))

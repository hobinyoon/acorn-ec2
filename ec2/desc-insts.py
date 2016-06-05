#!/usr/bin/env python

import sys

import DescInst


def main(argv):
	tags = {}
	for i in range(1, len(argv)):
		t = argv[i].split(":")
		if len(t) != 2:
			raise RuntimeError("Unexpected. argv[%d]=[%s]" % (i, argv[i]))
		tags[t[0]] = t[1]

	DescInst.Run(tags)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

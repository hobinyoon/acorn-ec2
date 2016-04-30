#!/usr/bin/env python

import sys

sys.path.insert(0, "../util/python")
import Cons
import Util


def main(argv):
	Util.RunSubp("mv /home/ubuntu/work /mnt/local-ssd0/")
	Util.RunSubp("ln -s /mnt/local-ssd0/work /home/ubuntu")


if __name__ == "__main__":
	sys.exit(main(sys.argv))

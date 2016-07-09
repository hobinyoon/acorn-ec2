#!/usr/bin/env python

import os
import sys

sys.path.insert(0, "%s/batch-processing" % os.path.dirname(__file__))
import InstMonitor

def main(argv):
	im = InstMonitor.IM()
	im.RunOnce()


if __name__ == "__main__":
	sys.exit(main(sys.argv))

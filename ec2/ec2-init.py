#!/usr/bin/env python

import datetime
import getpass
import os
import sys

sys.path.insert(0, "../util/python")
import Cons
import Util


def main(argv):

	# TODO: Under what uid this script is run?

	Util.RunSubp("touch /tmp/%s" % getpass.getuser())

	Util.RunSubp("sudo mkdir -p /var/log/acorn")

	# TODO: I hope that the current user is ubuntu. If not, figure out how to run
	# this under 'ubuntu'
	Util.RunSubp("sudo chown %s /var/log/acorn" % getpass.getuser())

#	fn = "/var/log/acorn/ec2-init.log"
#	with open(fn, "w") as fo:
#		fo.write("%s: started\n" % datetime.datetime.now().strftime("%y%m%d-%H%M%S"))

	# TODO: Do something based on the instance's tags

	# Attach local SSD volumes
	#fn = "%s/ec2-init.log" % (os.path.expanduser("~"), datetime.datetime.now().strftime("%y%m%d-%H%M%S"))


if __name__ == "__main__":
	sys.exit(main(sys.argv))

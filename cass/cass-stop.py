#!/usr/bin/env python

import os
import re
import sys
import time

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Util


def _Kill():
	out = Util.RunSubp("ps -ef | grep org.apache.cassandra.service.CassandraDaemo[n] || true"
			, shell = True, print_cmd = False, print_result = False)
	if len(out) == 0:
		print "No cassandra process to kill"
		return

	t = re.split(" +", out)
	#print t
	pid = t[1]
	sys.stdout.write("killing %s " % pid)
	sys.stdout.flush()
	Util.RunSubp("kill %s" % pid, print_cmd = False, print_result = False)

	# wait for the process to be gone
	while True:
		out = Util.RunSubp("ps -ef | grep org.apache.cassandra.service.CassandraDaemo[n] || true"
				, shell = True, print_cmd = False, print_result = False)
		if len(out) == 0:
			break
		sys.stdout.write(".")
		sys.stdout.flush()
		time.sleep(0.1)
	print ""


def main(argv):
	_Kill()


if __name__ == "__main__":
	sys.exit(main(sys.argv))

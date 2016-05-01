#!/usr/bin/env python

import datetime
import os
import sys
import traceback

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util


_fo_log = None


def _Log(msg):
	fn = "/var/log/acorn/ec2-init.log"
	global _fo_log
	if _fo_log == None:
		_fo_log = open(fn, "a")
	_fo_log.write("%s: %s\n" % (datetime.datetime.now().strftime("%y%m%d-%H%M%S"), msg))
	_fo_log.flush()


def _RunSubp(cmd, shell = False):
	_Log(cmd)
	r = Util.RunSubp(cmd, shell = shell, print_cmd = False, print_result = False)
	if len(r.strip()) > 0:
		_Log(Util.Indent(r, 2))


def _MountAndFormatLocalSSDs():
	# Make sure we are using the known machine types
	inst_type = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-type", print_cmd = False, print_result = False)
	if inst_type not in ["c3.xlarge"]:
		raise RuntimeError("Unexpected instance type %s" % inst_type)

	ssds = ["ssd0", "ssd1"]
	devs = ["xvdb", "xvdc"]

	for i in range(2):
		_Log("Setting up Local %s ..." % ssds[i])
		_RunSubp("sudo umount /dev/%s || true" % devs[i], shell=True)
		_RunSubp("sudo mkdir -p /mnt/local-%s" % ssds[i])

		# Instance store volumes come TRIMmed when they are allocated. Without
		# nodiscard, it takes about 80 secs for a 800GB SSD.
		_RunSubp("sudo mkfs.ext4 -m 0 -E nodiscard -L local-%s /dev/%s" % (ssds[i], devs[i]), shell=True)

		# -o discard for TRIM
		_RunSubp("sudo mount -t ext4 -o discard /dev/%s /mnt/local-%s" % (devs[i], ssds[i]), shell=True)
		_RunSubp("sudo chown -R ubuntu /mnt/local-%s" % ssds[i], shell=True)


def main(argv):
	try:
		# This script is run under the user 'ubuntu'.
		_MountAndFormatLocalSSDs()

	except RuntimeError as e:
		msg = "Exception: %s\n%s" % (e, traceback.format_exc())
		_Log(msg)
		Cons.P(msg)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

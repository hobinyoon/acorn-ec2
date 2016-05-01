#!/usr/bin/env python

import boto3
import datetime
import getpass
import os
import pprint
import sys
import traceback

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util


_inst_id = None
_tag_name = None

def _LogInstInfo():
	ami_id  = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/ami-id", print_cmd = False, print_result = False)
	global _inst_id
	_inst_id = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-id", print_cmd = False, print_result = False)
	inst_type = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-type", print_cmd = False, print_result = False)
	az = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone", print_cmd = False, print_result = False)
	pub_ip = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/public-ipv4", print_cmd = False, print_result = False)
	local_ip = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/local-ipv4", print_cmd = False, print_result = False)

	_Log("ami_id:    %s" % ami_id)
	_Log("inst_id:   %s" % _inst_id)
	_Log("inst_type: %s" % inst_type)
	_Log("az:        %s" % az)
	_Log("pub_ip:    %s" % pub_ip)
	_Log("local_ip:  %s" % local_ip)


_fo_log = None


def _Log(msg):
	fn = "/var/log/acorn/ec2-init.log"
	global _fo_log
	# Possible race. Okay for single threaded.
	if _fo_log == None:
		_fo_log = open(fn, "a")
	_fo_log.write("%s: %s\n" % (datetime.datetime.now().strftime("%y%m%d-%H%M%S"), msg))
	_fo_log.flush()


def _RunInitByTags():
	boto_client = boto3.client("ec2")

	r = boto_client.describe_tags()
	#Cons.P(pprint.pformat(r, indent=2, width=100))
	for r0 in r["Tags"]:
		res_id = r0["ResourceId"]
		if _inst_id != res_id:
			continue
		if _inst_id == res_id:
			key = r0["Key"]
			value = r0["Value"]
			#_Log("tags key   : %s" % key)
			#_Log("     value : %s" % value)

			if key == "Name":
				global _tag_name
				_tag_name = value
			_Log("tag:Name=%s" % _tag_name)

			fn_cmd = "%s/ec2-init.d/%s.py" % (os.path.dirname(os.path.realpath(__file__)), _tag_name)
			_Log("Running %s" % fn_cmd)
			Util.RunSubp(fn_cmd, print_cmd = False, print_result = False)
			break


def main(argv):
	try:
		# This script is run under the user 'ubuntu'.
		#Util.RunSubp("touch /tmp/%s" % getpass.getuser())

		Util.RunSubp("sudo mkdir -p /var/log/acorn")
		Util.RunSubp("sudo chown %s /var/log/acorn" % getpass.getuser())

		_Log("started")
		_LogInstInfo()
		_RunInitByTags()
	except RuntimeError as e:
		msg = "Exception: %s\n%s" % (e, traceback.format_exc())
		_Log(msg)
		Cons.P(msg)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

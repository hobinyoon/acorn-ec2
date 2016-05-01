#!/usr/bin/env python

import boto3
import datetime
import getpass
import os
import pprint
import sys
import traceback

sys.path.insert(0, "../util/python")
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
	_fo_log.write("%s: %s\n" % (datetime.datetime.now().strftime("%y%m%d-%H%M%S"), msg))


def _GetTags():
	boto_client = boto3.client("ec2")

	# Not sure if this is for the current (API-calling) instance.
	r = boto_client.describe_tags()
	#Cons.P(pprint.pformat(r, indent=2, width=100))
	r0 = r["Tags"][0]
	key = r0["Key"]
	res_id = r0["ResourceId"]
	value = r0["Value"]
	if _inst_id != res_id:
		raise RuntimeError("res_id=%s _inst_id=%s" % (res_id, _inst_id))

	#_Log("tags key   : %s" % key)
	#_Log("     value : %s" % value)

	if key == "Name":
		global _tag_name
		_tag_name = value
	_Log("tag:Name=%s" % _tag_name)


def _RunInitByTags():
	# TODO
	pass

def main(argv):
	try:
		# This script is run under the user 'ubuntu'.
		#Util.RunSubp("touch /tmp/%s" % getpass.getuser())

		Util.RunSubp("sudo mkdir -p /var/log/acorn")
		Util.RunSubp("sudo chown %s /var/log/acorn" % getpass.getuser())

		fn = "/var/log/acorn/ec2-init.log"
		global _fo_log
		_fo_log = open(fn, "a")
		_Log("started")
		_LogInstInfo()
		_GetTags()
		_RunInitByTags()
	except RuntimeError as e:
		msg = "Exception: %s\n%s" % (e, traceback.format_exc())
		_Log(msg)
		Cons.P(msg)

	# TODO: Do something based on the instance's tags

	# Attach local SSD volumes
	#fn = "%s/ec2-init.log" % (os.path.expanduser("~"), datetime.datetime.now().strftime("%y%m%d-%H%M%S"))


if __name__ == "__main__":
	sys.exit(main(sys.argv))

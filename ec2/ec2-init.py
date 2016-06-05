#!/usr/bin/env python

import boto3
import datetime
import getpass
import os
import pprint
import re
import sys
import traceback

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util


_inst_id = None
_region = None

def _LogInstInfo():
	ami_id    = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/ami-id", print_cmd = False, print_result = False)
	global _inst_id
	_inst_id  = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-id", print_cmd = False, print_result = False)
	inst_type = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-type", print_cmd = False, print_result = False)
	az        = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone", print_cmd = False, print_result = False)
	pub_ip    = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/public-ipv4", print_cmd = False, print_result = False)
	local_ip  = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/local-ipv4", print_cmd = False, print_result = False)

	# http://stackoverflow.com/questions/4249488/find-region-from-within-ec2-instance
	doc       = Util.RunSubp("curl -s http://169.254.169.254/latest/dynamic/instance-identity/document", print_cmd = False, print_result = False)
	for line in doc.split("\n"):
		# "region" : "us-west-1"
		tokens = filter(None, re.split(":| |,|\"", line))
		#Cons.P(tokens)
		if len(tokens) == 2 and tokens[0] == "region":
			global _region
			_region = tokens[1]
			break

	_Log("ami_id:    %s" % ami_id)
	_Log("inst_id:   %s" % _inst_id)
	_Log("inst_type: %s" % inst_type)
	_Log("az:        %s" % az)
	_Log("pub_ip:    %s" % pub_ip)
	_Log("local_ip:  %s" % local_ip)
	_Log("region:    %s" % _region)


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
	boto_client = boto3.session.Session().client("ec2", region_name=_region)
	r = boto_client.describe_tags()
	#Cons.P(pprint.pformat(r, indent=2, width=100))
	tags = {}
	for r0 in r["Tags"]:
		res_id = r0["ResourceId"]
		if _inst_id != res_id:
			continue
		if _inst_id == res_id:
			k = r0["Key"]
			v = r0["Value"]
			#_Log("tags key   : %s" % k)
			#_Log("     value : %s" % v)
			tags[k] = v
	_Log("tags: %s" % tags)

	# Stingizing is not necessary, but useful for tesing the init script
	# separately.
	tags_str = ",".join(["%s:%s" % (k, v) for (k, v) in tags.items()])

	fn_cmd = "%s/ec2-init.d/%s.py %s %s" \
			% (os.path.dirname(os.path.realpath(__file__))
					, _fn_init_script, _job_id, _jr_sqs_url, _jr_sqs_msg_receipt_handle, tags_str)
	_Log("Running %s" % fn_cmd)
	Util.RunSubp(fn_cmd, shell = True, print_cmd = False, print_result = False)


_fn_init_script = None
_job_id = None
_jr_sqs_url = None
_jr_sqs_msg_receipt_handle = None

def main(argv):
	try:
		# This script is run under the user 'ubuntu'.
		#Util.RunSubp("touch /tmp/%s" % getpass.getuser())

		if len(argv) != 5:
			print "Usage: %s init_script job_id jr_sqs_url jr_sqs_msg_receipt_handle" % argv[0]
			print "  E.g.: %s acorn-server 160605-1519 None None" % argv[0]
			print "        The two Nones are for testing purposes."
			sys.exit(1)

		global _fn_init_script
		global _job_id
		global _jr_sqs_url
		global _jr_sqs_msg_receipt_handle
		_fn_init_script = argv[1]
		_job_id = argv[2]
		_jr_sqs_url = argv[3]
		_jr_sqs_msg_receipt_handle = argv[4]

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

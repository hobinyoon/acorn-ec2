#!/usr/bin/env python

import boto3
import datetime
import getpass
import imp
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
	_Log("_fn_init_script           : %s" % _fn_init_script)
	_Log("_jr_sqs_url               : %s" % _jr_sqs_url)
	_Log("_jr_sqs_msg_receipt_handle: %s" % _jr_sqs_msg_receipt_handle)

	boto_client = boto3.session.Session().client("ec2", region_name=_region)
	r = boto_client.describe_tags()
	#Cons.P(pprint.pformat(r, indent=2, width=100))
	tags = {}
	for r0 in r["Tags"]:
		res_id = r0["ResourceId"]
		if _inst_id != res_id:
			continue
		if _inst_id == res_id:
			tags[r0["Key"]] = r0["Value"]
	_Log("tags:\n%s" % "\n".join(["  %s:%s" % (k, v) for (k, v) in sorted(tags.items())]))

	fn_module = "%s/ec2-init.d/%s.py" % (os.path.dirname(__file__), _fn_init_script)
	mod_name,file_ext = os.path.splitext(os.path.split(fn_module)[-1])
	if file_ext.lower() != '.py':
		raise RuntimeError("Unexpected file_ext: %s" % file_ext)
	py_mod = imp.load_source(mod_name, fn_module)
	getattr(py_mod, "main")(_jr_sqs_url, _jr_sqs_msg_receipt_handle, tags)


_fn_init_script = None
_jr_sqs_url = None
_jr_sqs_msg_receipt_handle = None

def main(argv):
	try:
		# This script is run under the user 'ubuntu'.
		#Util.RunSubp("touch /tmp/%s" % getpass.getuser())

		if len(argv) != 4:
			raise RuntimeError("Usage: %s init_script jr_sqs_url jr_sqs_msg_receipt_handle\n"
					"  E.g.: %s acorn-server None None\n"
					"        The two Nones are for testing purposes."
					% (argv[0], argv[0]))

		global _fn_init_script
		global _jr_sqs_url
		global _jr_sqs_msg_receipt_handle
		_fn_init_script = argv[1]
		_jr_sqs_url = argv[2]
		_jr_sqs_msg_receipt_handle = argv[3]

		Util.RunSubp("sudo mkdir -p /var/log/acorn")
		Util.RunSubp("sudo chown %s /var/log/acorn" % getpass.getuser())

		_Log("started")
		_LogInstInfo()
		_RunInitByTags()
	except Exception as e:
		msg = "Exception: %s\n%s" % (e, traceback.format_exc())
		_Log(msg)
		Cons.P(msg)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

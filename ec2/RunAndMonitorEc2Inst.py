import boto3
# Boto: http://boto3.readthedocs.org/en/latest/
import datetime
import os
import pprint
import re
import sys
import threading
import time

sys.path.insert(0, "%s/../util/python" % os.path.dirname(__file__))
import Cons
import Util

import Ec2Util


_threads = []
_dn_tmp = "%s/../.tmp" % os.path.dirname(__file__)
_job_id = None

_ec2_type = None
_tags = None
_jr_sqs_url = None
_jr_sqs_msg_receipt_handle = None
_init_script = None


def Run(regions, ec2_type, tags, jr_sqs_url, jr_sqs_msg_receipt_handle, init_script):
	Reset()

	Util.RunSubp("mkdir -p %s" % _dn_tmp, print_cmd = False)

	req_datetime = datetime.datetime.now()
	global _job_id
	_job_id = req_datetime.strftime("%y%m%d-%H%M%S")
	Cons.P("Job ID: %s (used for describing or terminating the cluster)" % _job_id)

	global _ec2_type, _tags, _jr_sqs_url, _jr_sqs_msg_receipt_handle, _init_script
	_ec2_type = ec2_type
	_tags = tags
	_tags["job_id"] = _job_id
	_jr_sqs_url = jr_sqs_url
	_jr_sqs_msg_receipt_handle = jr_sqs_msg_receipt_handle
	_init_script = init_script

	rams = []
	for r in regions:
		rams.append(RunAndMonitor(r))

	for ram in rams:
		t = threading.Thread(target=ram.RunEc2Inst)
		_threads.append(t)
		t.start()

	InstLaunchProgMon.Run()

	for t in _threads:
		t.join()


# This module can be called repeatedly
def Reset():
	global _threads, _job_id
	global _ec2_type, _tags, _jr_sqs_url, _jr_sqs_msg_receipt_handle, _init_script

	_threads = []
	_job_id = None

	_ec2_type = None
	_tags = None
	_jr_sqs_url = None
	_jr_sqs_msg_receipt_handle = None
	_init_script = None

	InstLaunchProgMon.Reset()


class RunAndMonitor():
	def __init__(self, az_or_region):
		if re.match(r".*[a-z]$", az_or_region):
			self.az = az_or_region
			self.region_name = self.az[:-1]
		else:
			self.az = None
			self.region_name = az_or_region
		self.ami_id = Ec2Util.GetLatestAmiId(self.region_name)


	def RunEc2Inst(self):
		# This is run as root
		user_data = \
"""#!/bin/bash
cd /home/ubuntu/work
rm -rf /home/ubuntu/work/acorn-tools
sudo -i -u ubuntu bash -c 'git clone https://github.com/hobinyoon/acorn-tools.git /home/ubuntu/work/acorn-tools'
sudo -i -u ubuntu /home/ubuntu/work/acorn-tools/ec2/ec2-init.py {0} {1} {2}
"""
		user_data = user_data.format(_init_script, _jr_sqs_url, _jr_sqs_msg_receipt_handle)

#cd /home/ubuntu/work/acorn-tools
#sudo -u ubuntu bash -c 'git pull'
# http://unix.stackexchange.com/questions/4342/how-do-i-get-sudo-u-user-to-use-the-users-env

		self.boto_client = boto3.session.Session().client("ec2", region_name = self.region_name)

		placement = {}
		if self.az != None:
			placement['AvailabilityZone'] = self.az

		response = self.boto_client.run_instances(
				DryRun = False
				, ImageId = self.ami_id
				, MinCount=1
				, MaxCount=1
				, SecurityGroups=["cass-server"]
				, EbsOptimized=True
				, InstanceType = _ec2_type
				, Placement=placement

				# User data is passed as a string. I don't see an option of specifying a file.
				, UserData=user_data

				, InstanceInitiatedShutdownBehavior='terminate'
				)
		#ConsP("Response:")
		#ConsP(Util.Indent(pprint.pformat(response, indent=2, width=100), 2))
		if len(response["Instances"]) != 1:
			raise RuntimeError("len(response[\"Instances\"])=%d" % len(response["Instances"]))
		self.inst_id = response["Instances"][0]["InstanceId"]
		#ConsP("region=%s inst_id=%s" % (self.region_name, self.inst_id))
		InstLaunchProgMon.SetRegion(self.inst_id, self.region_name)

		self._KeepCheckingInst()


	def _KeepCheckingInst(self):
		state = None
		tagged = False

		while True:
			response = self.boto_client.describe_instances(InstanceIds=[self.inst_id])
			# Note: describe_instances() returns StateReason, while
			# describe_instance_status() doesn't.

			InstLaunchProgMon.Update(self.inst_id, response)
			state = response["Reservations"][0]["Instances"][0]["State"]["Name"]
			# Create a tag
			if state == "pending" and tagged == False:
				tags_boto = []
				for k, v in _tags.iteritems():
					tags_boto.append({"Key": k, "Value": v})
					#ConsP("[%s]=[%s]" %(k, v))

				self.boto_client.create_tags(Resources = [self.inst_id], Tags = tags_boto)
				tagged = True

			elif state == "terminated" or state == "running":
				break
			time.sleep(1)

		# Make sure everything is ok.
		if state == "running":
			response = self.boto_client.describe_instances(InstanceIds=[self.inst_id])
			state = response["Reservations"][0]["Instances"][0]["State"]["Name"]
			InstLaunchProgMon.Update(self.inst_id, response)

			# With composite parametes it's not easy to make one of these any more.
			# Make (region-acorn_exp_param) to ipaddr files
			#fn = "%s/%s-%s" % (_dn_tmp, self.region_name, _tags["acorn_exp_param"])
			#with open(fn, "w") as fo:
			#	fo.write(response["Reservations"][0]["Instances"][0]["PublicIpAddress"])


class InstLaunchProgMon():
	progress = {}
	progress_lock = threading.Lock()

	class Entry():
		def __init__(self, region):
			self.region = region
			self.responses = []

		def AddResponse(self, response):
			self.responses.append(response)

	@staticmethod
	def Reset():
		InstLaunchProgMon.progress = {}
		InstLaunchProgMon.progress_lock = threading.Lock()

	@staticmethod
	def SetRegion(inst_id, region_name):
		with InstLaunchProgMon.progress_lock:
			InstLaunchProgMon.progress[inst_id] = InstLaunchProgMon.Entry(region_name)

	@staticmethod
	def Update(inst_id, response):
		with InstLaunchProgMon.progress_lock:
			InstLaunchProgMon.progress[inst_id].AddResponse(response)

	@staticmethod
	def Run():
		output_lines_written = 0
		while True:
			output = ""
			for k, v in InstLaunchProgMon.progress.iteritems():
				if len(output) > 0:
					output += "\n"
				inst_id = k
				output += ("%-15s %s" % (v.region, inst_id))
				prev_state = None
				same_state_cnt = 0
				for r in v.responses:
					state = r["Reservations"][0]["Instances"][0]["State"]["Name"]
					if state == "shutting-down":
						state_reason = response["Reservations"][0]["Instances"][0]["StateReason"]["Message"]
						state = "%s:%s" % (state, state_reason)

					if prev_state == None:
						output += (" %s" % state)
					elif prev_state != state:
						if same_state_cnt > 0:
							output += (" x%d %s" % ((same_state_cnt + 1), state))
						else:
							output += (" %s" % state)
						same_state_cnt = 0
					else:
						same_state_cnt += 1
					prev_state = state

				if same_state_cnt > 0:
					output += (" x%d" % (same_state_cnt + 1))

			if output_lines_written > 0:
				for l in range(output_lines_written - 1):
					# Clear current line
					sys.stdout.write(chr(27) + "[2K")
					# Move up
					sys.stdout.write(chr(27) + "[1F")
				# Clear current line
				sys.stdout.write(chr(27) + "[2K")
				# Move the cursor to column 1
				sys.stdout.write(chr(27) + "[1G")

			#sys.stdout.write(output)
			# Sort them
			sys.stdout.write("\n".join(sorted(output.split("\n"))))
			sys.stdout.flush()
			output_lines_written = len(output.split("\n"))

			# Are we done?
			all_done = True
			for t in _threads:
				if t.is_alive():
					all_done = False
					break
			if all_done:
				break

			time.sleep(0.1)
		print ""
		print ""

		InstLaunchProgMon.DescInsts()

	@staticmethod
	def DescInsts():
		fmt = "%-15s %10s %10s %13s %15s %15s %10s %-20s"
		ConsP(Util.BuildHeader(fmt,
			"Placement:AvailabilityZone"
			" InstanceId"
			" InstanceType"
			" LaunchTime"
			" PrivateIpAddress"
			" PublicIpAddress"
			" State:Name"
			" Tags"
			))

		for k, v in InstLaunchProgMon.progress.iteritems():
			r = v.responses[-1]["Reservations"][0]["Instances"][0]

			tags = {}
			if "Tags" in r:
				for t in r["Tags"]:
					tags[t["Key"]] = t["Value"]

			#ConsP(Util.Indent(pprint.pformat(r, indent=2, width=100), 2))
			ConsP(fmt % (
				_Value(_Value(r, "Placement"), "AvailabilityZone")
				, _Value(r, "InstanceId")
				, _Value(r, "InstanceType")
				, _Value(r, "LaunchTime").strftime("%y%m%d-%H%M%S")
				, _Value(r, "PrivateIpAddress")
				, _Value(r, "PublicIpAddress")
				, _Value(_Value(r, "State"), "Name")
				, ",".join(["%s:%s" % (k, v) for (k, v) in sorted(tags.items())])
				))


_print_lock = threading.Lock()

def ConsP(msg):
	with _print_lock:
		Cons.P(msg)


def _Value(dict_, key):
	if key == "":
		return ""

	if key in dict_:
		return dict_[key]
	else:
		return ""


#def _RunEc2InstR3XlargeEbs():
#	response = boto_client.run_instances(
#			DryRun = False
#			, ImageId = "ami-1fc7d575"
#			, MinCount=1
#			, MaxCount=1
#			, SecurityGroups=["cass-server"]
#			, EbsOptimized=True
#			, InstanceType="r3.xlarge"
#			, BlockDeviceMappings=[
#				{
#					'DeviceName': '/dev/sdc',
#					'Ebs': {
#						'VolumeSize': 16384,
#						'DeleteOnTermination': True,
#						'VolumeType': 'gp2',
#						'Encrypted': False
#						},
#					},
#				],
#			)
#
#			# What's the defalt value, when not specified? Might be True. I see the
#			# Basic CloudWatch monitoring on the web console.
#			# Monitoring={
#			#     'Enabled': True|False
#			# },
#			#
#			# "stop" when not specified.
#			#   InstanceInitiatedShutdownBehavior='stop'|'terminate',
#	ConsP("Response:")
#	ConsP(Util.Indent(pprint.pformat(response, indent=2, width=100), 2))
#	if len(response["Instances"]) != 1:
#		raise RuntimeError("len(response[\"Instances\"])=%d" % len(response["Instances"]))
#	inst_id = response["Instances"][0]["InstanceId"]
#	return inst_id

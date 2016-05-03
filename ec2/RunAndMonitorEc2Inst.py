import boto3
# Boto: http://boto3.readthedocs.org/en/latest/
import os
import pprint
import sys
import threading
import time

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util


_threads = []
_dn_tmp = "%s/../.tmp" % os.path.dirname(os.path.realpath(__file__))


def Run(regions = ["us-east-1"], tag_name = None):
	Util.RunSubp("mkdir -p %s" % _dn_tmp, print_cmd = False)

	rams = []
	for r in regions:
		rams.append(RunAndMonitor(r, tag_name))

	for ram in rams:
		t = threading.Thread(target=ram.RunEc2Inst)
		_threads.append(t)
		t.start()

	InstLaunchProgMon.Run()

	for t in _threads:
		t.join()


class RunAndMonitor():
	def __init__(self, region_name, tag_name):
		if region_name == "us-east-1":
			self.ami_id = "ami-4766872a"
		elif region_name == "us-west-1":
			self.ami_id = "ami-6c21580c"
		else:
			raise RuntimeError("Unexpected region %s" % region_name)
		self.region_name = region_name
		self.tag_name = tag_name


	def RunEc2Inst(self):
		# This is run as root
		init_script = \
"""#!/bin/bash
cd /home/ubuntu/work
rm -rf /home/ubuntu/work/acorn-tools
sudo -i -u ubuntu bash -c 'git clone https://github.com/hobinyoon/acorn-tools.git /home/ubuntu/work/acorn-tools'
sudo -i -u ubuntu /home/ubuntu/work/acorn-tools/ec2/ec2-init.py
"""
#cd /home/ubuntu/work/acorn-tools
#sudo -u ubuntu bash -c 'git pull'
# http://unix.stackexchange.com/questions/4342/how-do-i-get-sudo-u-user-to-use-the-users-env

		self.boto_client = boto3.session.Session().client("ec2", region_name = self.region_name)
		response = self.boto_client.run_instances(
				DryRun = False
				, ImageId = self.ami_id
				, MinCount=1
				, MaxCount=1
				, SecurityGroups=["cass-server"]
				, EbsOptimized=True
				# 4 vCPUs, 7.5 Gib RAM, EBS only, $0.209 per Hour
				# "The specified instance type can only be used in a VPC. A subnet ID or network interface ID is required to carry out the request."
				#, InstanceType="c4.xlarge"

				# 4 vCPUs, 7.5 Gib RAM, 2 x 40 SSD, $0.21 per Hour
				#, InstanceType="c3.xlarge"

				# For fast development
				, InstanceType="c3.4xlarge"

				# AZ doesn't need to be specified. self.az = "us-west-1c"
				#, Placement={'AvailabilityZone': self.az}

				# I don't see a user data file. Just string.
				, UserData=init_script
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
		while True:
			response = self.boto_client.describe_instances(InstanceIds=[self.inst_id])
			# Note: describe_instances() returns StateReason, while
			# describe_instance_status() doesn't.

			state = response["Reservations"][0]["Instances"][0]["State"]["Name"]
			InstLaunchProgMon.Update(self.inst_id, response)
			if state == "terminated" or state == "running":
				break
			time.sleep(1)

		# Create a tag and check one more time to make sure the tag is in place.
		if state == "running":
			self.boto_client.create_tags(
					Resources = [self.inst_id],
					Tags = [{
						"Key": "Name",
						"Value": self.tag_name
						}]
					)

			response = self.boto_client.describe_instances(InstanceIds=[self.inst_id])
			state = response["Reservations"][0]["Instances"][0]["State"]["Name"]
			InstLaunchProgMon.Update(self.inst_id, response)

			# Make region-ipaddr files
			fn = "%s/%s" % (_dn_tmp, self.region_name)
			with open(fn, "w") as fo:
				fo.write(response["Reservations"][0]["Instances"][0]["PublicIpAddress"])


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
				output += ("%s %s" % (v.region, inst_id))
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

			sys.stdout.write(output)
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
		fmt = "%10s %10s %10s %13s %15s %15s %10s %20s"
		ConsP(Util.BuildHeader(fmt,
			"Placement:AvailabilityZone"
			" InstanceId"
			" InstanceType"
			" LaunchTime"
			" PrivateIpAddress"
			" PublicIpAddress"
			" State:Name"
			" Tag:Name"
			))

		for k, v in InstLaunchProgMon.progress.iteritems():
			r = v.responses[-1]["Reservations"][0]["Instances"][0]

			tag_name = None
			if "Tags" in r:
				for t in r["Tags"]:
					if t["Key"] == "Name":
						tag_name = t["Value"]

			#ConsP(Util.Indent(pprint.pformat(r, indent=2, width=100), 2))
			ConsP(fmt % (
				_Value(_Value(r, "Placement"), "AvailabilityZone")
				, _Value(r, "InstanceId")
				, _Value(r, "InstanceType")
				, _Value(r, "LaunchTime").strftime("%y%m%d-%H%M%S")
				, _Value(r, "PrivateIpAddress")
				, _Value(r, "PublicIpAddress")
				, _Value(_Value(r, "State"), "Name")
				, tag_name
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

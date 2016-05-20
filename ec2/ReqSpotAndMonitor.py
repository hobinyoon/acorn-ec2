import base64
import boto3
# Boto: http://boto3.readthedocs.org/en/latest/
import os
import pprint
import re
import sys
import threading
import time

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util

import Ec2Util


_threads = []
_dn_tmp = "%s/../.tmp" % os.path.dirname(os.path.realpath(__file__))


def Run(regions = ["us-east-1"], tag_name = None, ec2_type = None, price = None):
	if price == None:
		raise RuntimeError("Need a price")

	Util.RunSubp("mkdir -p %s" % _dn_tmp, print_cmd = False)

	rams = []
	for r in regions:
		rams.append(ReqAndMonitor(r, tag_name, ec2_type, price))

	for ram in rams:
		t = threading.Thread(target=ram.Run)
		_threads.append(t)
		t.start()

	InstLaunchProgMon.Run()

	for t in _threads:
		t.join()


class ReqAndMonitor():
	def __init__(self, az_or_region, tag_name, ec2_type, price):
		if re.match(r".*[a-z]$", az_or_region):
			self.az = az_or_region
			self.region_name = self.az[:-1]
		else:
			self.az = None
			self.region_name = az_or_region
		self.ami_id = Ec2Util.GetLatestAmiId(self.region_name)

		self.tag_name = tag_name
		self.ec2_type = ec2_type
		self.price = price

		self.inst_id = None


	def Run(self):
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

		ls = {'ImageId': self.ami_id,
				#'KeyName': 'string',
				'SecurityGroups': ["cass-server"],
				'UserData': base64.b64encode(init_script),
				#'AddressingType': 'string',
				'InstanceType': self.ec2_type,
				'EbsOptimized': True,
				}
		if self.az != None:
			ls['Placement'] = {}
			ls['Placement']['AvailabilityZone'] = self.az

		response = self.boto_client.request_spot_instances(
				SpotPrice=self.price,
				#ClientToken='string',
				InstanceCount=1,
				Type='one-time',
				#ValidFrom=datetime(2015, 1, 1),
				#ValidUntil=datetime(2015, 1, 1),
				#LaunchGroup='string',
				#AvailabilityZoneGroup='string',

				# https://aws.amazon.com/blogs/aws/new-ec2-spot-blocks-for-defined-duration-workloads/
				#BlockDurationMinutes=123,

				LaunchSpecification = ls,
				)
		#ConsP("Response:")
		#ConsP(Util.Indent(pprint.pformat(response, indent=2, width=100), 2))

		if len(response["SpotInstanceRequests"]) != 1:
			raise RuntimeError("len(response[\"SpotInstanceRequests\"])=%d" % len(response["SpotInstanceRequests"]))
		self.spot_req_id = response["SpotInstanceRequests"][0]["SpotInstanceRequestId"]
		#ConsP("region=%s spot_req_id=%s" % (self.region_name, self.spot_req_id))

		InstLaunchProgMon.SetRegion(self.spot_req_id, self.region_name)

		self._KeepCheckingSpotReq()
		self._KeepCheckingInst()


	def _KeepCheckingSpotReq(self):
		response = None
		while True:
			response = self.boto_client.describe_spot_instance_requests(
					SpotInstanceRequestIds=[self.spot_req_id])
			if len(response["SpotInstanceRequests"]) != 1:
				raise RuntimeError("len(response[\"SpotInstanceRequests\"])=%d" % len(response["SpotInstanceRequests"]))
			#ConsP(Util.Indent(pprint.pformat(response, indent=2, width=100), 2))

			InstLaunchProgMon.UpdateSpotReq(self.spot_req_id, response)
			status_code = response["SpotInstanceRequests"][0]["Status"]["Code"]
			if status_code == "fulfilled":
				break
			time.sleep(1)

		# Get inst_id
		#ConsP(Util.Indent(pprint.pformat(response, indent=2, width=100), 2))
		self.inst_id = response["SpotInstanceRequests"][0]["InstanceId"]
		InstLaunchProgMon.SetInstID(self.spot_req_id, self.inst_id)

		# Note: may want to show the current pricing


	def _KeepCheckingInst(self):
		if self.inst_id == None:
			return

		state = None
		tagged = False

		while True:
			response = self.boto_client.describe_instances(InstanceIds=[self.inst_id])
			# Note: describe_instances() returns StateReason, while
			# describe_instance_status() doesn't.

			InstLaunchProgMon.UpdateDescInst(self.spot_req_id, response)
			state = response["Reservations"][0]["Instances"][0]["State"]["Name"]
			# Create a tag
			if state == "pending" and tagged == False:
				self.boto_client.create_tags(
						Resources = [self.inst_id],
						Tags = [{
							"Key": "Name",
							"Value": self.tag_name
							}]
						)
				tagged = True

			elif state == "terminated" or state == "running":
				break
			time.sleep(1)

		# Make sure everything is ok.
		if state == "running":
			response = self.boto_client.describe_instances(InstanceIds=[self.inst_id])
			state = response["Reservations"][0]["Instances"][0]["State"]["Name"]
			InstLaunchProgMon.UpdateDescInst(self.spot_req_id, response)

			# Make region-ipaddr files
			fn = "%s/%s" % (_dn_tmp, self.region_name)
			with open(fn, "w") as fo:
				fo.write(response["Reservations"][0]["Instances"][0]["PublicIpAddress"])


class InstLaunchProgMon():
	# Note: making the key to region seems more natural for the output. Lowest priority.
	#
	# key: spot_req_id, value: Entry
	progress = {}
	progress_lock = threading.Lock()

	class Entry():
		def __init__(self, region):
			self.region = region
			self.resp_desc_spot_req = []
			self.inst_id = None
			self.resp_desc_inst = []

		def AddDescSpotReqResponse(self, r):
			self.resp_desc_spot_req.append(r)

		def AddDescInstResponse(self, r):
			self.resp_desc_inst.append(r)

		def SetInstID(self, inst_id):
			self.inst_id = inst_id

	@staticmethod
	def SetRegion(spot_req_id, region_name):
		with InstLaunchProgMon.progress_lock:
			InstLaunchProgMon.progress[spot_req_id] = InstLaunchProgMon.Entry(region_name)

	@staticmethod
	def UpdateSpotReq(spot_req_id, response):
		with InstLaunchProgMon.progress_lock:
			InstLaunchProgMon.progress[spot_req_id].AddDescSpotReqResponse(response)

	@staticmethod
	def SetInstID(spot_req_id, inst_id):
		with InstLaunchProgMon.progress_lock:
			InstLaunchProgMon.progress[spot_req_id].SetInstID(inst_id)

	@staticmethod
	def UpdateDescInst(spot_req_id, response):
		with InstLaunchProgMon.progress_lock:
			InstLaunchProgMon.progress[spot_req_id].AddDescInstResponse(response)

	@staticmethod
	def Run():
		output_lines_written = 0
		while True:
			output = ""
			for spot_req_id, v in InstLaunchProgMon.progress.iteritems():
				if len(output) > 0:
					output += "\n"
				output += ("%-15s %s" % (v.region, spot_req_id))

				# Spot req status
				prev_status = None
				same_status_cnt = 0
				for r in v.resp_desc_spot_req:
					status = r["SpotInstanceRequests"][0]["Status"]["Code"]

					if prev_status == None:
						output += (" %s" % status)
					elif prev_status != status:
						if same_status_cnt > 0:
							output += (" x%d %s" % ((same_status_cnt + 1), status))
						else:
							output += (" %s" % status)
						same_status_cnt = 0
					else:
						same_status_cnt += 1
					prev_status = status

				if same_status_cnt > 0:
					output += (" x%d" % (same_status_cnt + 1))

				# Inst state
				if v.inst_id != None:
					output += (" %s" % v.inst_id)
					prev_state = None
					same_state_cnt = 0
					for r in v.resp_desc_inst:
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

			# Clear prev output
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
		fmt = "%-15s %10s %10s %13s %15s %15s %10s %20s"
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

		for spot_req_id, v in InstLaunchProgMon.progress.iteritems():
			r = v.resp_desc_inst[-1]["Reservations"][0]["Instances"][0]

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

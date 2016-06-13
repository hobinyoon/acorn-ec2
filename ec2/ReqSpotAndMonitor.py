import base64
import boto3
import botocore
import datetime
import os
import pprint
import re
import sys
import threading
import time

sys.path.insert(0, "%s/../util/python" % os.path.dirname(__file__))
import ConsMt
import Util

import RegionToAmi


_threads = []
_dn_tmp = "%s/../.tmp" % os.path.dirname(__file__)
_job_id = None

_ec2_type = None
_tags = None
_jr_sqs_url = None
_jr_sqs_msg_receipt_handle = None
_init_script = None
_price = None


def Run(regions, ec2_type, tags, jr_sqs_url, jr_sqs_msg_receipt_handle, init_script, price = None):
	if price == None:
		raise RuntimeError("Need a price")

	Reset()

	# TODO: Update RunSubp
	Util.RunSubp("mkdir -p %s" % _dn_tmp, print_cmd = False)

	req_datetime = datetime.datetime.now()
	global _job_id
	_job_id = req_datetime.strftime("%y%m%d-%H%M%S")
	ConsMt.P("job_id:%s (for describing and terminating the cluster)" % _job_id)

	global _ec2_type, _tags, _jr_sqs_url, _jr_sqs_msg_receipt_handle, _init_script, _price
	_ec2_type = ec2_type
	_tags = tags
	_tags["job_id"] = _job_id
	_jr_sqs_url = jr_sqs_url
	_jr_sqs_msg_receipt_handle = jr_sqs_msg_receipt_handle
	_init_script = init_script
	_price = price

	rams = []
	for r in regions:
		rams.append(ReqAndMonitor(r))

	for ram in rams:
		t = threading.Thread(target=ram.Run)
		t.daemon = True
		_threads.append(t)
		t.start()

	InstLaunchProgMon.Run()

	for t in _threads:
		t.join()

	for ram in rams:
		if ram.exit_success == False:
			raise RuntimeError("ReqAndMonitor %s failed" % ram)


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


class ReqAndMonitor():
	def __init__(self, az_or_region):
		if re.match(r".*[a-z]$", az_or_region):
			self.az = az_or_region
			self.region_name = self.az[:-1]
		else:
			self.az = None
			self.region_name = az_or_region
		self.ami_id = RegionToAmi.GetLatestAmiId(self.region_name)

		self.inst_id = None

		self.exit_success = False


	def Run(self):
		try:
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

			ls = {'ImageId': self.ami_id,
					#'KeyName': 'string',
					'SecurityGroups': ["cass-server"],
					'UserData': base64.b64encode(user_data),
					#'AddressingType': 'string',
					'InstanceType': _ec2_type,
					'EbsOptimized': True,
					}
			if self.az != None:
				ls['Placement'] = {}
				ls['Placement']['AvailabilityZone'] = self.az

			response = self.boto_client.request_spot_instances(
					SpotPrice=str(_price),
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
			#ConsMt.P("Response:")
			#ConsMt.P(Util.Indent(pprint.pformat(response, indent=2, width=100), 2))

			if len(response["SpotInstanceRequests"]) != 1:
				raise RuntimeError("len(response[\"SpotInstanceRequests\"])=%d" % len(response["SpotInstanceRequests"]))
			self.spot_req_id = response["SpotInstanceRequests"][0]["SpotInstanceRequestId"]
			#ConsMt.P("region=%s spot_req_id=%s" % (self.region_name, self.spot_req_id))

			InstLaunchProgMon.SetRegion(self.spot_req_id, self.region_name)

			self._KeepCheckingSpotReq()
			self._KeepCheckingInst()

			self.exit_success = True
		except Exception as e:
			ConsMt.P(e)
			sys.exit(1)


	def _KeepCheckingSpotReq(self):
		response = None
		while True:
			response = self.boto_client.describe_spot_instance_requests(
					SpotInstanceRequestIds=[self.spot_req_id])
			if len(response["SpotInstanceRequests"]) != 1:
				raise RuntimeError("len(response[\"SpotInstanceRequests\"])=%d" % len(response["SpotInstanceRequests"]))
			#ConsMt.P(Util.Indent(pprint.pformat(response, indent=2, width=100), 2))

			InstLaunchProgMon.UpdateSpotReq(self.spot_req_id, response)
			status_code = response["SpotInstanceRequests"][0]["Status"]["Code"]
			if status_code == "fulfilled":
				break
			time.sleep(1)

		# Get inst_id
		#ConsMt.P(Util.Indent(pprint.pformat(response, indent=2, width=100), 2))
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
			# Create tags
			if state == "pending" and tagged == False:
				tags_boto = []
				for k, v in _tags.iteritems():
					tags_boto.append({"Key": k, "Value": v})
					#ConsMt.P("[%s]=[%s]" %(k, v))

				self.boto_client.create_tags(Resources = [self.inst_id], Tags = tags_boto)
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
	def Reset():
		with InstLaunchProgMon.progress_lock:
			InstLaunchProgMon.progress = {}

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
							output += (" x%2d %s" % ((same_status_cnt + 1), status))
						else:
							output += (" %s" % status)
						same_status_cnt = 0
					else:
						same_status_cnt += 1
					prev_status = status

				if same_status_cnt > 0:
					output += (" x%2d" % (same_status_cnt + 1))

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
								output += (" x%2d %s" % ((same_state_cnt + 1), state))
							else:
								output += (" %s" % state)
							same_state_cnt = 0
						else:
							same_state_cnt += 1
						prev_state = state

					if same_state_cnt > 0:
						output += (" x%2d" % (same_state_cnt + 1))

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

			# Update status every so often
			time.sleep(0.1)
		print ""

		InstLaunchProgMon.DescInsts()

	@staticmethod
	def DescInsts():
		fmt = "%-15s %10s %10s %13s %15s %10s"
		ConsMt.P(Util.BuildHeader(fmt,
			"Placement:AvailabilityZone"
			" InstanceId"
			" InstanceType"
			" LaunchTime"
			#" PrivateIpAddress"
			" PublicIpAddress"
			" State:Name"
			#" Tags"
			))

		for spot_req_id, v in InstLaunchProgMon.progress.iteritems():
			if len(v.resp_desc_inst) == 0:
				continue
			r = v.resp_desc_inst[-1]["Reservations"][0]["Instances"][0]

			tags = {}
			if "Tags" in r:
				for t in r["Tags"]:
					tags[t["Key"]] = t["Value"]

			#ConsMt.P(Util.Indent(pprint.pformat(r, indent=2, width=100), 2))
			ConsMt.P(fmt % (
				_Value(_Value(r, "Placement"), "AvailabilityZone")
				, _Value(r, "InstanceId")
				, _Value(r, "InstanceType")
				, _Value(r, "LaunchTime").strftime("%y%m%d-%H%M%S")
				#, _Value(r, "PrivateIpAddress")
				, _Value(r, "PublicIpAddress")
				, _Value(_Value(r, "State"), "Name")
				#, ",".join(["%s:%s" % (k, v) for (k, v) in sorted(tags.items())])
				))


def _Value(dict_, key):
	if key == "":
		return ""

	if key in dict_:
		return dict_[key]
	else:
		return ""

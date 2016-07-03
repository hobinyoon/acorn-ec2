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
import traceback

sys.path.insert(0, "%s/../util/python" % os.path.dirname(__file__))
import Cons
import Util

import Ec2Region


_threads = []
_dn_tmp = "%s/../.tmp" % os.path.dirname(__file__)
_job_id = None

_tags = None
_num_regions = None
_jr_sqs_url = None
_jr_sqs_msg_receipt_handle = None
_init_script = None


def Run(region_spot_req, tags, jr_sqs_url, jr_sqs_msg_receipt_handle, init_script):
	Reset()

	Util.RunSubp("mkdir -p %s" % _dn_tmp, print_cmd = False)

	req_datetime = datetime.datetime.now()
	global _job_id
	_job_id = req_datetime.strftime("%y%m%d-%H%M%S")
	Cons.P("job_id:%s (for describing and terminating the cluster)" % _job_id)

	global _tags, _num_regions, _jr_sqs_url, _jr_sqs_msg_receipt_handle, _init_script
	_tags = tags
	_tags["job_id"] = _job_id
	_num_regions = len(region_spot_req)
	_jr_sqs_url = jr_sqs_url
	_jr_sqs_msg_receipt_handle = jr_sqs_msg_receipt_handle
	_init_script = init_script

	rams = []
	for region, spot_req_params in region_spot_req.iteritems():
		rams.append(ReqAndMonitor(region, spot_req_params))

	for ram in rams:
		t = threading.Thread(target=ram.Run)
		t.daemon = True
		_threads.append(t)
		t.start()

	InstLaunchProgMon.Run()

	for t in _threads:
		t.join()


# This module can be called repeatedly
def Reset():
	global _threads, _job_id
	global _tags, _jr_sqs_url, _jr_sqs_msg_receipt_handle, _init_script

	_threads = []
	_job_id = None

	_tags = None
	_num_regions = None
	_jr_sqs_url = None
	_jr_sqs_msg_receipt_handle = None
	_init_script = None

	InstLaunchProgMon.Reset()


class ReqAndMonitor():
	def __init__(self, az_or_region, spot_req_params):
		if re.match(r".*[a-z]$", az_or_region):
			self.az = az_or_region
			self.region_name = self.az[:-1]
		else:
			self.az = None
			self.region_name = az_or_region
		self.ami_id = Ec2Region.GetLatestAmiId(self.region_name)

		self.inst_type = spot_req_params["inst_type"]
		self.max_price = spot_req_params["max_price"]

		self.inst_id = None


	def Run(self):
		try:
			self._ReqSpotInst()
			self._KeepCheckingSpotReq()
			self._KeepCheckingInst()
		except Exception as e:
			Cons.P("%s\nregion=%s\n%s" % (e, self.region_name, traceback.format_exc()))
			os._exit(1)


	# Note: InstMonitor would be a better place for this.
	def _GetPriceHistory(self):
		now = datetime.datetime.now()
		one_day_ago = now - datetime.timedelta(days=1)

		r = None
		if self.az is None:
			r = self.boto_client.describe_spot_price_history(
					StartTime = one_day_ago,
					EndTime = now,
					ProductDescriptions = ["Linux/UNIX"],
					InstanceTypes = [self.inst_type],
					)
		else:
			r = self.boto_client.describe_spot_price_history(
					StartTime = one_day_ago,
					EndTime = now,
					ProductDescriptions = ["Linux/UNIX"],
					InstanceTypes = [self.inst_type],
					AvailabilityZone = self.az
					)
		#Cons.P(pprint.pformat(r))

		# {az: {timestamp: price} }
		az_ts_price = {}
		for sp in r["SpotPriceHistory"]:
			az = sp["AvailabilityZone"]
			ts = sp["Timestamp"]
			sp = float(sp["SpotPrice"])
			if az not in az_ts_price:
				az_ts_price[az] = {}
			az_ts_price[az][ts] = sp

		for az, v in sorted(az_ts_price.iteritems()):
			ts_prev = None
			price_prev = None
			dur_sum = 0
			dur_price_sum = 0.0
			price_max = 0.0
			for ts, price in sorted(v.iteritems()):
				if ts_prev is not None:
					dur = (ts - ts_prev).total_seconds()
					dur_sum += dur
					dur_price_sum += (dur * price)

				price_max = max(price, price_max)
				ts_prev = ts
				price_prev = price
			price_avg = dur_price_sum / dur_sum
			Cons.P("%s cur=%f avg=%f max=%f" % (az, price_prev, price_avg, price_max))


	def _ReqSpotInst(self):
		# This is run as root
		#
		# http://unix.stackexchange.com/questions/4342/how-do-i-get-sudo-u-user-to-use-the-users-env
		user_data = \
"""#!/bin/bash
cd /home/ubuntu/work
rm -rf /home/ubuntu/work/acorn-tools
sudo -i -u ubuntu bash -c 'git clone https://github.com/hobinyoon/acorn-tools.git /home/ubuntu/work/acorn-tools'
sudo -i -u ubuntu /home/ubuntu/work/acorn-tools/ec2/ec2-init.py {0} {1} {2} {3}
"""
		user_data = user_data.format(_init_script, _jr_sqs_url, _jr_sqs_msg_receipt_handle, _num_regions)


		self.boto_client = boto3.session.Session().client("ec2", region_name = self.region_name)

		#self._GetPriceHistory()

		ls = {'ImageId': self.ami_id,
				#'KeyName': 'string',
				'SecurityGroups': ["cass-server"],
				'UserData': base64.b64encode(user_data),
				#'AddressingType': 'string',
				'InstanceType': self.inst_type,
				'EbsOptimized': True,
				}
		if self.az != None:
			ls['Placement'] = {}
			ls['Placement']['AvailabilityZone'] = self.az

		r = None
		while True:
			try:
				r = self.boto_client.request_spot_instances(
						SpotPrice=str(self.max_price),
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
				#Cons.P("SpotInstReqResp: %s" % pprint.pformat(r))
				if len(r["SpotInstanceRequests"]) != 1:
					raise RuntimeError("len(r[\"SpotInstanceRequests\"])=%d" % len(r["SpotInstanceRequests"]))
				self.spot_req_id = r["SpotInstanceRequests"][0]["SpotInstanceRequestId"]
				#Cons.P("region_name=%s spot_req_id=%s" % (self.region_name, self.spot_req_id))
				InstLaunchProgMon.SetSpotReqId(self.region_name, self.spot_req_id)
				break
			except botocore.exceptions.ClientError as e:
				if e.response["Error"]["Code"] == "RequestLimitExceeded":
					InstLaunchProgMon.UpdateError(self.region_name, e)
					time.sleep(5)
				else:
					raise e


	def _KeepCheckingSpotReq(self):
		r = None
		while True:
			while True:
				try:
					r = self.boto_client.describe_spot_instance_requests(
							SpotInstanceRequestIds=[self.spot_req_id])
					break
				except botocore.exceptions.ClientError as e:
					if e.response["Error"]["Code"] == "InvalidSpotInstanceRequestID.NotFound":
						InstLaunchProgMon.UpdateError(self.region_name, e)
						time.sleep(1)
					else:
						raise e

			if len(r["SpotInstanceRequests"]) != 1:
				raise RuntimeError("len(r[\"SpotInstanceRequests\"])=%d" % len(r["SpotInstanceRequests"]))
			#Cons.P(Util.Indent(pprint.pformat(r, indent=2, width=100), 2))

			InstLaunchProgMon.UpdateDescSpotInstResp(self.region_name, r)
			if r["SpotInstanceRequests"][0]["Status"]["Code"] == "fulfilled":
				break
			time.sleep(1)

		# Get inst_id
		#Cons.P(Util.Indent(pprint.pformat(r, indent=2, width=100), 2))
		self.inst_id = r["SpotInstanceRequests"][0]["InstanceId"]
		InstLaunchProgMon.SetInstID(self.region_name, self.inst_id)


	def _KeepCheckingInst(self):
		state = None
		tagged = False

		while True:
			r = None
			while True:
				try:
					r = self.boto_client.describe_instances(InstanceIds=[self.inst_id])
					# Note: describe_instances() returns StateReason, while
					# describe_instance_status() doesn't.
					break
				except botocore.exceptions.ClientError as e:
					if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
						InstLaunchProgMon.UpdateError(self.region_name, e)
						time.sleep(1)
					else:
						raise e

			InstLaunchProgMon.UpdateDescInstResp(self.region_name, r)
			state = r["Reservations"][0]["Instances"][0]["State"]["Name"]

			# Create tags
			if state == "pending" and tagged == False:
				tags_boto = []
				for k, v in _tags.iteritems():
					tags_boto.append({"Key": k, "Value": v})

				while True:
					try:
						self.boto_client.create_tags(Resources = [self.inst_id], Tags = tags_boto)
						tagged = True
						break
					except botocore.exceptions.ClientError as e:
						if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
							InstLaunchProgMon.UpdateError(self.region_name, e)
							time.sleep(1)
						elif e.response["Error"]["Code"] == "RequestLimitExceeded":
							InstLaunchProgMon.UpdateError(self.region_name, e)
							time.sleep(5)
						else:
							raise e

			elif state in ["terminated", "running"]:
				break
			time.sleep(1)

		# Make sure everything is ok.
		if state == "running":
			r = self.boto_client.describe_instances(InstanceIds=[self.inst_id])
			state = r["Reservations"][0]["Instances"][0]["State"]["Name"]
			InstLaunchProgMon.UpdateDescInstResp(self.region_name, r)

			# Make region-ipaddr files
			fn = "%s/%s" % (_dn_tmp, self.region_name)
			with open(fn, "w") as fo:
				fo.write(r["Reservations"][0]["Instances"][0]["PublicIpAddress"])


class InstLaunchProgMon():
	# The key is region. Spot req ID can't be used. A status (or an error) can be
	# returned before a spot request id is returned.
	# {region: [status]}
	_status_by_regions = {}
	_status_by_regions_lock = threading.Lock()

	@staticmethod
	def Reset():
		with InstLaunchProgMon._status_by_regions_lock:
			InstLaunchProgMon._status_by_regions = {}

	@staticmethod
	def SetSpotReqId(region_name, spot_req_id):
		with InstLaunchProgMon._status_by_regions_lock:
			if region_name not in InstLaunchProgMon._status_by_regions:
				InstLaunchProgMon._status_by_regions[region_name] = []
			InstLaunchProgMon._status_by_regions[region_name].append(spot_req_id)

	@staticmethod
	def UpdateDescSpotInstResp(region_name, r):
		with InstLaunchProgMon._status_by_regions_lock:
			InstLaunchProgMon._status_by_regions[region_name].append(InstLaunchProgMon.DescSpotInstResp(r))

	@staticmethod
	def SetInstID(region_name, inst_id):
		with InstLaunchProgMon._status_by_regions_lock:
			InstLaunchProgMon._status_by_regions[region_name].append(inst_id)

	@staticmethod
	def UpdateDescInstResp(region_name, r):
		with InstLaunchProgMon._status_by_regions_lock:
			InstLaunchProgMon._status_by_regions[region_name].append(InstLaunchProgMon.DescInstResp(r))

	@staticmethod
	def UpdateError(region_name, e):
		with InstLaunchProgMon._status_by_regions_lock:
			if region_name not in InstLaunchProgMon._status_by_regions:
				InstLaunchProgMon._status_by_regions[region_name] = []
			InstLaunchProgMon._status_by_regions[region_name].append(InstLaunchProgMon.Error(e))

	class DescSpotInstResp():
		def __init__(self, r):
			self.r = r

	class DescInstResp():
		def __init__(self, r):
			self.r = r

	class Error():
		def __init__(self, e):
			self.e = e


	@staticmethod
	def Run():
		output_lines_written = 0
		while True:
			status_by_regions = None
			with InstLaunchProgMon._status_by_regions_lock:
				status_by_regions = InstLaunchProgMon._status_by_regions.copy()

			output = ""
			for region, status in sorted(status_by_regions.iteritems()):
				if len(output) > 0:
					output += "\n"
				output += ("%-15s" % region)

				prev_s = None
				same_s_cnt = 1

				for s in status:
					s1 = None
					if type(s) is str:
						s1 = s
					elif isinstance(s, InstLaunchProgMon.DescSpotInstResp):
						s1 = s.r["SpotInstanceRequests"][0]["Status"]["Code"]
					elif isinstance(s, InstLaunchProgMon.DescInstResp):
						s1 = s.r["Reservations"][0]["Instances"][0]["State"]["Name"]
					elif isinstance(s, InstLaunchProgMon.Error):
						if isinstance(s.e, botocore.exceptions.ClientError):
							s1 = s.e.response["Error"]["Code"]
						else:
							s1 = str(s.e)
					else:
						raise RuntimeError("Unexpected s: %s" % s)

					# Print prev one when it's different from the current one
					if prev_s == s1:
						same_s_cnt += 1
					else:
						if prev_s is not None:
							if same_s_cnt == 1:
								if len(output.split("\n")[-1]) > 100:
									output += "\n               "
								output += (" %s" % prev_s)
							else:
								if len(output.split("\n")[-1]) > 100:
									output += "\n               "
								output += (" %s x%2d" % (prev_s, same_s_cnt))
							same_s_cnt = 1
					prev_s = s1

				# Print the last one
				if same_s_cnt == 1:
					if len(output.split("\n")[-1]) > 100:
						output += "\n               "
					output += (" %s" % prev_s)
				else:
					if len(output.split("\n")[-1]) > 100:
						output += "\n               "
					output += (" %s x%2d" % (prev_s, same_s_cnt))

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

			# Update status every so often
			time.sleep(0.1)
		print ""

		InstLaunchProgMon.DescInsts()

	@staticmethod
	def DescInsts():
		fmt = "%-15s %19s %10s %13s %15s %10s"
		Cons.P(Util.BuildHeader(fmt,
			"Placement:AvailabilityZone"
			" InstanceId"
			" InstanceType"
			" LaunchTime"
			#" PrivateIpAddress"
			" PublicIpAddress"
			" State:Name"
			))

		with InstLaunchProgMon._status_by_regions_lock:
			for region, status in sorted(InstLaunchProgMon._status_by_regions.iteritems()):
				for s in reversed(status):
					if isinstance(s, InstLaunchProgMon.DescInstResp):
						# Print only the last desc instance response per region
						r = s.r["Reservations"][0]["Instances"][0]
						Cons.P(fmt % (
							_Value(_Value(r, "Placement"), "AvailabilityZone")
							, _Value(r, "InstanceId")
							, _Value(r, "InstanceType")
							, _Value(r, "LaunchTime").strftime("%y%m%d-%H%M%S")
							#, _Value(r, "PrivateIpAddress")
							, _Value(r, "PublicIpAddress")
							, _Value(_Value(r, "State"), "Name")
							))
						break


def _Value(dict_, key):
	if key == "":
		return ""

	if key in dict_:
		return dict_[key]
	else:
		return ""

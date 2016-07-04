import base64
import botocore
import datetime
import os
import pprint
import sys
import threading
import time
import traceback

sys.path.insert(0, "%s/../util/python" % os.path.dirname(__file__))
import Cons
import Util

import BotoClient
import Ec2Region
import SpotInstLaunchProgMon
import SpotPrice


_dn_tmp = "%s/../.tmp" % os.path.dirname(__file__)
_job_id = None

_tags = None
_num_regions = None
_jr_sqs_url = None
_jr_sqs_msg_receipt_handle = None
_init_script = None

_region_az_lowest_max_spot_price = None
_pm = None


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

	# Get AZ with the lowest last-2-day max spot price
	# {region: az}
	global _region_az_lowest_max_spot_price
	_region_az_lowest_max_spot_price = SpotPrice.GetTheLowestMaxPriceAZs(region_spot_req)

	with SpotInstLaunchProgMon.SpotInstLaunchProgMon() as pm:
		global _pm
		_pm = pm

		rams = []
		for region, spot_req_params in region_spot_req.iteritems():
			rams.append(ReqAndMonitor(region, spot_req_params))

		threads = []
		for ram in rams:
			t = threading.Thread(target=ram.Run)
			t.daemon = True
			threads.append(t)
			t.start()

		for t in threads:
			t.join()


# This module can be called repeatedly
def Reset():
	global _job_id
	global _tags, _jr_sqs_url, _jr_sqs_msg_receipt_handle, _init_script

	_job_id = None

	_tags = None
	_num_regions = None
	_jr_sqs_url = None
	_jr_sqs_msg_receipt_handle = None
	_init_script = None


class ReqAndMonitor():
	def __init__(self, region, spot_req_params):
		self.region = region
		# Spot requests are made to specific AZs, which has the lowest last-1-day
		# max price.
		self.az = _region_az_lowest_max_spot_price[region]

		self.ami_id = Ec2Region.GetLatestAmiId(self.region)
		self.inst_type = spot_req_params["inst_type"]
		self.max_price = spot_req_params["max_price"]

		self.inst_id = None


	def Run(self):
		try:
			self._ReqSpotInst()
			self._KeepCheckingSpotReq()
			self._KeepCheckingInst()
		except Exception as e:
			Cons.P("%s\nregion=%s\n%s" % (e, self.region, traceback.format_exc()))
			os._exit(1)


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

		ls = {'ImageId': self.ami_id
				#, 'KeyName': 'string'
				, 'SecurityGroups': ["cass-server"]
				, 'UserData': base64.b64encode(user_data)
				#, 'AddressingType': 'string'
				, 'InstanceType': self.inst_type
				, 'EbsOptimized': True
				, 'Placement': {'AvailabilityZone': self.az}
				}

		while True:
			try:
				r = BotoClient.Get(self.region).request_spot_instances(
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
				#Cons.P("region=%s spot_req_id=%s" % (self.region, self.spot_req_id))
				_pm.SetSpotReqId(self.region, self.spot_req_id)
				break
			except botocore.exceptions.ClientError as e:
				if e.response["Error"]["Code"] == "RequestLimitExceeded":
					_pm.UpdateError(self.region, e)
					time.sleep(5)
				else:
					raise e


	def _KeepCheckingSpotReq(self):
		r = None
		while True:
			while True:
				try:
					r = BotoClient.Get(self.region).describe_spot_instance_requests(
							SpotInstanceRequestIds=[self.spot_req_id])
					break
				except botocore.exceptions.ClientError as e:
					if e.response["Error"]["Code"] == "InvalidSpotInstanceRequestID.NotFound":
						_pm.UpdateError(self.region, e)
						time.sleep(1)
					else:
						raise e

			if len(r["SpotInstanceRequests"]) != 1:
				raise RuntimeError("len(r[\"SpotInstanceRequests\"])=%d" % len(r["SpotInstanceRequests"]))
			#Cons.P(Util.Indent(pprint.pformat(r, indent=2, width=100), 2))

			_pm.UpdateDescSpotInstResp(self.region, r)
			if r["SpotInstanceRequests"][0]["Status"]["Code"] == "fulfilled":
				break
			time.sleep(1)

		# Get inst_id
		#Cons.P(Util.Indent(pprint.pformat(r, indent=2, width=100), 2))
		self.inst_id = r["SpotInstanceRequests"][0]["InstanceId"]
		_pm.SetInstID(self.region, self.inst_id)


	def _KeepCheckingInst(self):
		state = None
		tagged = False

		while True:
			r = None
			while True:
				try:
					r = BotoClient.Get(self.region).describe_instances(InstanceIds=[self.inst_id])
					# Note: describe_instances() returns StateReason, while
					# describe_instance_status() doesn't.
					break
				except botocore.exceptions.ClientError as e:
					if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
						_pm.UpdateError(self.region, e)
						time.sleep(1)
					else:
						raise e

			_pm.UpdateDescInstResp(self.region, r)
			state = r["Reservations"][0]["Instances"][0]["State"]["Name"]

			# Create tags
			if state == "pending" and tagged == False:
				tags_boto = []
				for k, v in _tags.iteritems():
					tags_boto.append({"Key": k, "Value": v})

				while True:
					try:
						BotoClient.Get(self.region).create_tags(Resources = [self.inst_id], Tags = tags_boto)
						tagged = True
						break
					except botocore.exceptions.ClientError as e:
						if e.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
							_pm.UpdateError(self.region, e)
							time.sleep(1)
						elif e.response["Error"]["Code"] == "RequestLimitExceeded":
							_pm.UpdateError(self.region, e)
							time.sleep(5)
						else:
							raise e

			elif state in ["terminated", "running"]:
				break
			time.sleep(1)

		# Make sure everything is ok.
		if state == "running":
			r = BotoClient.Get(self.region).describe_instances(InstanceIds=[self.inst_id])
			state = r["Reservations"][0]["Instances"][0]["State"]["Name"]
			_pm.UpdateDescInstResp(self.region, r)

			# Make region-ipaddr files
			fn = "%s/%s" % (_dn_tmp, self.region)
			with open(fn, "w") as fo:
				fo.write(r["Reservations"][0]["Instances"][0]["PublicIpAddress"])

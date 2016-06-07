import boto3
import datetime
import os
import sys
import threading
import time

import ConsMt

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Util

_fmt_desc_inst = "%13s %-15s %10s %15s %13s"

_monitors = set()

def StopAll():
	for m in _monitors:
		m.ReqStop()


class IM:
	def __enter__(self):
		_monitors.add(self)
		self.stop_requested = False
		self.cv = threading.Condition()
		self.t = threading.Thread(target=self.DescInst)
		self.t.daemon = True
		self.t.start()

	def DescInst(self):
		self.bt = time.time()
		self.lines_printed = 0
		while self.stop_requested == False:
			bt = time.time()

			self._DescInst()

			if self.stop_requested:
				break

			wait_time = 10 - (time.time() - bt)
			if wait_time > 0:
				with self.cv:
					self.cv.wait(wait_time)

	def _DescInst(self):
		sys.stdout.write("Describing instances:")
		sys.stdout.flush()
		self.lines_printed += 1

		dis = []
		regions_all = [
				"us-east-1"
				, "us-west-1"
				, "us-west-2"
				, "eu-west-1"
				, "eu-central-1"
				, "ap-southeast-1"
				, "ap-southeast-2"
				, "ap-northeast-2"
				, "ap-northeast-1"
				, "sa-east-1"
				]
		for r in regions_all:
			dis.append(DescInstPerRegion(r))

		self.threads = []
		for di in dis:
			t = threading.Thread(target=di.Run)
			self.threads.append(t)
			t.daemon = True
			t.start()

		for t in self.threads:
			t.join()
		print ""

		# Clear previous lines
		for i in range(self.lines_printed):
			# Clear current line
			sys.stdout.write(chr(27) + "[2K")
			# Move the cursor up
			sys.stdout.write(chr(27) + "[1A")
			# Move the cursor to column 1
			sys.stdout.write(chr(27) + "[1G")
		# Clear current line
		sys.stdout.write(chr(27) + "[2K")
		self.lines_printed = 0

		num_insts = 0
		for di in dis:
			num_insts += di.NumInsts()
		if num_insts == 0:
			ConsMt.P("No instances found.")
			self.lines_printed += 1
			return

		header = Util.BuildHeader(_fmt_desc_inst,
			"job_id"
			" Placement:AvailabilityZone"
			" InstanceId"
			" PublicIpAddress"
			" State:Name"
			)
		ConsMt.P(header)
		self.lines_printed += len(header.split("\n"))

		results = []
		for di in dis:
			results += di.GetResults()
		for r in sorted(results):
			ConsMt.P(r)
			self.lines_printed += 1

		ConsMt.P("")
		ConsMt.P("Time since the last msg: %s" % (str(datetime.timedelta(seconds=(time.time() - self.bt)))))
		ConsMt.P("")
		self.lines_printed += 3

	def __exit__(self, type, value, traceback):
		self.ReqStop()

	def ReqStop(self):
		self.stop_requested = True
		with self.cv:
			self.cv.notifyAll()
		if self.t != None:
			self.t.join()
			# It doesn't kill the running threads immediately, which is fine. There
			# is only like 1 - 2 secs of delay.


class DescInstPerRegion:
	def __init__(self, region):
		self.region = region
		self.key_error = None

	def Run(self):
		try:
			boto_client = boto3.session.Session().client("ec2", region_name=self.region)
			self.response = boto_client.describe_instances()
		except KeyError as e:
			#ConsMt.P("region=%s KeyError=[%s]" % (self.region, e))
			self.key_error = e

		ConsMt.sys_stdout_write(" %s" % self.region)

	def NumInsts(self):
		if self.key_error is not None:
			return 0
		num = 0
		for r in self.response["Reservations"]:
			for r1 in r["Instances"]:
				num += 1
		return num

	def GetInstDesc(self):
		ids = []
		if self.key_error is not None:
			return ids
		for r in self.response["Reservations"]:
			ids += r["Instances"]
		return ids


	def GetResults(self):
		if self.key_error is not None:
			return ["region=%s KeyError=[%s]" % (self.region, self.key_error)]

		#ConsMt.P(pprint.pformat(self.response, indent=2, width=100))
		results = []
		for r in self.response["Reservations"]:
			for r1 in r["Instances"]:
				if _Value(_Value(r1, "State"), "Name") == "terminated":
					continue

				tags = {}
				if "Tags" in r1:
					for t in r1["Tags"]:
						tags[t["Key"]] = t["Value"]

				results.append(_fmt_desc_inst % (
					tags.get("job_id")
					, _Value(_Value(r1, "Placement"), "AvailabilityZone")
					, _Value(r1, "InstanceId")
					, _Value(r1, "PublicIpAddress")
					, _Value(_Value(r1, "State"), "Name")
					))
		return results


def _Value(dict_, key):
	if key == "":
		return ""

	if key in dict_:
		return dict_[key]
	else:
		return ""



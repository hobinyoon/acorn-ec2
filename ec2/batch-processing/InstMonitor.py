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


class IM:
	monitor_interval_in_sec = 10

	def __init__(self):
		self.cv = threading.Condition()

	def __enter__(self):
		self.stop_requested = False
		self.t = threading.Thread(target=self.DescInst)
		self.t.daemon = True
		self.t.start()

	def DescInst(self):
		self.desc_inst_start_time = time.time()
		self.stdout_msg = ""
		while self.stop_requested == False:
			bt = time.time()
			self._DescInst()
			if self.stop_requested:
				break
			wait_time = IM.monitor_interval_in_sec - (time.time() - bt)
			if wait_time > 0:
				with self.cv:
					self.cv.wait(wait_time)

	def _DescInst(self):
		DIO.P("Describing instances:")

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
		DIO.P("\n\n")
		self.last_desc_inst_time = time.time()

		num_insts = 0
		for di in dis:
			num_insts += di.NumInsts()
		if num_insts == 0:
			DIO.P("No instances found.\n")
			return

		header = Util.BuildHeader(_fmt_desc_inst,
			"job_id"
			" Placement:AvailabilityZone"
			" InstanceId"
			" PublicIpAddress"
			" State:Name"
			)
		DIO.P(header + "\n")

		results = []
		for di in dis:
			results += di.GetResults()
		for r in sorted(results):
			DIO.P(r + "\n")

		DIO.P("\nTime since the last msg: %s" % (str(datetime.timedelta(seconds=(time.time() - self.desc_inst_start_time)))))
		DIO.Flush()

	def __exit__(self, type, value, traceback):
		self.ReqStop()

	def ReqStop(self):
		self.stop_requested = True
		with self.cv:
			self.cv.notifyAll()
		if self.t != None:
			#self.t.join() doesn't kill the running threads immediately.
			try:
				self.t.exit()
			except AttributeError:
				pass


# Describe instance output
class DIO:
	msg = ""
	msg_lock = threading.Lock()
	lines_printed = 0

	@staticmethod
	def P(msg):
		with DIO.msg_lock:
			DIO.msg += msg

	@staticmethod
	def Flush():
		with DIO.msg_lock:
			# Clear previous printed lines
			for i in range(DIO.lines_printed):
				# Clear current line
				sys.stdout.write(chr(27) + "[2K")
				# Move the cursor up
				sys.stdout.write(chr(27) + "[1A")
				# Move the cursor to column 1
				sys.stdout.write(chr(27) + "[1G")
			# Clear current line
			sys.stdout.write(chr(27) + "[2K")

			ConsMt.Pnnl(DIO.msg)
			DIO.lines_printed = len(DIO.msg.split("\n")) - 1
			DIO.msg = ""


class DescInstPerRegion:
	def __init__(self, region):
		self.region = region

	def Run(self):
		boto_client = boto3.session.Session().client("ec2", region_name=self.region)
		self.response = boto_client.describe_instances()
		DIO.P(" %s" % self.region)

	def NumInsts(self):
		num = 0
		for r in self.response["Reservations"]:
			for r1 in r["Instances"]:
				num += 1
		return num

	def GetInstDesc(self):
		ids = []
		for r in self.response["Reservations"]:
			ids += r["Instances"]
		return ids

	def GetResults(self):
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

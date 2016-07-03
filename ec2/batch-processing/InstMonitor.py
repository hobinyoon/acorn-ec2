import boto3
import botocore
import datetime
import os
import sys
import threading
import time

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "%s/.." % os.path.dirname(__file__))
import Ec2Region

_fmt_desc_inst = "%13s %-15s %19s %15s %13s"


class IM:
	monitor_interval_in_sec = 10

	def __enter__(self):
		self.stop_requested = False
		self.dio = DIO()
		self.cv = threading.Condition()
		self.t = threading.Thread(target=self.DescInst)
		self.t.daemon = True
		self.t.start()

	def __exit__(self, type, value, traceback):
		self.ReqStop()

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
		self.dio.P("\nDescribing instances:")

		DescInstPerRegion.Reset()

		dis = []
		for r in Ec2Region.All():
			dis.append(DescInstPerRegion(r, self.dio))

		self.per_region_threads = []
		for di in dis:
			t = threading.Thread(target=di.Run)
			self.per_region_threads.append(t)
			t.daemon = True
			t.start()

		# Exit immediately when requested
		for t in self.per_region_threads:
			while t.isAlive():
				if self.stop_requested:
					return
				t.join(0.1)

		self.dio.P("\n")

		num_insts = 0
		for di in dis:
			num_insts += di.NumInsts()
		if num_insts == 0:
			self.dio.P("No instances found.\n")
		else:
			header = Util.BuildHeader(_fmt_desc_inst,
				"job_id"
				" Placement:AvailabilityZone"
				" InstanceId"
				" PublicIpAddress"
				" State:Name"
				)
			self.dio.P(header + "\n")

			results = []
			for di in dis:
				results += di.GetResults()
			for r in sorted(results):
				self.dio.P(r + "\n")

		self.dio.P("Time since the last msg: %s" % (str(datetime.timedelta(seconds=(time.time() - self.desc_inst_start_time)))))
		self.dio.Flush()

	def ReqStop(self):
		self.stop_requested = True
		with self.cv:
			self.cv.notifyAll()
		if self.t != None:
			# There doesn't seem to be a good way of immediately stopping a running
			# thread by calling a non-existing function. thread module has exit(),
			# but it's a low level-API, not enough documentation, seems to be getting
			# deprecated.
			#
			# Worked around by specifying timeout to join() to each per-region thread
			# above
			self.t.join()
		self.dio.MayPrintNewlines()


# Describe instance output
class DIO:
	def __init__(self):
		self.msg = ""
		self.msg_lock = threading.Lock()
		self.lines_printed = 0

	def P(self, msg):
		with self.msg_lock:
			self.msg += msg

	def Flush(self):
		with self.msg_lock:
			# Clear previous printed lines
			for i in range(self.lines_printed):
				# Clear current line
				sys.stdout.write(chr(27) + "[2K")
				# Move the cursor up
				sys.stdout.write(chr(27) + "[1A")
				# Move the cursor to column 1
				sys.stdout.write(chr(27) + "[1G")
			# Clear current line
			sys.stdout.write(chr(27) + "[2K")

			Cons.Pnnl(self.msg)
			self.lines_printed = len(self.msg.split("\n")) - 1
			self.msg = ""

	def MayPrintNewlines(self):
		if self.lines_printed > 0:
			Cons.P("")


class DescInstPerRegion:
	boto_responses_received = 0
	boto_responses_received_lock = threading.Lock()

	@staticmethod
	def Reset():
		with DescInstPerRegion.boto_responses_received_lock:
			DescInstPerRegion.boto_responses_received = 0

	def __init__(self, region, dio):
		self.region = region
		self.dio = dio

	def Run(self):
		while True:
			try:
				bc = BcMgr.Get(self.region)
				self.response = bc.describe_instances()
				with DescInstPerRegion.boto_responses_received_lock:
					DescInstPerRegion.boto_responses_received += 1
					if DescInstPerRegion.boto_responses_received == 7:
						#             Describing instances:
						self.dio.P("\n                      %s" % self.region)
					else:
						self.dio.P(" %s" % self.region)
				break
			except botocore.exceptions.ClientError as e:
				Cons.P("%s. Region=%s. Resetting boto client after 1 sec ..." % (e, self.region))
				time.sleep(1)
				BcMgr.Reset(self.region)

	def NumInsts(self):
		num = 0
		for r in self.response["Reservations"]:
			for r1 in r["Instances"]:
				if _Value(_Value(r1, "State"), "Name") == "terminated":
					continue
				num += 1
		return num

	def GetInstDesc(self):
		ids = []
		for r in self.response["Reservations"]:
			ids += r["Instances"]
		return ids

	def GetResults(self):
		#Cons.P(pprint.pformat(self.response, indent=2, width=100))
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


# Boto session client manager
class BcMgr:
	region_bc = {}

	@staticmethod
	def Get(region):
		bc = BcMgr.region_bc.get(region, None)
		if bc == None:
			bc = boto3.session.Session().client("ec2", region_name=region)
			BcMgr.region_bc[region] = bc
		return bc

	@staticmethod
	def Reset(region):
		BcMgr.region_bc[region] = None

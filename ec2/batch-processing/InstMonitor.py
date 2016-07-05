import botocore
import datetime
import os
import Queue
import sys
import threading
import time

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "%s/.." % os.path.dirname(__file__))
import BotoClient
import Ec2Region


class IM:
	monitor_interval_in_sec = 10

	def __enter__(self):
		self.stop_requested = False
		self.dio = DIO()
		self.cv = threading.Condition()
		self.t = threading.Thread(target=self.DescInst)
		self.t.daemon = True
		self.t.start()
		return self

	def __exit__(self, type, value, traceback):
		self.ReqStop()

	def DescInst(self):
		self.desc_inst_start_time = datetime.datetime.now()
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
			num_insts += len(di.Instances())
		if num_insts == 0:
			self.dio.P("No instances found.\n")
		else:
			self.dio.P("#"
					" job_id"
					" (Placement:AvailabilityZone"
					" InstanceId"
					" PublicIpAddress"
					" State:Name) ...\n")

			# Group by job_id. Only for those with job_ids
			#   { job_id: {region: Inst} }
			jobid_inst = {}
			# Think about what to do with instances without job IDs
			nojobid_inst = []
			for di in dis:
				for i in di.Instances():
					if i.job_id is not None:
						if i.job_id not in jobid_inst:
							jobid_inst[i.job_id] = {}
						jobid_inst[i.job_id][i.region] = i
					else:
						nojobid_inst.append(i)

			ClusterCleaner.Clean(jobid_inst)

			for job_id, v in sorted(jobid_inst.iteritems()):
				self.dio.P("%s %d" % (job_id, len(v)))
				for k1, i in v.iteritems():
					#msg = " (%s %s %s %s)" % (i.az, i.inst_id, i.public_ip, i.state)
					msg = " (%s %s %s)" % (i.az, i.public_ip, i.state)
					if self.dio.LastLineWidth() + len(msg) > DIO.max_column_width:
						self.dio.P("\n  ")
					self.dio.P(msg)
				self.dio.P("\n")

			if len(nojobid_inst) > 0:
				self.dio.P("%-13s %d" % ("no-job-id", len(nojobid_inst)))
				for i in nojobid_inst:
					msg = " (%s %s %s)" % (i.az, i.public_ip, i.state)
					if self.dio.LastLineWidth() + len(msg) > DIO.max_column_width:
						self.dio.P("\n  ")
					self.dio.P(msg)
				self.dio.P("\n")

		self.dio.P("Time since the last msg: %s" % (datetime.datetime.now() - self.desc_inst_start_time))
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
	max_column_width = 120

	def __init__(self):
		self.msg = ""
		self.msg_lock = threading.Lock()
		self.lines_printed = 0

	def P(self, msg):
		with self.msg_lock:
			self.msg += msg

	def LastLineWidth(self):
		with self.msg_lock:
			return len(self.msg.split("\n")[-1])

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
		self.instances = []

	def Run(self):
		while True:
			try:
				self.response = BotoClient.Get(self.region).describe_instances()
				self.instances = []
				for r in self.response["Reservations"]:
					for r1 in r["Instances"]:
						if _Value(_Value(r1, "State"), "Name") == "terminated":
							continue
						self.instances.append(Inst(r1))

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
				BotoClient.Reset(self.region)

	def Instances(self):
		return self.instances


class Inst():
	def __init__(self, desc_inst_resp):
		self.tags = {}
		if "Tags" in desc_inst_resp:
			for t in desc_inst_resp["Tags"]:
				self.tags[t["Key"]] = t["Value"]

		self.job_id = self.tags.get("job_id")
		self.az = _Value(_Value(desc_inst_resp, "Placement"), "AvailabilityZone")
		self.region = self.az[:-1]
		self.inst_id = _Value(desc_inst_resp, "InstanceId")
		self.public_ip = _Value(desc_inst_resp, "PublicIpAddress")
		self.state = _Value(_Value(desc_inst_resp, "State"), "Name")


class ClusterCleaner():
	wait_time_before_clean = 20

	# First time we've seen a cluster with under 11 nodes
	#   { job_id: datetime }
	jobid_first_time_under11 = {}

	# Cluster cleaner job queue
	_q = Queue.Queue(maxsize=1)

	@staticmethod
	def Queue():
		return ClusterCleaner._q

	@staticmethod
	def Clean(jobid_inst):
		# jobid_inst: { job_id: {region: Inst} }

		for job_id, v in jobid_inst.iteritems():
			# Only monitor acorn-server nodes. Do not monitor dev nodes.
			is_acorn_server = False
			for region, i in v.iteritems():
				if "init_script" in i.tags:
					if i.tags["init_script"] == "acorn-server":
						is_acorn_server = True
						break
			if not is_acorn_server:
				continue
			Cons.P(Util.FileLine())

			if len(v) == 11:
				ClusterCleaner.jobid_first_time_under11.pop(job_id, None)
				continue

			if job_id not in ClusterCleaner.jobid_first_time_under11:
				ClusterCleaner.jobid_first_time_under11[job_id] = datetime.datetime.now()
				continue

			# If the cluster had less than 11 nodes and has been there for more then
			# 6 mins, terminate it.
			diff = (datetime.datetime.now() - ClusterCleaner.jobid_first_time_under11[job_id]).total_seconds()
			if diff > ClusterCleaner.wait_time_before_clean:
				Cons.P("Cluster with job_id %s has less than 11 nodes for the last %d seconds. Termination requested." % diff)
			Cons.P(Util.FileLine())
			ClusterCleaner._q.put(ClusterCleaner.Msg(job_id), block=False)

			# Reset to give the job-controller some time to clean up the cluster
			ClusterCleaner.jobid_first_time_under11[job_id] = datetime.datetime.now()

	class Msg():
		def __init__(self, job_id):
			self.job_id = job_id


def _Value(dict_, key):
	if dict_ is None:
		return None
	if key is None:
		return None
	return dict_.get(key)

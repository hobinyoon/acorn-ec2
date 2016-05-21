import boto3
import os
import pprint
import sys
import threading

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util


_fmt = "%-15s %10s %10s %13s %15s %15s %13s %20s"
_regions_all = [
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


def Run(acorn_exp_param = None):
	sys.stdout.write("desc_instances:")
	sys.stdout.flush()

	dis = []
	for r in _regions_all:
		dis.append(DescInstPerRegion(r, acorn_exp_param))

	threads = []
	for di in dis:
		t = threading.Thread(target=di.Run)
		threads.append(t)
		t.start()

	for t in threads:
		t.join()
	print ""

	num_insts = 0
	for di in dis:
		num_insts += di.NumInsts()
	if num_insts == 0:
		ConsP("No instances found.")
		return

	print ""
	ConsP(Util.BuildHeader(_fmt,
		"Placement:AvailabilityZone"
		" InstanceId"
		" InstanceType"
		" LaunchTime"
		" PrivateIpAddress"
		" PublicIpAddress"
		" State:Name"
		" Tag:Name"
		))

	for di in dis:
		di.PrintResult()


def GetInstDescs(acorn_exp_param = None):
	sys.stdout.write("desc_instances:")
	sys.stdout.flush()

	dis = []
	for r in _regions_all:
		dis.append(DescInstPerRegion(r, acorn_exp_param))

	threads = []
	for di in dis:
		t = threading.Thread(target=di.Run)
		threads.append(t)
		t.start()

	for t in threads:
		t.join()
	print ""

	inst_descs = []
	for di in dis:
		inst_descs += di.GetInstDesc()
	return inst_descs


class DescInstPerRegion:
	def __init__(self, region, acorn_exp_param):
		self.region = region
		self.tags = {"cluster_name": "acorn-server", "acorn_exp_param": acorn_exp_param}
		self.exception = None

	def Run(self):
		try:
			# http://boto3.readthedocs.io/en/latest/guide/session.html
			session = boto3.session.Session()
			boto_client = session.client("ec2", region_name=self.region)

			if self.tags == None:
				self.response = boto_client.describe_instances()
			else:
				filters = []
				for k, v in self.tags.iteritems():
					d = {}
					d["Name"] = ("tag:%s" % k)
					d["Values"] = [v]
					filters.append(d)
				self.response = boto_client.describe_instances(Filters = filters)

		except KeyError as e:
			#ConsP("region=%s KeyError=[%s]" % (self.region, e))
			self.exception = e

		sys_stdout_write(" %s" % self.region)

	def NumInsts(self):
		if self.exception != None:
			return 0
		num = 0
		for r in self.response["Reservations"]:
			for r1 in r["Instances"]:
				num += 1
		return num

	def GetInstDesc(self):
		ids = []
		if self.exception != None:
			return ids
		for r in self.response["Reservations"]:
			ids += r["Instances"]
		return ids


	def PrintResult(self):
		if self.exception != None:
			ConsP("region=%s KeyError=[%s]" % (self.region, self.exception))
			return

		#ConsP(pprint.pformat(self.response, indent=2, width=100))

		for r in self.response["Reservations"]:
			for r1 in r["Instances"]:
				tags_str = ""
				if "Tags" in r1:
					for t in r1["Tags"]:
						if len(tags_str) > 0:
							tags_str += ","
						tags_str += "%s:%s" % (t["Key"], t["Value"])

				ConsP(_fmt % (
					_Value(_Value(r1, "Placement"), "AvailabilityZone")
					, _Value(r1, "InstanceId")
					, _Value(r1, "InstanceType")
					, _Value(r1, "LaunchTime").strftime("%y%m%d-%H%M%S")
					, _Value(r1, "PrivateIpAddress")
					, _Value(r1, "PublicIpAddress")
					, _Value(_Value(r1, "State"), "Name")
					, tags_str
					))


def _Value(dict_, key):
	if key == "":
		return ""

	if key in dict_:
		return dict_[key]
	else:
		return ""


_print_lock = threading.Lock()

# Serialization is not needed in this file. Leave it for now.
def ConsP(msg):
	with _print_lock:
		Cons.P(msg)


def sys_stdout_write(msg):
	with _print_lock:
		sys.stdout.write(msg)
		sys.stdout.flush()

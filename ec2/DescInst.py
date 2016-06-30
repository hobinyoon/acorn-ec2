import boto3
import os
import pprint
import sys
import threading

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util

import Ec2Region


_fmt = "%13s %-15s %19s %15s %13s"

def Run(tags = None):
	sys.stdout.write("desc_instances:")
	sys.stdout.flush()

	dis = []
	for r in Ec2Region.All():
		dis.append(DescInstPerRegion(r, tags))

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
		Cons.P("No instances found.")
		return

	print ""
	Cons.P(Util.BuildHeader(_fmt,
		"job_id"
		" Placement:AvailabilityZone"
		" InstanceId"
		#" InstanceType"
		#" LaunchTime"
		#" PrivateIpAddress"
		" PublicIpAddress"
		" State:Name"
		#" Tag:Name"
		))

	results = []
	for di in dis:
		results += di.GetResults()
	for r in sorted(results):
		Cons.P(r)


def GetInstDescs(tags = None):
	sys.stdout.write("desc_instances:")
	sys.stdout.flush()

	dis = []
	for r in Ec2Region.All():
		dis.append(DescInstPerRegion(r, tags))

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
	def __init__(self, region, tags):
		self.region = region
		self.tags = tags
		self.key_error = None

	def Run(self):
		try:
			# http://boto3.readthedocs.io/en/latest/guide/session.html
			session = boto3.session.Session()
			boto_client = session.client("ec2", region_name=self.region)

			if self.tags is None:
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
			#Cons.P("region=%s KeyError=[%s]" % (self.region, e))
			self.key_error = e

		Cons.sys_stdout_write(" %s" % self.region)

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

				results.append(_fmt % (
					tags.get("job_id")
					, _Value(_Value(r1, "Placement"), "AvailabilityZone")
					, _Value(r1, "InstanceId")
					#, _Value(r1, "InstanceType")
					#, _Value(r1, "LaunchTime").strftime("%y%m%d-%H%M%S")
					#, _Value(r1, "PrivateIpAddress")
					, _Value(r1, "PublicIpAddress")
					, _Value(_Value(r1, "State"), "Name")
					#, ", ".join(["%s:%s" % (k, v) for (k, v) in sorted(tags.items())])
					))
		return results


def _Value(dict_, key):
	if key == "":
		return ""

	if key in dict_:
		return dict_[key]
	else:
		return ""

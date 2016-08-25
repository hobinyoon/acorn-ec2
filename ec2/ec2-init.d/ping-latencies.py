#!/usr/bin/env python

import base64
import boto3
import botocore
import datetime
import errno
import imp
import os
import pprint
import subprocess
import sys
import threading
import time
import traceback
import yaml
import zipfile

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "%s" % os.path.dirname(__file__))
import GetIPs


def _Log(msg):
	Cons.P("%s: %s" % (time.strftime("%y%m%d-%H%M%S"), msg))


_az = None
_region = None

def _SetHostname():
	# Hostname consists of availability zone name and launch req datetime
	hn = "%s-%s" % (_az, _tags["job_id"])

	# http://askubuntu.com/questions/9540/how-do-i-change-the-computer-name
	Util.RunSubp("sudo sh -c 'echo \"%s\" > /etc/hostname'" % hn)
	Util.RunSubp("sudo sed -i '/^127.0.0.1 localhost.*/c\\127.0.0.1 localhost %s' /etc/hosts" % hn)
	Util.RunSubp("sudo service hostname restart")


def _SyncTime():
	# Sync time. Important for Cassandra.
	# http://askubuntu.com/questions/254826/how-to-force-a-clock-update-using-ntp
	_Log("Synching time ...")
	Util.RunSubp("sudo service ntp stop")

	# Fails with a rc 1 in the init script. Mask with true for now.
	Util.RunSubp("sudo /usr/sbin/ntpd -gq || true")

	Util.RunSubp("sudo service ntp start")


def _InstallPkgs():
	Util.RunSubp("sudo apt-get update && sudo apt-get install -y pssh dstat")


def _MountAndFormatLocalSSDs():
	# Make sure we are using the known machine types
	inst_type = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-type", print_cmd = False, print_output = False)

	ssds = []
	devs = []

	# All c3 types has 2 SSDs
	if inst_type.startswith("c3."):
		ssds = ["ssd0", "ssd1"]
		devs = ["xvdb", "xvdc"]
	elif inst_type in ["r3.large", "r3.xlarge", "r3.2xlarge", "r3.4xlarge"
			, "i2.xlarge"]:
		ssds = ["ssd0"]
		devs = ["xvdb"]
	else:
		raise RuntimeError("Unexpected instance type %s" % inst_type)

	Util.RunSubp("sudo umount /mnt || true")
	for i in range(len(ssds)):
		_Log("Setting up Local %s ..." % ssds[i])
		Util.RunSubp("sudo umount /dev/%s || true" % devs[i])
		Util.RunSubp("sudo mkdir -p /mnt/local-%s" % ssds[i])

		# Prevent lazy Initialization
		# - "When creating an Ext4 file system, the existing regions of the inode
		#   tables must be cleaned (overwritten with nulls, or "zeroed"). The
		#   "lazyinit" feature should significantly accelerate the creation of a
		#   file system, because it does not immediately initialize all inode
		#   tables, initializing them gradually instead during the initial mounting
		#   process in background (from Kernel version 2.6.37)."
		#   - https://www.thomas-krenn.com/en/wiki/Ext4_Filesystem
		# - Default values are 1s, which do lazy init.
		#   - man mkfs.ext4
		#
		# nodiscard is in the documentation
		# - https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ssd-instance-store.html
		# - Without nodiscard, it takes about 80 secs for a 800GB SSD.
		Util.RunSubp("sudo mkfs.ext4 -m 0 -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 -L local-%s /dev/%s"
				% (ssds[i], devs[i]))

		# Some are already mounted. I suspect /etc/fstab does the magic when the
		# file system is created. Give it some time and umount
		time.sleep(1)
		Util.RunSubp("sudo umount /dev/%s || true" % devs[i])

		# -o discard for TRIM
		Util.RunSubp("sudo mount -t ext4 -o discard /dev/%s /mnt/local-%s" % (devs[i], ssds[i]))
		Util.RunSubp("sudo chown -R ubuntu /mnt/local-%s" % ssds[i])


_dn_ping_output = "/mnt/local-ssd0/ping-result"
_all_ips = None

def _PingAllNodes():
	global _all_ips
	# Wait until you see all nodes
	while True:
		_all_ips = GetIPs.GetByTags(_tags)
		if len(_all_ips) == _num_regions:
			break
		time.sleep(1)
	_Log(_all_ips)

	ip_this = GetIPs.GetMyPubIp()

	Util.MkDirs(_dn_ping_output)

	threads = []
	for ip in _all_ips:
		t = threading.Thread(target=__Ping, args=[ip_this, ip])
		t.daemon = True
		t.start()
		threads.append(t)
	
	for t in threads:
		t.join()


def __Ping(ip_from, ip_to):
	try:
		cmd = "ping -c 1800 -D %s" % ip_to
		# Note: You can map ip addr to region name later
		fn = "%s/%s-%s" % (_dn_ping_output, ip_from, ip_to)
		with open(fn, "w") as fo:
			subprocess.call(cmd, shell=True, stdout=fo, stderr=subprocess.STDOUT)
	except Exception as e:
		Cons.P("%s\n%s" % (e, traceback.format_exc()))
		os._exit(1)


s3_bucket_name = "ping-latency"


def _UploadResult():
	prev_dir = os.getcwd()
	os.chdir("/mnt/local-ssd0")

	# Zip .run
	# http://stackoverflow.com/questions/1855095/how-to-create-a-zip-archive-of-a-directory
	fn_out = "/mnt/local-ssd0/%s-%s.zip" % (_job_id, GetIPs.GetMyPubIp())
	with zipfile.ZipFile(fn_out, "w", zipfile.ZIP_DEFLATED) as zf:
		for root, dirs, files in os.walk(_dn_ping_output):
			for f in files:
				_ZfWrite(zf, os.path.join(root, f))

	_Log("Created %s %d" % (os.path.abspath(fn_out), os.path.getsize(fn_out)))

	# Upload to S3
	_Log("Uploading data to S3 ...")
	s3 = boto3.resource("s3", region_name = _s3_region)
	# If you don't specify a region, the bucket will be created in US Standard.
	#  http://boto3.readthedocs.io/en/latest/reference/services/s3.html#S3.Client.create_bucket
	r = s3.create_bucket(Bucket=s3_bucket_name)
	#_Log(pprint.pformat(r))
	r = s3.Object(s3_bucket_name, "%s-%s.zip" % (_job_id, GetIPs.GetMyPubIp())).put(Body=open(fn_out, "rb"))
	#_Log(pprint.pformat(r))

	os.chdir(prev_dir)


# Ignore non-existent files
def _ZfWrite(zf, fn):
	try:
		zf.write(fn)
	except OSError as e:
		if e.errno == errno.ENOENT:
			pass
		else:
			raise e


def _PostJobDoneMsg():
	# Post a "job done" message, so that the controller node can delete the job
	# req msg and shutdown the cluster.
	_Log("Posting a job completion message ...")
	q = _GetJcQ()
	_EnqJcMsg(q)


_sqs_region = "us-east-1"
_s3_region  = "us-east-1"
_bc = None


q_name_jc = "acorn-jobs-completed"
_sqs = None

# Get the queue. Create one if not exists.
def _GetJcQ():
	global _sqs
	_sqs = boto3.resource("sqs", region_name = _sqs_region)

	_Log("Getting the job completion queue ...")
	try:
		queue = _sqs.get_queue_by_name(
				QueueName = q_name_jc,
				# QueueOwnerAWSAccountId='string'
				)
		#_Log(pprint.pformat(vars(queue), indent=2))
		#{ '_url': 'https://queue.amazonaws.com/998754746880/acorn-exps',
		#		  'meta': ResourceMeta('sqs', identifiers=[u'url'])}
		return queue
	except botocore.exceptions.ClientError as e:
		if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
			pass
		else:
			raise e

	_Log("The queue doesn't exists. Creating one ...")
	response = _bc.create_queue(QueueName = q_name_jc)
	# Default message retention period is 4 days.

	return sqs.get_queue_by_name(QueueName = q_name_jc)


msg_body_jc = "acorn-job-completion"

def _EnqJcMsg(q):
	_Log("Enq a job completion message ...")
	msg_attrs = {}

	for k, v in {
			# job_id for notifying the completion of the job to the job controller
			"job_id": _job_id
			# Job request msg handle to be deleted
			, "job_req_msg_recript_handle": _jr_sqs_msg_receipt_handle}.iteritems():
		msg_attrs[k] = {"StringValue": v, "DataType": "String"}

	q.send_message(MessageBody=msg_body_jc, MessageAttributes=msg_attrs)


# Loading the Youtube data file form EBS takes long, and could make a big
# difference among nodes in different regions, which varies the start times of
# Youtube clients in different regions.
def _UnzipAcornDataToLocalSsd():
	_Log("Unzip Acorn data to local SSD ...")

	dn_in = "/home/ubuntu/work/acorn-data"
	dn_out = "/mnt/local-ssd0/work/acorn-data"

	try:
		os.makedirs(dn_out)
	except OSError as e:
		if e.errno == errno.EEXIST and os.path.isdir(dn_out):
			pass
		else:
			raise

	fn_youtube_reqs = None
	with open(_fn_acorn_youtube_yaml, "r") as fo:
		doc = yaml.load(fo)
		fn_youtube_reqs = doc["fn_youtube_reqs"]

	fn_in = "%s/%s.7z" % (dn_in, fn_youtube_reqs)
	cmd = "7z e -y -o%s %s" % (dn_out, fn_in)
	Util.RunSubp(cmd)

	# Used to use vmtouch
	#cmd = "/usr/local/bin/vmtouch -t -f %s" % self.fn


_jr_sqs_url = None
_jr_sqs_msg_receipt_handle = None
_num_regions = None
_tags = {}
_job_id = None

def main(argv):
	try:
		# This script is run under the user 'ubuntu'.

		if len(argv) != 5:
			raise RuntimeError("Unexpected argv %s" % argv)

		global _jr_sqs_url, _jr_sqs_msg_receipt_handle, _num_regions
		_jr_sqs_url = argv[1]
		_jr_sqs_msg_receipt_handle = argv[2]
		_num_regions = int(argv[3])
		tags_str = argv[4]

		global _tags
		for t in tags_str.split(","):
			t1 = t.split(":")
			if len(t1) != 2:
				raise RuntimeError("Unexpected format %s" % t1)
			_tags[t1[0]] = t1[1]
		_Log("tags:\n%s" % "\n".join(["  %s:%s" % (k, v) for (k, v) in sorted(_tags.items())]))

		global _job_id
		_job_id = _tags["job_id"]

		global _az, _region
		_az = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone", print_cmd = False, print_output = False)
		_region = _az[:-1]

		_SetHostname()
		_SyncTime()
		#_InstallPkgs()
		_MountAndFormatLocalSSDs()

		_PingAllNodes()
		_UploadResult()

		# Manually terminate the cluster. You don't know which one will be the last.
	except Exception as e:
		msg = "Exception: %s\n%s" % (e, traceback.format_exc())
		_Log(msg)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

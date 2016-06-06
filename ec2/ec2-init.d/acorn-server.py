#!/usr/bin/env python

import base64
import datetime
import os
import sys
import threading
import time
import traceback

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Cons
import Util

sys.path.insert(0, "%s" % os.path.dirname(__file__))
import GetIPs

_fo_log = None


def _Log(msg):
	fn = "/var/log/acorn/ec2-init.log"
	global _fo_log
	if _fo_log == None:
		_fo_log = open(fn, "a")
	_fo_log.write("%s: %s\n" % (datetime.datetime.now().strftime("%y%m%d-%H%M%S"), msg))
	_fo_log.flush()


def _RunSubp(cmd, shell = False):
	_Log(cmd)
	r = Util.RunSubp(cmd, shell = shell, print_cmd = False, print_result = False)
	if len(r.strip()) > 0:
		_Log(Util.Indent(r, 2))


_region = None

def _SetHostname():
	az = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone", print_cmd = False, print_result = False)
	global _region
	_region = az[:-1]

	# Hostname consists of availability zone name and launch req datetime
	hn = "%s-%s" % (az, _tags["job_id"])

	# http://askubuntu.com/questions/9540/how-do-i-change-the-computer-name
	cmd = "sudo sh -c 'echo \"%s\" > /etc/hostname'" % hn
	Util.RunSubp(cmd, shell=True)
	cmd = "sudo sed -i '/^127.0.0.1 localhost.*/c\\127.0.0.1 localhost %s' /etc/hosts" % hn
	Util.RunSubp(cmd, shell=True)
	cmd = "sudo service hostname restart"
	Util.RunSubp(cmd)


def _SyncTime():
	# Sync time. Important for Cassandra.
	# http://askubuntu.com/questions/254826/how-to-force-a-clock-update-using-ntp
	_Log("Synching time ...")
	_RunSubp("sudo service ntp stop")

	# Fails with a rc 1 in the init script. Mask with true for now.
	_RunSubp("sudo /usr/sbin/ntpd -gq || true", shell = True)

	_RunSubp("sudo service ntp start")


def _InstallPkgs():
	_RunSubp("sudo apt-get update && sudo apt-get install -y pssh dstat", shell = True)


def _MountAndFormatLocalSSDs():
	# Make sure we are using the known machine types
	inst_type = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/instance-type", print_cmd = False, print_result = False)
	if not inst_type.startswith("c3."):
		raise RuntimeError("Unexpected instance type %s" % inst_type)

	ssds = ["ssd0", "ssd1"]
	devs = ["xvdb", "xvdc"]

	for i in range(2):
		_Log("Setting up Local %s ..." % ssds[i])
		_RunSubp("sudo umount /dev/%s || true" % devs[i], shell=True)
		_RunSubp("sudo mkdir -p /mnt/local-%s" % ssds[i])

		# Instance store volumes come TRIMmed when they are allocated. Without
		# nodiscard, it takes about 80 secs for a 800GB SSD.
		_RunSubp("sudo mkfs.ext4 -m 0 -E nodiscard -L local-%s /dev/%s" % (ssds[i], devs[i]), shell=True)

		# -o discard for TRIM
		_RunSubp("sudo mount -t ext4 -o discard /dev/%s /mnt/local-%s" % (devs[i], ssds[i]), shell=True)
		_RunSubp("sudo chown -R ubuntu /mnt/local-%s" % ssds[i], shell=True)


def _CloneAcornSrcAndBuild():
	_RunSubp("mkdir -p /mnt/local-ssd0/work")
	_RunSubp("rm -rf /mnt/local-ssd0/work/acorn")
	_RunSubp("git clone https://github.com/hobinyoon/apache-cassandra-3.0.5-src.git /mnt/local-ssd0/work/apache-cassandra-3.0.5-src")
	_RunSubp("rm -rf /home/ubuntu/work/acorn")
	_RunSubp("ln -s /mnt/local-ssd0/work/apache-cassandra-3.0.5-src /home/ubuntu/work/acorn")
	# Note: report progress. clone done.

	# http://stackoverflow.com/questions/26067350/unmappable-character-for-encoding-ascii-but-my-files-are-in-utf-8
	_RunSubp("cd /home/ubuntu/work/acorn && (JAVA_TOOL_OPTIONS=-Dfile.encoding=UTF8 ant)", shell = True)
	# Note: report progress. build done.


def _EditCassConf():
	_Log("Getting IP addrs of all running instances of tags %s ..." % _tags)
	ips = GetIPs.GetByTags(_tags)
	_Log(ips)

	fn_cass_yaml = "/home/ubuntu/work/acorn/conf/cassandra.yaml"
	_Log("Editing %s ..." % fn_cass_yaml)

	# Update cassandra cluster name if specified.
	if "cass_cluster_name" in _tags:
		# http://stackoverflow.com/questions/7517632/how-do-i-escape-double-and-single-quotes-in-sed-bash
		_RunSubp("sed -i 's/^cluster_name: .*/cluster_name: '\"'\"'%s'\"'\"'/g' %s"
				% (_tags["cass_cluster_name"], fn_cass_yaml)
				, shell = True)

	cmd = "sed -i 's/" \
			"^          - seeds: .*" \
			"/          - seeds: \"%s\"" \
			"/g' %s" % (",".join(ips), fn_cass_yaml)
	_RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^listen_address: localhost" \
			"/#listen_address: localhost" \
			"/g' %s" % fn_cass_yaml
	_RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^# listen_interface: eth0" \
			"/listen_interface: eth0" \
			"/g' %s" % fn_cass_yaml
	_RunSubp(cmd, shell = True)

	# sed doesn't support "?"
	#   http://stackoverflow.com/questions/4348166/using-with-sed
	cmd = "sed -i 's/" \
			"^\(# \|\)broadcast_address: .*" \
			"/broadcast_address: %s" \
			"/g' %s" % (GetIPs.GetMyPubIp(), fn_cass_yaml)
	_RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^rpc_address: localhost" \
			"/#rpc_address: localhost" \
			"/g' %s" % fn_cass_yaml
	_RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^# rpc_interface: eth1" \
			"/rpc_interface: eth0" \
			"/g' %s" % fn_cass_yaml
	_RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^\(# \|\)broadcast_rpc_address: .*" \
			"/broadcast_rpc_address: %s" \
			"/g' %s" % (GetIPs.GetMyPubIp(), fn_cass_yaml)
	_RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^endpoint_snitch:.*" \
			"/endpoint_snitch: Ec2MultiRegionSnitch" \
			"/g' %s" % fn_cass_yaml
	_RunSubp(cmd, shell = True)

	# Edit parameters requested from tags
	for k, v in _tags.iteritems():
		if k.startswith("acorn_options."):
			#              01234567890123
			k1 = k[14:]
			cmd = "sed -i 's/" \
					"^    %s:.*" \
					"/%s: %s" \
					"/g' %s" % (k1, k1, v, fn_cass_yaml)
			_RunSubp(cmd, shell = True)


def _EditYoutubeClientConf():
	fn = "/home/ubuntu/work/acorn/acorn/clients/youtube/acorn-youtube.yaml"
	_Log("Editing %s ..." % fn)
	for k, v in _tags.iteritems():
		if k.startswith("acorn-youtube."):
			#              01234567890123
			k1 = k[14:]
			cmd = "sed -i 's/" \
					"^%s:.*" \
					"/%s: %s" \
					"/g' %s" % (k1, k1, v, fn)
			_RunSubp(cmd, shell = True)


def _RunCass():
	_Log("Running Cassandra ...")
	_RunSubp("rm -rf ~/work/acorn/data")
	_RunSubp("/home/ubuntu/work/acorn/bin/cassandra")


def _WaitUntilYouSeeAllCassNodes():
	_Log("Wait until you see all Cassandra nodes ...")
	# Keep checking until you see all nodes are up -- "UN" status.
	while True:
		# Get all IPs with the tags. Hope every node sees all other nodes by this
		# time.
		ips = GetIPs.GetByTags(_tags)
		num_nodes = _RunSubp("/home/ubuntu/work/acorn/bin/nodetool status | grep \"^UN \" | wc -l", shell = True)
		if num_nodes == len(ips):
			break
		time.sleep(2)


def _RunYoutubeClient():
	# Start the experiment from the master (or the leader) node.
	if _region != "us-east-1":
		return
	cmd = "%s/work/acorn/acorn/clients/youtube/run-youtube-cluster.py" % os.path.expanduser('~')
	_RunSubp(cmd)


def _DeqJobReqMsgEnqJobDoneMsg():
	if _region != "us-east-1":
		return

	_DeqJobReqMsg()
	_EnqJobDoneMsg()


_sqs_region = "us-east-1"
_bc = None

def _DeqJobReqMsg():
	# Delete the request message from the request queue. Should be done here. The
	# controller node, which launches a cluster, doesn't know when an experiment
	# is done.

	_Log("Deleting the job request message: url: %s, receipt_handle: %s" % (_jr_sqs_url, _jr_sqs_msg_receipt_handle))
	global _bc
	_bc = boto3.client("sqs", region_name = _sqs_region)
	response = _bc.delete_message(
			QueueUrl = _jr_sqs_url,
			ReceiptHandle = _jr_sqs_msg_receipt_handle 
			)
	_Log(pprint.pformat(response, indent=2))


def _EnqJobDoneMsg():
	_Log("Posting a job completion message ...")

	# Post a "job done" message to the job completed queue, so that the
	# controller node can shutdown the cluster.

	q = _GetJcQ()
	_EnqJcMsg(q)


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
		#_Log(pprint.pformat(e, indent=2))
		#_Log(pprint.pformat(vars(e), indent=2))
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
	# _tags contains job_id, which is used to terminate the cluster
	_Log("Enq a job completion message ...")
	msg_attrs = {}
	for k, v in _tags.iteritems():
		msg_attrs[k] = {"StringValue": v, "DataType": "String"}
	q.send_message(MessageBody=msg_body_jc, MessageAttributes={msg_attrs})


def _CacheEbsDataFileIntoMemory():
	fn = "/home/ubuntu/work/acorn-data/tweets-010"
	if "acorn-youtube.fn_youtube_reqs" in _tags:
		fn = "/home/ubuntu/work/acorn-data/%s" % _tags["acorn-youtube.fn_youtube_reqs"]
	_RunSubp("/usr/local/bin/vmtouch -t -f %s" % fn)


_jr_sqs_url = None
_jr_sqs_msg_receipt_handle = None
_tags = {}

def main(argv):
	try:
		# This script is run under the user 'ubuntu'.

		if len(argv) != 4:
			raise RuntimeError("Unexpected argv %s" % argv)

		global _jr_sqs_url, _jr_sqs_msg_receipt_handle
		_jr_sqs_url = argv[1]
		_jr_sqs_msg_receipt_handle = argv[2]

		global _tags
		_tags = argv[3]

		# Loading the Youtube data file form EBS takes long, like up to 5 mins, and
		# could make a big difference among nodes in different regions, which
		# varies the start times of Youtube clients in different regions.
		t = threading.Thread(target=_CacheEbsDataFileIntoMemory)
		# So that it can (abruptly) terminate on SIGINT
		t.daemon = True
		t.start()

		_SetHostname()
		_SyncTime()
		_InstallPkgs()
		_MountAndFormatLocalSSDs()
		_CloneAcornSrcAndBuild()
		_EditCassConf()
		_EditYoutubeClientConf()
		_RunCass()

		t.join()

		_WaitUntilYouSeeAllCassNodes()
		_RunYoutubeClient()
		_DeqJobReqMsgEnqJobDoneMsg()
	except Exception as e:
		msg = "Exception: %s\n%s" % (e, traceback.format_exc())
		_Log(msg)
		Cons.P(msg)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

#!/usr/bin/env python

import datetime
import os
import sys
import traceback

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util

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


def _SetHostname(tags):
	az = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone", print_cmd = False, print_result = False)
	# Hostname consists of availability zone name and acorn experiment parameters
	hn = "%s-%s" % (az, tags["acorn_exp_param"])

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


def _EditCassConf(tags):
	_Log("Getting IP addrs of all running instances of tags %s ..." % tags)

	cluster_name = tags["cluster_name"]

	ips = GetIPs.GetByTags(tags)
	_Log(ips)

	_Log("Editing conf/cassandra.yaml ...")
	# http://stackoverflow.com/questions/7517632/how-do-i-escape-double-and-single-quotes-in-sed-bash
	_RunSubp("sed -i 's/^cluster_name: .*/cluster_name: '\"'\"'%s'\"'\"'/g' /home/ubuntu/work/acorn/conf/cassandra.yaml" % cluster_name
			, shell = True)

	cmd = "sed -i 's/" \
			"^          - seeds: .*" \
			"/          - seeds: \"%s\"" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml" % ",".join(ips)
	_RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^listen_address: localhost" \
			"/#listen_address: localhost" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml"
	_RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^# listen_interface: eth0" \
			"/listen_interface: eth0" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml"
	_RunSubp(cmd, shell = True)

	# sed doesn't support ?
	#   http://stackoverflow.com/questions/4348166/using-with-sed
	cmd = "sed -i 's/" \
			"^\(# \|\)broadcast_address: .*" \
			"/broadcast_address: %s" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml" % GetIPs.GetMyPubIp()
	_RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^rpc_address: localhost" \
			"/#rpc_address: localhost" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml"
	_RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^# rpc_interface: eth1" \
			"/rpc_interface: eth0" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml"
	_RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^\(# \|\)broadcast_rpc_address: .*" \
			"/broadcast_rpc_address: %s" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml" % GetIPs.GetMyPubIp()
	_RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^endpoint_snitch:.*" \
			"/endpoint_snitch: Ec2MultiRegionSnitch" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml"
	_RunSubp(cmd, shell = True)


def _RunCass():
	_Log("Running Cassandra ...")
	_RunSubp("rm -rf ~/work/acorn/data")
	_RunSubp("/home/ubuntu/work/acorn/bin/cassandra")

	# Check if all nodes are joined
	_RunSubp("/home/ubuntu/work/acorn/bin/nodetool status")


def _CacheEbsDataFileIntoMemory():
	_RunSubp("/usr/local/bin/vmtouch -t /home/ubuntu/work/acorn-data/150812-143151-tweets-5667779")


def main(argv):
	try:
		# This script is run under the user 'ubuntu'.

		if len(argv) != 2:
			raise RuntimeError("Unexpected argv %s" % argv)
		tags_str = argv[1]

		tags = {}
		for kv in tags_str.split(","):
			t = kv.split(":")
			if len(t) != 2:
				raise RuntimeError("Unexpected kv=[%s]" % kv)
			tags[t[0]] = t[1]

		_SetHostname(tags)
		_SyncTime()
		_InstallPkgs()
		_MountAndFormatLocalSSDs()
		_CloneAcornSrcAndBuild()

		_EditCassConf(tags)
		_RunCass()
		_CacheEbsDataFileIntoMemory()

	except RuntimeError as e:
		msg = "Exception: %s\n%s" % (e, traceback.format_exc())
		_Log(msg)
		Cons.P(msg)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

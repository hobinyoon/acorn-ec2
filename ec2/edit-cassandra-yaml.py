#!/usr/bin/env python

import os
import sys

sys.path.insert(0, "%s/../util/python" % os.path.dirname(os.path.realpath(__file__)))
import Cons
import Util

sys.path.insert(0, "%s/ec2-init.d" % os.path.dirname(os.path.realpath(__file__)))
import GetIPs


def _EditCassConf():
	tag_name = "acorn-server"
	Cons.P("Getting IP addrs of all running %s instances ..." % tag_name)
	ips = GetIPs.GetByTag(tag_name)
	_Log(ips)

	_Log("Editing conf/cassandra.yaml ...")
	# http://stackoverflow.com/questions/7517632/how-do-i-escape-double-and-single-quotes-in-sed-bash
	Util.RunSubp("sed -i 's/^cluster_name: .*/cluster_name: '\"'\"'acorn'\"'\"'/g' /home/ubuntu/work/acorn/conf/cassandra.yaml", shell = True)

	cmd = "sed -i 's/" \
			"^          - seeds: .*" \
			"/          - seeds: \"%s\"" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml" % ",".join(ips)
	Util.RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^listen_address: localhost" \
			"/#listen_address: localhost" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml"
	Util.RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^# listen_interface: eth0" \
			"/listen_interface: eth0" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml"
	Util.RunSubp(cmd, shell = True)

	# sed doesn't support ?
	#   http://stackoverflow.com/questions/4348166/using-with-sed
	cmd = "sed -i 's/" \
			"^\(# \|\)broadcast_address: .*" \
			"/broadcast_address: %s" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml" % GetIPs.GetMyPubIp()
	Util.RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^rpc_address: localhost" \
			"/#rpc_address: localhost" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml"
	Util.RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^# rpc_interface: eth1" \
			"/rpc_interface: eth0" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml"
	Util.RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^\(# \|\)broadcast_rpc_address: .*" \
			"/broadcast_rpc_address: %s" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml" % GetIPs.GetMyPubIp()
	Util.RunSubp(cmd, shell = True)

	cmd = "sed -i 's/" \
			"^endpoint_snitch:.*" \
			"/endpoint_snitch: Ec2MultiRegionSnitch" \
			"/g' /home/ubuntu/work/acorn/conf/cassandra.yaml"
	Util.RunSubp(cmd, shell = True)


def main(argv):
	_EditCassConf()


if __name__ == "__main__":
	sys.exit(main(sys.argv))

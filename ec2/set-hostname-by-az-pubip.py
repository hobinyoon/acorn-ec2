#!/usr/bin/env python

# Run inside an EC2 instance

import sys

sys.path.insert(0, "../util/python")
import Cons
import Util


def _SetHostnameWithAzPubIp():
	with Cons.MeasureTime("Setting hostname"):
		az = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone")
		pub_ip = Util.RunSubp("curl -s http://169.254.169.254/latest/meta-data/public-ipv4")
		hn = "%s-%s" % (az, pub_ip.split(".")[-1])
		Cons.P("hostname: %s" % hn)

		# http://askubuntu.com/questions/9540/how-do-i-change-the-computer-name
		cmd = "sudo sh -c 'echo \"%s\" > /etc/hostname'" % hn
		Util.RunSubp(cmd, shell=True)
		cmd = "sudo sed -i '/^127.0.0.1 localhost.*/c\\127.0.0.1 localhost %s' /etc/hosts" % hn
		Util.RunSubp(cmd, shell=True)
		cmd = "sudo service hostname restart"
		Util.RunSubp(cmd)


def main(argv):
	_SetHostnameWithAzPubIp()


if __name__ == "__main__":
	sys.exit(main(sys.argv))

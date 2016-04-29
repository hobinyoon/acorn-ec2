#!/usr/bin/env python

# Run inside an EC2 instance

import sys

sys.path.insert(0, "../util/python")
import Cons
import Util


def main(argv):
	ssds = ["ssd0", "ssd1"]
	devs = ["xvdb", "xvdc"]

	for i in range(2):
		with Cons.MeasureTime("Setting up Local %s ..." % ssds[i]):
			Util.RunSubp("sudo mkdir -p /mnt/local-%s" % ssds[i], shell=True)
			Util.RunSubp("sudo umount /dev/%s || true" % devs[i], shell=True)

			# Instance store volumes come TRIMmed when they are allocated. Without
			# nodiscard, it takes about 80 secs for a 800GB SSD.
			Util.RunSubp("sudo mkfs.ext4 -m 0 -E nodiscard -L local-%s /dev/%s" % (ssds[i], devs[i]), shell=True)

			# -o discard for TRIM
			Util.RunSubp("sudo mount -t ext4 -o discard /dev/%s /mnt/local-%s" % (devs[i], ssds[i]), shell=True)
			Util.RunSubp("sudo chown -R ubuntu /mnt/local-%s" % ssds[i], shell=True)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

import os
import sys

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Util

import ConsMt


def Sync():
	dn = "%s/work/acorn-data" % os.path.expanduser("~")
	Util.MkDirs(dn)

	# http://docs.aws.amazon.com/cli/latest/reference/s3/sync.html
	cmd = "aws s3 sync s3://acorn-youtube %s" % dn
	out = Util.RunSubp(cmd, shell = True, print_cmd = True, print_result = False)
	for line in out.split("\n"):
		if len(line.strip()) > 0:
			ContMt.P("  %s" % line)


def Test():
	Sync()


def main(argv):
	Test()


if __name__ == "__main__":
	sys.exit(main(sys.argv))

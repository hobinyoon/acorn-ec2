import os
import sys

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Util


def Sync():
	dn = "%s/work/acorn-data" % os.path.expanduser("~")
	Util.MkDirs(dn)

	# http://docs.aws.amazon.com/cli/latest/reference/s3/sync.html
	cmd = "aws s3 sync s3://acorn-youtube %s" % dn
	Util.RunSubp(cmd, shell = True, print_cmd = True, print_result = True)


def Test():
	Sync()


def main(argv):
	Test()


if __name__ == "__main__":
	sys.exit(main(sys.argv))

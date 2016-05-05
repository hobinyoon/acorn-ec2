# -*- coding: utf-8 -*-

def GetLatestAmiId(region):
	ami_id = None

	# N. Virginia
	if region == "us-east-1":
		ami_id = "ami-1ef81873"

	# N. California
	elif region == "us-west-1":
		ami_id = "ami-7f10691f"

	# Oregon
	elif region == "us-west-2":
		ami_id = "ami-af887acf"

	# Ireland
	elif region == "eu-west-1":
		ami_id = "ami-fd6de58e"

	# Frankfurt
	elif region == "eu-central-1":
		ami_id = "ami-036d8f6c"

	# Singapore
	elif region == "ap-southeast-1":
		ami_id = "ami-a135e2c2"

	# Tokyo
	elif region == "ap-northeast-1":
		ami_id = "ami-158e947b"

	# Seoul
	elif region == "ap-northeast-2":
		ami_id = "ami-a74d85c9"

	# Sydney
	elif region == "ap-southeast-2":
		ami_id = "ami-583c103b"

	# São Paulo
	elif region == "sa-east-1":
		ami_id = "ami-2c5dd440"

	else:
		raise RuntimeError("Unexpected region %s" % region)

	return ami_id

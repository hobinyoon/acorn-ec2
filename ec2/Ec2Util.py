# -*- coding: utf-8 -*-

def GetLatestAmiId(region):
	ami_id = None

	# N. Virginia
	if region == "us-east-1":
		ami_id = "ami-4f44ad22"

	# N. California
	elif region == "us-west-1":
		ami_id = "ami-c37109a3"

	# Oregon
	elif region == "us-west-2":
		ami_id = "ami-884ab6e8"

	# Ireland
	elif region == "eu-west-1":
		ami_id = "ami-b440d4c7"

	# Frankfurt
	elif region == "eu-central-1":
		ami_id = "ami-acec00c3"

	# Singapore
	elif region == "ap-southeast-1":
		ami_id = "ami-3620f655"

	# Tokyo
	elif region == "ap-northeast-1":
		ami_id = "ami-08f31269"

	# Seoul
	elif region == "ap-northeast-2":
		ami_id = "ami-30864d5e"

	# Sydney
	elif region == "ap-southeast-2":
		ami_id = "ami-36250a55"

	# São Paulo
	elif region == "sa-east-1":
		ami_id = "ami-af4bc3c3"

	else:
		raise RuntimeError("Unexpected region %s" % region)

	return ami_id

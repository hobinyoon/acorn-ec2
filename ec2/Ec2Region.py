def GetLatestAmiId(region):
	region_ami = {
			"ap-northeast-1": "ami-1ed9287f"
			, "ap-northeast-2": "ami-7a8a4014"
			, "ap-south-1": "ami-d791fbb8"
			, "ap-southeast-1": "ami-8c33e1ef"
			, "ap-southeast-2": "ami-1a83ab79"
			, "eu-central-1": "ami-a3e70ccc"
			, "eu-west-1": "ami-edd04a9e"
			, "sa-east-1": "ami-8ea237e2"
			, "us-east-1": "ami-00f33d6d"
			, "us-west-1": "ami-f7064197"
			, "us-west-2": "ami-7ef83e1e"
			}

	return region_ami[region]


def All():
	return [
			"ap-northeast-1"
			, "ap-northeast-2"
			, "ap-south-1"
			, "ap-southeast-1"
			, "ap-southeast-2"
			, "eu-central-1"
			, "eu-west-1"
			, "sa-east-1"
			, "us-east-1"
			, "us-west-1"
			, "us-west-2"
			]

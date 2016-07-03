def GetLatestAmiId(region):
	region_ami = {
			"ap-northeast-1": "ami-daed1ebb"
			, "ap-northeast-2": "ami-7a874d14"
			, "ap-south-1": "ami-6d91fb02"
			, "ap-southeast-1": "ami-70bd6013"
			, "ap-southeast-2": "ami-9b3813f8"
			, "eu-central-1": "ami-d047acbf"
			, "eu-west-1": "ami-ec9cf99f"
			, "sa-east-1": "ami-29188d45"
			, "us-east-1": "ami-1a7dc70d"
			, "us-west-1": "ami-ea92d58a"
			, "us-west-2": "ami-1b40817b"
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

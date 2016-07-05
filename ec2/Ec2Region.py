def GetLatestAmiId(region):
	region_ami = {
			"ap-northeast-1": "ami-08b84a69"
			, "ap-northeast-2": "ami-61915b0f"
			, "ap-south-1": "ami-b8e68cd7"
			, "ap-southeast-1": "ami-50d40933"
			, "ap-southeast-2": "ami-fc13389f"
			, "eu-central-1": "ami-9b8862f4"
			, "eu-west-1": "ami-2c1d795f"
			, "sa-east-1": "ami-ab33a6c7"
			, "us-east-1": "ami-a742c7b0"
			, "us-west-1": "ami-b9b1f6d9"
			, "us-west-2": "ami-f58e4e95"
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

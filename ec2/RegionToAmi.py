def GetLatestAmiId(region):
	region_ami = {
			"ap-northeast-1": "ami-c94ca7a8"
			, "ap-southeast-1": "ami-1bd40778"
			, "ap-southeast-2": "ami-d2f6dfb1"
			, "eu-central-1": "ami-9eda33f1"
			, "eu-west-1": "ami-8b5ec1f8"
			, "sa-east-1": "ami-80bf35ec"
			, "us-east-1": "ami-abf83cc6"
			, "us-west-1": "ami-e5602585"
			, "us-west-2": "ami-3244be52"
			}

	return region_ami[region]

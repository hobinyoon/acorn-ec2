def GetLatestAmiId(region):
	region_ami = {
			"ap-northeast-1": "ami-687f8909"
			, "ap-southeast-1": "ami-d1ee3cb2"
			, "ap-southeast-2": "ami-dbd4fcb8"
			, "eu-central-1": "ami-08bb5067"
			, "eu-west-1": "ami-74be2507"
			, "sa-east-1": "ami-bceb7ed0"
			, "us-east-1": "ami-4e975823"
			, "us-west-1": "ami-afcf8bcf"
			, "us-west-2": "ami-75814715"
			}

	return region_ami[region]

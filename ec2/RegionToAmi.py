def GetLatestAmiId(region):
	region_ami = {
		"us-east-1": "ami-f051949d"
		, "us-west-1"      : "ami-17480d77"
		, "us-west-2"      : "ami-1d69937d"
		, "eu-west-1"      : "ami-58e37c2b"
		, "eu-central-1"   : "ami-27f71e48"
		, "ap-southeast-1" : "ami-aba774c8"
		, "ap-southeast-2" : "ami-bbe4cdd8"
		, "ap-northeast-1" : "ami-a307ecc2"
		, "sa-east-1"      : "ami-199c1675"
		}

	return region_ami[region]

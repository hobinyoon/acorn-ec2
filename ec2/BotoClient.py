import boto3
import os
import sys

sys.path.insert(0, "%s/../util/python" % os.path.dirname(__file__))

_region_bc = {}

def Get(region):
	if region not in _region_bc:
		_region_bc[region] = boto3.session.Session().client("ec2", region_name = region)
	return _region_bc[region]

def Reset(region):
	_region_bc.pop(region, None)

import datetime
import os
import sys
import threading
import traceback

sys.path.insert(0, "%s/../util/python" % os.path.dirname(__file__))
import Cons
import Util

import BotoClient


_num_checked = 0
_num_checked_lock = threading.Lock()
_az_price = None
_az_price_lock = threading.Lock()


def GetTheLowestMaxPriceAZs(region_spot_req):
	_Reset()

	Cons.Pnnl("Checking spot prices:")
	global _num_checked
	_num_checked = 0

	# {region, CheckSpotPrice()}
	csps = {}
	for region, spot_req_params in region_spot_req.iteritems():
		inst_type = spot_req_params["inst_type"]
		csps[region] = CheckSpotPrice(region, inst_type)

	threads = []
	for region, v in csps.iteritems():
		t = threading.Thread(target=v.Run)
		t.daemon = True
		threads.append(t)
		t.start()

	for t in threads:
		t.join()
	Cons.P("")

	Cons.P("Region         inst_type az   cur 1d-avg 1d-max")
	region_az_lowest_price = {}
	for region, v in sorted(csps.iteritems()):
		v.Print()
		region_az_lowest_price[region] = v.AzLowestPrice()
	return region_az_lowest_price


def GetCurPrice(az):
	with _az_price_lock:
		return _az_price[az].cur


def _Reset():
	global _az_price
	_az_price = {}


class CheckSpotPrice():
	def __init__(self, region, inst_type):
		self.region = region
		self.inst_type = inst_type
		self.az_price = {}


	def Run(self):
		try:
			now = datetime.datetime.now()
			one_day_ago = now - datetime.timedelta(days=2)

			r = BotoClient.Get(self.region).describe_spot_price_history(
					StartTime = one_day_ago,
					EndTime = now,
					ProductDescriptions = ["Linux/UNIX"],
					InstanceTypes = [self.inst_type],
					)

			# {az: {timestamp: price} }
			az_ts_price = {}
			for sp in r["SpotPriceHistory"]:
				az = sp["AvailabilityZone"]
				ts = sp["Timestamp"]
				sp = float(sp["SpotPrice"])
				if az not in az_ts_price:
					az_ts_price[az] = {}
				az_ts_price[az][ts] = sp

			for az, v in sorted(az_ts_price.iteritems()):
				ts_prev = None
				price_prev = None
				dur_sum = 0
				dur_price_sum = 0.0
				price_max = 0.0
				price_avg = 0.0
				for ts, price in sorted(v.iteritems()):
					if ts_prev is not None:
						dur = (ts - ts_prev).total_seconds()
						dur_sum += dur
						dur_price_sum += (dur * price)

					price_max = max(price, price_max)
					ts_prev = ts
					price_prev = price
				if dur_sum != 0.0:
					price_avg = dur_price_sum / dur_sum
				price_cur = price_prev
				p = CurSpotPrice(az, price_cur, price_avg, price_max)
				self.az_price[az] = p
				with _az_price_lock:
					_az_price[az] = p

			with _num_checked_lock:
				global _num_checked
				_num_checked += 1
				if _num_checked == 7:
					#                        Checking spot prices:
					Cons.sys_stdout_write("\n                     ")
				Cons.sys_stdout_write(" %s" % self.region)
		except Exception as e:
			Cons.P("%s\nregion=%s\n%s" % (e, self.region, traceback.format_exc()))
			os._exit(1)


	def Print(self):
		# ap-southeast-1
		# 01234567890123
		Cons.Pnnl("%-14s %-10s" % (self.region, self.inst_type))
		for az, p in sorted(self.az_price.iteritems()):
			Cons.sys_stdout_write(" %s" % p)
		Cons.P("")


	def AzLowestPrice(self):
		az_lowest = None
		p_lowest = None
		for az, p in self.az_price.iteritems():
			if az_lowest is None:
				az_lowest = az
				p_lowest = p.max
			else:
				if p.max < p_lowest:
					az_lowest = az
					p_lowest = p.max
		return az_lowest


class CurSpotPrice():
	def __init__(self, az, price_cur, price_avg, price_max):
		self.az = az
		self.cur = price_cur
		self.avg = price_avg
		self.max = price_max

	def __str__(self):
		return "%s %0.4f %0.4f %0.4f" % (self.az[-1:], self.cur, self.avg, self.max)

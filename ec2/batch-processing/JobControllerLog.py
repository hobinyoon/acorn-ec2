import os
import sys
import threading
import time
import traceback

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Cons
import Util


_log_lock = threading.Lock()

def P(msg):
	with _log_lock:
		startswith_newline = False
		if msg.startswith("\n"):
			startswith_newline = True
			msg = msg[1:]

		if startswith_newline:
			Cons.P("")
		m0 = "%s: %s" % (time.strftime("%y%m%d-%H%M%S"), msg)
		Cons.P(m0)

		dn = "%s/.log" % os.path.dirname(__file__)
		if not os.path.isdir(dn):
			Util.MkDirs(dn)

		with open("%s/job-controller" % dn, "a") as fo:
			fo.write("%s\n" % m0)

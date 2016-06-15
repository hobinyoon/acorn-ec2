import os
import sys
import threading

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Cons

_print_lock = threading.Lock()

def P(msg):
	with _print_lock:
		Cons.P(msg)

def Pnnl(msg):
	with _print_lock:
		Cons.Pnnl(msg)

def sys_stdout_write(msg):
	with _print_lock:
		sys.stdout.write(msg)
		sys.stdout.flush()

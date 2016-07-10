#!/usr/bin/env python

import Queue
import sys
import traceback

import InstMonitor
import JobCompletion
import JobControllerLog
import JobReq


def main(argv):
	try:
		JobControllerLog.P("Starting ...")
		PollMsgs()
	except KeyboardInterrupt as e:
		JobControllerLog.P("\nGot a keyboard interrupt. Stopping ...")
	except Exception as e:
		JobControllerLog.P("\nGot an exception: %s\n%s" % (e, traceback.format_exc()))
	# Deleting the job request queue is useful for preventing the job request
	# reappearing

	# You can temporarily disable this for dev, but needs to be very careful. You
	# can easily spend $1000 a night.
	JobReq.DeleteQ()


# Not sure if a Queue is necessary when the maxsize is 1. Leave it for now.
_q_jr = Queue.Queue(maxsize=1)
_q_jc = Queue.Queue(maxsize=1)

# General message queue
_q_general_msg = Queue.Queue(maxsize=10)

def PollMsgs():
	JobReq.PollBackground(_q_jr)
	JobCompletion.PollBackground(_q_jc)

	while True:
		with InstMonitor.IM():
			# Blocked waiting until a request is available
			#
			# Interruptable get
			#   http://stackoverflow.com/questions/212797/keyboard-interruptable-blocking-queue-in-python
			while True:
				try:
					msg = _q_general_msg.get(timeout=0.01)
					break
				except Queue.Empty:
					pass

				try:
					msg = InstMonitor.ClusterCleaner.Queue().get(timeout=0.01)
					break
				except Queue.Empty:
					pass

				try:
					msg = _q_jc.get(timeout=0.01)
					break
				except Queue.Empty:
					pass

				try:
					msg = _q_jr.get(timeout=0.01)
					break
				except Queue.Empty:
					pass

		if isinstance(msg, str):
			JobControllerLog.P("\nGot a message: %s" % msg)
		elif isinstance(msg, InstMonitor.ClusterCleaner.Msg):
			ProcessClusterCleanReq(msg)
		elif isinstance(msg, JobReq.Msg):
			JobReq.Process(msg, _q_general_msg)
		elif isinstance(msg, JobCompletion.Msg):
			JobCompletion.Process(msg)
		else:
			raise RuntimeError("Unexpected type %s" % type(msg))


def ProcessClusterCleanReq(req):
	job_id = req.job_id
	JobControllerLog.P("\nGot a cluster clean request. job_id:%s" % job_id)
	JobCompletion.TermCluster(job_id)


if __name__ == "__main__":
	sys.exit(main(sys.argv))

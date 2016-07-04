import boto3
import botocore
import os
import pprint
import sys
import threading
import traceback

sys.path.insert(0, "%s/../../util/python" % os.path.dirname(__file__))
import Cons


def PollBackground(jc_q):
	_Init()

	global _thr_poll
	_thr_poll = threading.Thread(target=_Poll, args=[jc_q])
	_thr_poll.daemon = True
	_thr_poll.start()


_initialized = False
_bc = None
_sqs = None
_sqs_region = "us-east-1"

def _Init():
	global _initialized
	if _initialized == False:
		global _bc, _sqs
		_bc = boto3.client("sqs", region_name = _sqs_region)
		_sqs = boto3.resource("sqs", region_name = _sqs_region)
		_initialized = True


def DeleteMsg(jc):
	Cons.P("Deleting the job completion msg ...")
	_bc = boto3.client("sqs", region_name = _sqs_region)
	#Cons.P(pprint.pformat(jc.msg))
	r = _bc.delete_message(
			QueueUrl = jc.msg.queue_url,
			ReceiptHandle = jc.msg.receipt_handle
			)
	Cons.P(pprint.pformat(r, indent=2))


def _Poll(jc_q):
	q = _GetQ()

	while True:
		try:
			msgs = q.receive_messages(
					#AttributeNames=[
					#	'Policy'|'VisibilityTimeout'|'MaximumMessageSize'|'MessageRetentionPeriod'|'ApproximateNumberOfMessages'|'ApproximateNumberOfMessagesNotVisible'|'CreatedTimestamp'|'LastModifiedTimestamp'|'QueueArn'|'ApproximateNumberOfMessagesDelayed'|'DelaySeconds'|'ReceiveMessageWaitTimeSeconds'|'RedrivePolicy',
					#	],
					MessageAttributeNames=["All"],
					MaxNumberOfMessages=1,
					VisibilityTimeout=60,
					WaitTimeSeconds=1
					)
			for m in msgs:
				# put the job completion msg. Wait when the queue is full.
				jc_q.put(JobCompleted(m), block=True, timeout=None)
		except botocore.exceptions.EndpointConnectionError as e:
			# Could not connect to the endpoint URL: "https://queue.amazonaws.com/"
			# Retrying after 1 sec doesn't seem to help. Might be the server being
			# unreliable. Just kill the server.
			Cons.P("%s\n%s" % (e, traceback.format_exc()))
			os._exit(1)
		except Exception as e:
			Cons.P("%s\n%s" % (e, traceback.format_exc()))
			os._exit(1)


class JobCompleted:
	msg_body_jc = "acorn-job-completion"

	def __init__(self, msg):
		if msg.body != JobCompleted.msg_body_jc:
			raise RuntimeError("Unexpected. msg.body=[%s]" % msg.body)
		if msg.receipt_handle is None:
			raise RuntimeError("Unexpected")
		if msg.message_attributes is None:
			raise RuntimeError("Unexpected")

		self.attrs = {}
		for k, v in msg.message_attributes.iteritems():
			if v["DataType"] != "String":
				raise RuntimeError("Unexpected")
			v1 = v["StringValue"]
			self.attrs[k] = v1
			#Cons.P("  %s: %s" % (k, v1))

		self.msg = msg


q_name_jc = "acorn-jobs-completed"

def _GetQ():
	# Get the queue. Create one if not exists.
	try:
		queue = _sqs.get_queue_by_name(
				QueueName = q_name_jc,
				# QueueOwnerAWSAccountId='string'
				)
		#Cons.P(pprint.pformat(vars(queue), indent=2))
		#{ '_url': 'https://queue.amazonaws.com/998754746880/acorn-exps',
		#		  'meta': ResourceMeta('sqs', identifiers=[u'url'])}
		return queue
	except botocore.exceptions.ClientError as e:
		if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
			pass
		else:
			raise e

	Cons.Pnnl("The queue doesn't exists. Creating one ")
	while True:
		response = None
		try:
			response = _bc.create_queue(QueueName = q_name_jc)
			# Default message retention period is 4 days.
			print ""
			break
		except botocore.exceptions.ClientError as e:
			# When calling the CreateQueue operation: You must wait 60 seconds after
			# deleting a queue before you can create another with the same name.
			# It doesn't give me how much more you need to wait. Polling until succeed.
			if e.response["Error"]["Code"] == "AWS.SimpleQueueService.QueueDeletedRecently":
				sys.stdout.write(".")
				sys.stdout.flush()
				time.sleep(2)
			else:
				raise e

	return _sqs.get_queue_by_name(QueueName = q_name_jc)

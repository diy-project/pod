import atexit
import boto3
import json
import logging
import time

from abc import abstractmethod, abstractproperty
from random import SystemRandom
from threading import Event, Lock, Thread

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


class Future(object):

    def __init__(self):
        self.event = Event()
        self.result = None
        self.aborted = False

    def get(self, timeout=None):
        self.event.wait(timeout)
        if self.result is None:
            self.aborted = True
        return self.result

    def set(self, result):
        self.result = result
        self.event.set()

    def isAborted(self):
        return self.aborted


class LambdaSqsTaskConfig(object):

    @abstractproperty
    def queue_prefix(self):
        pass

    @abstractproperty
    def lambda_function(self):
        pass

    @abstractproperty
    def max_workers(self):
        pass

    @abstractproperty
    def load_factor(self):
        pass

    @abstractproperty
    def result_attributes(self):
        """List of SQS message attributes required by the caller"""
        pass

    @abstractmethod
    def pre_invoke_callback(self, workerId, workerArgs):
        """Add any extra args to workerArgs"""
        pass

    @abstractmethod
    def post_return_callback(self, workerId, workerResponse):
        """Called on worker exit. WorkerResponse is None if there was
        an error"""
        pass


class WorkerManager(object):

    secureRandom = SystemRandom()
    lambdaClient = boto3.client('lambda')
    sqs = boto3.client('sqs')

    def __init__(self, taskConfig):
        self.config = taskConfig

        self.numWorkers = 0
        self.numWorkersLock = Lock()

        # RequestId -> Future
        self.tasksInProgress = {}

        self._init_message_queues(taskConfig.queue_prefix)

        resultThread = Thread(self._result_daemon)
        resultThread.daemon = True
        resultThread.start()

    def _init_message_queues(self, prefix):
        # Setup the message queues
        currentTime = time.time()
        queueAttributes = {
            'MessageRetentionPeriod': 60,
            'ReceiveMessageWaitTimeSeconds': 20
        }
        self.taskQueueName = '%s-task-%d' % (prefix, currentTime)
        self.taskQueue = self.sqs.create_queue(
            QueueName=self.taskQueueName,
            Attributes=queueAttributes)
        atexit.register(lambda x: x.sqs.delete_queue(x.taskQueue.url),
                        self)
        self.resultQueueName = '%s-result-%d' % (prefix, currentTime)
        self.resultQueue = self.sqs.create_queue(
            QueueName=self.resultQueueName,
            Attributes=queueAttributes)
        atexit.register(lambda x: x.sqs.delete_queue(x.resultQueue.url),
                        self)


    def execute(self, messageBody, messageAttributes=None, timeout=None):
        with self.numWorkersLock:
            if self._should_spawn_worker():
                self._spawn_new_worker()
        kwargs = {}
        if messageAttributes:
            kwargs['MessageAttributes'] = messageAttributes
        messageStatus = self.taskQueue.send_message(
            MessageBody=json.dumps(messageBody), **kwargs)

        # Use the MessageId as taskId
        taskId = messageStatus['MessageId']

        taskFuture = Future()
        self.tasksInProgress[taskId] = taskFuture

        result = taskFuture.get(timeout=timeout)
        return result

    def _should_spawn_worker(self):
        return (self.numWorkers == 0 or
                (self.numWorkers < self.config.max_workers and
                 len(self.tasksInProgress) >
                 self.numWorkers * self.config.load_factor))

    def _spawn_new_worker(self):
        workerId = self.secureRandom.getrandbits(32)
        workerArgs = {
            'workerId': workerId,
            'taskQueue': self.taskQueueName,
            'resultQueue': self.resultQueueName,
        }
        functionName = self.config.lambda_function
        self.config.pre_invoke_callback(workerId, workerArgs)
        t = Thread(target=self._wait_for_worker,
                   args=(functionName, workerId, workerArgs))
        t.daemon = True
        t.start()
        with self.numWorkersLock:
            self.numWorkers += 1

    def _wait_for_worker(self, functionName, workerId, workerArgs):
        try:
            response = self.lambdaClient.invoke(FunctionName=functionName,
                                                Payload=json.dumps(workerArgs))
            if response['StatusCode'] != 200:
                logger.error('Worker %d exited unexpectedly: %s: status=%d',
                             workerId,
                             response['FunctionError'],
                             response['StatusCode'])
                self.config.post_return_callback(workerId, None)
            else:
                workerResponse = json.loads(response['Payload'].read())
                self.config.post_return_callback(workerId, workerResponse)

        finally:
            with self.numWorkersLock:
                self.numWorkers -= 1

    def _result_daemon(self):
        while True:
            messages = self.sqs.receive_message(
                QueueUrl=self.resultQueue.url,
                AttributeNames=self.config.result_attributes,
                MessageAttributeNames=10)

            if messageId

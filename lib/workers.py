import atexit
import boto3
import json
import logging
import time

from abc import abstractmethod, abstractproperty
from random import SystemRandom
from threading import Condition, Event, Lock, Thread, Timer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


MAX_SQS_REQUEST_MESSAGES = 10


_moduleReady = False
_secureRandom = None
_lambda = None
_sqs = None
def _lazy_module_init():
    global _moduleReady
    if not _moduleReady:
        global _secureRandom, _lambda, _sqs
        _secureRandom = SystemRandom()
        _lambda = boto3.client('lambda')
        _sqs = boto3.client('sqs')
        _moduleReady = True


class Future(object):

    def __init__(self):
        self.__event = Event()
        self.__result = None
        self.__aborted = False

    def get(self, timeout=None):
        self.__event.wait(timeout)
        if self.__result is None:
            self.__aborted = True
        return self.__result

    def set(self, result):
        self.__result = result
        self.__event.set()

    @property
    def isAborted(self):
        return self.__aborted


class LambdaSqsTaskConfig(object):

    @abstractproperty
    def queue_prefix(self):
        """Prefix of the temporary SQS queues"""
        pass

    @abstractproperty
    def lambda_function(self):
        """Name of lambda function to call"""
        pass

    @abstractproperty
    def max_workers(self):
        pass

    @abstractproperty
    def load_factor(self):
        """Target ratio of pending tasks to workers"""
        pass

    def worker_wait_time(self):
        """Number of seconds each worker will wait for work"""
        return 1

    def message_retention_period(self):
        """
        Number of seconds each message will persist before
        timing our
        """
        return 60

    def pre_invoke_callback(self, workerId, workerArgs):
        """Add any extra args to workerArgs"""
        pass

    def post_return_callback(self, workerId, workerResponse):
        """
        Called on worker exit. WorkerResponse is None if there was
        an error
        """
        pass


class WorkerManager(object):

    def __init__(self, taskConfig):
        _lazy_module_init()

        self.__config = taskConfig

        self.__numWorkers = 0
        self.__numWorkersLock = Lock()

        # RequestId -> Future
        self.__numTasksInProgress = 0
        self.__tasksInProgress = {}
        self.__tasksInProgressLock = Lock()
        self.__tasksInProgressCondition = Condition(self.__tasksInProgressLock)

        self.__init_message_queues(taskConfig.queue_prefix)

        # Start result fetcher thread
        rt = Thread(self.__result_daemon)
        rt.daemon = True
        rt.start()

    def __init_message_queues(self, prefix):
        """Setup the message queues"""
        currentTime = time.time()
        queueAttributes = {
            'MessageRetentionPeriod': self.__config.message_retention_period,
            'ReceiveMessageWaitTimeSeconds': self.__config.worker_wait_time
        }

        taskQueueName = '%s-task-%d' % (prefix, currentTime)
        self.__taskQueueName = taskQueueName
        self.__taskQueue = _sqs.create_queue(
            QueueName=taskQueueName,
            Attributes=queueAttributes)
        taskQueueUrl = self.__taskQueue.url
        atexit.register(lambda: _sqs.delete_queue(taskQueueUrl))

        resultQueueName = '%s-result-%d' % (prefix, currentTime)
        self.__resultQueueName = resultQueueName
        self.__resultQueue = _sqs.create_queue(
            QueueName=resultQueueName,
            Attributes=queueAttributes)
        resultQueueUrl = self.__resultQueue.url
        atexit.register(lambda: _sqs.delete_queue(resultQueueUrl))


    def execute(self, messageBody, messageAttributes=None, timeout=None):
        """Enqueue a message in the task queue"""
        with self.__numWorkersLock:
            if self.__should_spawn_worker():
                self.__spawn_new_worker()

        kwargs = {}
        if messageAttributes:
            kwargs['MessageAttributes'] = messageAttributes
        messageStatus = self.__taskQueue.send_message(
            MessageBody=json.dumps(messageBody), **kwargs)

        # Use the MessageId as taskId
        taskId = messageStatus['MessageId']

        taskFuture = Future()
        with self.__tasksInProgressLock:
            self.__tasksInProgress[taskId] = taskFuture
            self.__numTasksInProgress = len(self.__tasksInProgress)
            self.__tasksInProgressCondition.notify_all()

        result = taskFuture.get(timeout=timeout)
        return result

    def __should_spawn_worker(self):
        return (self.__numWorkers == 0 or
                (self.__numWorkers < self.__config.max_workers and
                 self.__numTasksInProgress >
                 self.__numWorkers * self.__config.load_factor))

    def __spawn_new_worker(self):
        workerId = _secureRandom.getrandbits(32)
        workerArgs = {
            'workerId': workerId,
            'taskQueue': self.__taskQueueName,
            'resultQueue': self.__resultQueueName,
        }
        functionName = self.__config.lambda_function
        self.__config.pre_invoke_callback(workerId, workerArgs)
        t = Thread(target=self.__wait_for_worker,
                   args=(functionName, workerId, workerArgs))
        t.daemon = True
        t.start()
        self.__numWorkers += 1
        assert self.__numWorkers <= self.__config.max_workers,\
            'Max worker limit exceeded'

    def __wait_for_worker(self, functionName, workerId, workerArgs):
        """Wait for the worker to exit and the lambda to return"""
        try:
            response = _lambda.invoke(FunctionName=functionName,
                                      Payload=json.dumps(workerArgs))
            if response['StatusCode'] != 200:
                logger.error('Worker %d exited unexpectedly: %s: status=%d',
                             workerId,
                             response['FunctionError'],
                             response['StatusCode'])
                self.__config.post_return_callback(workerId, None)
            else:
                workerResponse = json.loads(response['Payload'].read())
                self.__config.post_return_callback(workerId, workerResponse)

        finally:
            with self.__numWorkersLock:
                self.__numWorkers -= 1
                assert self.__numWorkers >= 0, 'Workers cannot be negative'

    def __result_daemon(self):
        """Poll SQS result queue and set futures"""
        requiredAttributes = ['All']
        resultQueueUrl = self.__resultQueue.url
        while True:
            # Don't poll SQS unless there is a task in progress
            with self.__tasksInProgressLock:
                if self.__numTasksInProgress == 0:
                    self.__tasksInProgressCondition.wait()

            # Poll for new messages
            messages = None
            try:
                messages = self.__resultQueue.receive_message(
                    AttributeNames=requiredAttributes,
                    MessageAttributeNames=MAX_SQS_REQUEST_MESSAGES)
                for message in messages['messages']:
                    try:
                        taskId = message['MessageAttributes']['taskId']['StringValue']
                        with self.__tasksInProgressLock:
                            taskFuture = self.__tasksInProgress.get(taskId)
                            if taskFuture is None:
                                logger.debug('No future for task: %s', taskId)
                            else:
                                taskFuture.set(message)
                                del self.__tasksInProgress[taskId]
                                self.__numTasksInProgress = len(self.__tasksInProgress)
                    except Exception, e:
                        logger.error('Failed to parse message: %s', message)
                        logger.exception(e)
            except Exception, e:
                logger.error('Error polling SQS')
                logger.exception(e)
            finally:
                if messages is not None:
                    try:
                        result =self.__resultQueue.delete_messages(
                            Entries=[{
                                'Id': message['MessageId'],
                                'ReceiptHandle': message['ReceiptHandle']
                            } for message in messages]
                        )
                        if len(result['Successful']) != len(messages):
                            raise Exception('Failed to delete all messages: %s'
                                            % result['Failed'])
                    except Exception, e:
                        logger.exception(e)

    def __clean_up_aborted_tasks(self):
        """Clean up all orphaned tasks"""
        with self.__tasksInProgressLock:
            for k, v in self.__tasksInProgress.items():
                if v.isAborted: del self.__tasksInProgress[k]
            self.__numTasksInProgress = len(self.__tasksInProgress)

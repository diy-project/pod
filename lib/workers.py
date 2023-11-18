import atexit
import json
import logging
import random
import time

from abc import abstractproperty
from concurrent.futures import ThreadPoolExecutor
from threading import Condition, Event, Lock, Thread

from lib.stats import Stats, LambdaStatsModel, SqsStatsModel
from shared.workers import LambdaSqsResult, LambdaSqsTask

# Re-expose these classes
LambdaSqsResult = LambdaSqsResult
LambdaSqsTask = LambdaSqsTask

logger = logging.getLogger(__name__)

try:
    import boto3
except ImportError as e:
    logger.error('Failed to import boto3')
    boto3 = None

MAX_SQS_REQUEST_MESSAGES = 10

DEFAULT_POLLING_THREADS = 4
DEFAULT_HANDLER_THREADS = 4


class Future(object):

    def __init__(self):
        self.__done = Event()
        self.__result = None
        self.__aborted = False

        self._partial = {}

    def get(self, timeout=None):
        self.__done.wait(timeout)
        if self.__result is None:
            self.__aborted = True
        return self.__result

    def set(self, result):
        self.__result = result
        self.__done.set()

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

    @property
    def worker_wait_time(self):
        """Number of seconds each worker will wait for work"""
        return 1

    @property
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

    def __init__(self, taskConfig, stats=None):
        self.__config = taskConfig
        if stats is None:
            stats = Stats()
        self.__stats = stats

        if 'lambda' not in stats.models:
            stats.register_model('lambda', LambdaStatsModel())
        self.__lambdaStats = stats.get_model('lambda')

        if 'sqs' not in stats.models:
            stats.register_model('sqs', SqsStatsModel())
        self.__sqsStats = stats.get_model('sqs')

        self.__lambda = boto3.client('lambda')

        self.__numWorkers = 0
        self.__numWorkersLock = Lock()

        # RequestId -> Future
        self.__numTasksInProgress = 0
        self.__tasksInProgress = {}
        self.__tasksInProgressLock = Lock()
        self.__tasksInProgressCondition = Condition(self.__tasksInProgressLock)

        self.__init_message_queues()

        # Start result fetcher thread
        self.__result_handler_pool = ThreadPoolExecutor(DEFAULT_POLLING_THREADS)
        for i in range(DEFAULT_POLLING_THREADS):
            rt = Thread(target=self.__result_daemon)
            rt.daemon = True
            rt.start()


    def __init_message_queues(self):
        """Setup the message queues"""
        sqs = boto3.resource('sqs')

        currentTime = time.time()
        taskQueueAttributes = {
            'MessageRetentionPeriod': str(self.__config.message_retention_period),
            'ReceiveMessageWaitTimeSeconds': str(self.__config.worker_wait_time),
        }
        taskQueueName = '%s_task_%d' % (self.__config.queue_prefix, currentTime)
        self.__taskQueueName = taskQueueName
        taskQueue = sqs.create_queue(
            QueueName=taskQueueName,
            Attributes=taskQueueAttributes)
        self.__taskQueue = taskQueue
        atexit.register(lambda: taskQueue.delete())
        logger.info('Created task queue: %s', taskQueueName)

        resultQueueAttributes = {
            'MessageRetentionPeriod':
                str(self.__config.message_retention_period),
            'ReceiveMessageWaitTimeSeconds': str(20),
        }
        resultQueueName = '%s_result_%d' % (self.__config.queue_prefix,
                                            currentTime)
        self.__resultQueueName = resultQueueName
        resultQueue = sqs.create_queue(
            QueueName=resultQueueName,
            Attributes=resultQueueAttributes)
        atexit.register(lambda: resultQueue.delete())
        logger.info('Created result queue: %s', resultQueueName)

    def execute(self, task, timeout=None):
        """Enqueue a message in the task queue"""
        assert isinstance(task, LambdaSqsTask)
        with self.__numWorkersLock:
            if self.__should_spawn_worker():
                self.__spawn_new_worker()

        kwargs = {}
        if task.messageAttributes:
            kwargs['MessageAttributes'] = task.messageAttributes
        messageStatus = self.__taskQueue.send_message(
            MessageBody=task.body, **kwargs)

        # Use the MessageId as taskId
        taskId = messageStatus['MessageId']

        taskFuture = Future()
        with self.__tasksInProgressLock:
            self.__tasksInProgress[taskId] = taskFuture
            self.__numTasksInProgress = len(self.__tasksInProgress)
            self.__tasksInProgressCondition.notify()

        # Do this before sleeping
        self.__sqsStats.record_send(
            SqsStatsModel.estimate_message_size(
                messageAttributes=task.messageAttributes,
                messageBody=task.body))

        result = taskFuture.get(timeout=timeout)

        with self.__tasksInProgressLock:
            del self.__tasksInProgress[taskId]
            self.__numTasksInProgress = len(self.__tasksInProgress)

        return result

    def __should_spawn_worker(self):
        if self.__config.max_workers == 0:
            return False
        return (self.__numWorkers == 0 or
                (self.__numWorkers < self.__config.max_workers and
                 self.__numTasksInProgress >
                 self.__numWorkers * self.__config.load_factor))

    def __spawn_new_worker(self):
        workerId = random.getrandbits(32)
        logger.info('Starting new worker: %d', workerId)
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
            with self.__lambdaStats.record() as billingObject:
                response = self.__lambda.invoke(
                    FunctionName=functionName,
                    Payload=json.dumps(workerArgs),
                    LogType='Tail')
                billingObject.parse_log(response['LogResult'])
            if response['StatusCode'] != 200 or 'FunctionError' in response:
                logger.error('Worker %d exited unexpectedly: %s: status=%d',
                             workerId,
                             response['FunctionError'],
                             response['StatusCode'])
                logger.error(response['Payload'].read())
                self.__config.post_return_callback(workerId, None)
            else:
                workerResponse = json.loads(response['Payload'].read())
                self.__config.post_return_callback(workerId, workerResponse)

        finally:
            with self.__numWorkersLock:
                self.__numWorkers -= 1
                assert self.__numWorkers >= 0, 'Workers cannot be negative'

    def __handle_single_result_message(self, message):
        # TODO: Fix me. Assume maximally sized messages for now
        try:
            result = LambdaSqsResult.from_message(message)
            taskId = result.taskId
            with self.__tasksInProgressLock:
                taskFuture = self.__tasksInProgress.get(taskId)

            if taskFuture is None:
                logger.info('No future for task: %s', taskId)
                return

            # Handle fragmented
            if result.isFragmented:
                taskFuture._partial[result.fragmentId] = result
                logger.info('Setting result: %s', taskId)
                if len(taskFuture._partial) == result.numFragments:
                    taskFuture.set([
                        taskFuture._partial[i]
                        for i in range(result.numFragments)
                    ])
            else:
                logger.info('Setting result: %s', taskId)
                taskFuture.set(result)
        except Exception as e:
            logger.error('Failed to parse message: %s', message)
            logger.exception(e)
        finally:
            self.__sqsStats.record_receive(
                SqsStatsModel.estimate_message_size(message=message))

    def __result_daemon(self):
        """Poll SQS result queue and set futures"""
        requiredAttributes = ['All']
        sqs = boto3.resource('sqs')
        resultQueue = sqs.get_queue_by_name(QueueName=self.__resultQueueName)
        while True:
            # Don't poll SQS unless there is a task in progress
            with self.__tasksInProgressLock:
                if self.__numTasksInProgress == 0:
                    self.__tasksInProgressCondition.wait()

            # Poll for new messages
            logger.info('Polling for new results')
            messages = None
            try:
                self.__sqsStats.record_poll()
                messages = resultQueue.receive_messages(
                    MessageAttributeNames=requiredAttributes,
                    MaxNumberOfMessages=MAX_SQS_REQUEST_MESSAGES)
                logger.info('Received %d messages', len(messages))
                self.__result_handler_pool.map(
                    self.__handle_single_result_message, messages)
            except Exception as e:
                logger.error('Error polling SQS')
                logger.exception(e)
            finally:
                if messages is not None and len(messages) > 0:
                    try:
                        result = resultQueue.delete_messages(
                            Entries=[{
                                'Id': message.message_id,
                                'ReceiptHandle': message.receipt_handle
                            } for message in messages]
                        )
                        if len(result['Successful']) != len(messages):
                            raise Exception('Failed to delete all messages: %s'
                                            % result['Failed'])
                    except Exception as e:
                        logger.exception(e)

import os
import time
import json

from abc import abstractproperty
from collections import OrderedDict
from datetime import datetime
from termcolor import colored
from threading import Thread


class _AbstractModel(object):
    pass


class _AbstractCostModel(_AbstractModel):

    @abstractproperty
    def cost(self):
        return 0.0


class _AbstractTimeModel(_AbstractModel):

    @abstractproperty
    def time(self):
        return 0


class _AbstractDataModel(_AbstractModel):

    @abstractproperty
    def bytesDown(self):
        return 0

    @abstractproperty
    def bytesUp(self):
        return 0


MEGABYTE = 2 ** 20
DEFAULT_COLORS = ('green', 'yellow', 'cyan', 'red')
def _cls(): os.system('cls' if os.name == 'nt' else 'clear')


class Stats(object):

    def __init__(self):
        self.__models = OrderedDict()
        self.__startDate = datetime.now()

    def register_model(self, name, model):
        assert isinstance(model, _AbstractModel)
        self.__models[name] = model

    def get_model(self, name):
        return self.__models[name]

    def _dump_live_summary(self, colors=DEFAULT_COLORS):
        _cls()
        td = datetime.now() - self.__startDate
        print colored('Displaying stats for %dd %dh %dm %ds' %
                      (td.days, td.seconds / 3600, (td.seconds % 3600) / 60,
                       td.seconds % 60),
                      'white', 'on_green')
        numColors = len(colors)
        totalCost = 0.0
        for i, name in enumerate(self.__models):
            model = self.__models[name]
            color = colors[i % numColors]
            values = []
            if isinstance(model, ProxyStatsModel):
                values.append('count: {:8d}'.format(model.totalRequests))
                values.append('delay: {:6d}ms'.format(int(model.meanDelay)))
            if isinstance(model, _AbstractCostModel):
                modelCost = model.cost
                totalCost += modelCost
                values.append('cost: ${:8f}'.format(modelCost))
            if isinstance(model, _AbstractTimeModel):
                values.append('time: {:8d}s'.format(int(model.time) / 1000))
            if isinstance(model, _AbstractDataModel):
                MBDown = float(model.bytesDown) / MEGABYTE
                MBUp = float(model.bytesUp) / MEGABYTE
                values.append('up: {:9.3f}MB'.format(MBUp))
                values.append('down: {:7.3f}MB'.format(MBDown))

            print colored('[%#8s]' % name, color),\
                '  '.join(values)

        name = 'total'
        color = DEFAULT_COLORS[(i + 1) % numColors]
        print colored('[%#8s]' % name, color), 'cost: ${:8f}'.format(totalCost)

    def start_live_summary(self, frequency=5):
        # Only require threading if using this
        this = self
        def live_summary():
            while True:
                time.sleep(frequency)
                this._dump_live_summary()
        t = Thread(target=live_summary)
        t.daemon = True
        t.start()

    @property
    def models(self):
        return self.__models.keys()


class LambdaStatsModel(_AbstractCostModel, _AbstractTimeModel):

    class Constants:
        PER_REQUEST_COST = 0.2 / (10 ** 6)
        PER_100MS_COST = 0.000000208

    def __init__(self):
        self._totalMillis = 0
        self._totalRequests = 0

    @property
    def cost(self):
        return (LambdaStatsModel.Constants.PER_REQUEST_COST * self._totalRequests
                + self._totalMillis / 100 * LambdaStatsModel.Constants.PER_100MS_COST)

    @property
    def time(self):
        return self._totalMillis

    class Request(object):

        def __init__(self, model):
            self.__model = model

        def __enter__(self):
            self.__startTime = time.time()

        def __exit__(self, exc_type, exc_val, exc_tb):
            runTime = time.time() - self.__startTime
            self.__model._totalRequests += 1
            estMillisBilled = int(runTime * 1000)
            if estMillisBilled % 100 != 0:
                estMillisBilled += (100 - estMillisBilled % 100)
            self.__model._totalMillis += estMillisBilled

    def record(self):
        return LambdaStatsModel.Request(self)


class SqsStatsModel(_AbstractCostModel, _AbstractDataModel):

    class Constants:
        PER_REQUEST_COST = 0.4 / (10 ** 6)
        MAX_REQUEST_SIZE = 256 * 1024
        BILLING_UNIT_SIZE = 64 * 1024

    def __init__(self):
        self.__totalMessagesReceived = 0
        self.__totalMessagesSent = 0
        self.__totalPolls = 0

        # For computing cost
        self.__totalRequests = 0

        self.__totalBytesUp = 0
        self.__totalBytesDown = 0
        self.__totalMessagesReceived = 0

    @property
    def cost(self):
        return self.__totalRequests * SqsStatsModel.Constants.PER_REQUEST_COST

    @property
    def bytesUp(self):
        return self.__totalBytesUp

    @property
    def bytesDown(self):
        return self.__totalBytesDown

    @staticmethod
    def estimate_message_size(message=None, messageAttributes=None,
                              messageBody=None):
        size = 0
        if message is not None:
            if message.message_attributes is not None:
                size += len(json.dumps(message.message_attributes))
            size += len(message.body)
        else:
            if messageAttributes is not None:
                size += len(json.dumps(messageAttributes))
            size += len(messageBody)
        return size

    def record_poll(self):
        self.__totalPolls += 1
        self.__totalRequests += 1

    def record_send(self, size=Constants.MAX_REQUEST_SIZE):
        self.__totalMessagesSent += 1
        self.__totalBytesUp += size
        requests = size / SqsStatsModel.Constants.BILLING_UNIT_SIZE
        if size % SqsStatsModel.Constants.BILLING_UNIT_SIZE != 0:
            requests += 1
        # Assume someone on the other side is receiving the request
        # by polling and deleting it when done
        self.__totalRequests += 3 * requests

    def record_receive(self, size=Constants.MAX_REQUEST_SIZE):
        self.__totalMessagesReceived += 1
        self.__totalBytesDown += size
        requests = size / SqsStatsModel.Constants.BILLING_UNIT_SIZE
        if size % SqsStatsModel.Constants.BILLING_UNIT_SIZE != 0:
            requests += 1

        # Assume someone on the other side sent the request and
        # that we are deleting the message when done
        self.__totalRequests += 2 * requests


class S3StatsModel(_AbstractCostModel, _AbstractDataModel):

    class Constants:
        PER_PUT_COST = 0.0055 / 1000
        PER_GET_COST = 0.0044 / 10000

        # DATA_STORAGE_COST = 0.0264 / (2 ** 30)
        DATA_STORAGE_COST = 0.0
        DATA_RETRIEVAL_COST = 0.09 / (2 ** 30)

    def __init__(self, bothSides=True):
        self.__bothSides = bothSides
        self.__totalPuts = 0
        self.__totalGets = 0
        self.__totalBytesUp = 0
        self.__totalBytesDown = 0

    @property
    def cost(self):
        return (self.__totalPuts * S3StatsModel.Constants.PER_PUT_COST +
                self.__totalGets * S3StatsModel.Constants.PER_GET_COST +
                self.__totalBytesDown * S3StatsModel.Constants.DATA_RETRIEVAL_COST +
                self.__totalBytesUp * S3StatsModel.Constants.DATA_STORAGE_COST)
    
    @property
    def bytesUp(self):
        return self.__totalBytesUp

    @property
    def bytesDown(self):
        return self.__totalBytesDown

    def record_put(self, size):
        self.__totalPuts += 1

        if self.__bothSides:
            # Someone on the other side is gets the object
            self.__totalGets += 1
            self.__totalBytesDown += size

        self.__totalBytesUp += size

    def record_get(self, size):
        self.__totalGets += 1

        if self.__bothSides:
            # someone on the other side put the object
            self.__totalPuts += 1
            self.__totalBytesUp += size

        self.__totalBytesDown += size


class ProxyStatsModel(_AbstractDataModel):

    def __init__(self):
        self.__startTime = time.time()
        self._totalRequestsProxied = 0
        self._totalRequestDelays = 0.0
        self.__totalBytesDown = 0
        self.__totalBytesUp = 0

    class Delay(object):

        def __init__(self, model):
            self.__model = model

        def __enter__(self):
            self.__startTime = time.time()

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is None:
                delay = time.time() - self.__startTime
                self.__model._totalRequestDelays += delay * 1000
                self.__model._totalRequestsProxied += 1

    @property
    def totalRequests(self):
        return self._totalRequestsProxied

    @property
    def meanDelay(self):
        if self._totalRequestsProxied == 0:
            return 0.0
        return float(self._totalRequestDelays) / self._totalRequestsProxied

    @property
    def bytesUp(self):
        return self.__totalBytesUp

    @property
    def bytesDown(self):
        return self.__totalBytesDown

    def record_delay(self):
        return ProxyStatsModel.Delay(self)

    def record_bytes_up(self, n):
        self.__totalBytesUp += n

    def record_bytes_down(self, n):
        self.__totalBytesDown += n

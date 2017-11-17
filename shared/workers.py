"""
Note: this file will be copied to the Lambda too. Do not
add dependencies carelessly.
"""

class SqsMessage(object):

    def __init__(self, message=None):
        self._messageAttributes = {}
        self._body = None
        if message is not None:
            if message.message_attributes:
                self._messageAttributes = message.message_attributes
            self._body = message.body

    def add_string_attribute(self, name, value):
        assert len(value) > 0
        self._messageAttributes[name] = {
            'StringValue': value,
            'DataType': 'String'
        }

    def add_binary_attribute(self, name, value):
        assert len(value) > 0
        self._messageAttributes[name] = {
            'BinaryValue': value,
            'DataType': 'Binary'
        }

    def add_number_attribute(self, name, value):
        self._messageAttributes[name] = {
            'StringValue': str(value),
            'DataType': 'Number'
        }

    def has_attribute(self, name):
        return name in self._messageAttributes

    def get_string_attribute(self, name):
        return self._messageAttributes[name]['StringValue']

    def get_binary_attribute(self, name):
        return self._messageAttributes[name]['BinaryValue']

    def get_number_attribute(self, name, cast=int):
        return cast(self._messageAttributes[name]['StringValue'])

    def set_body(self, body):
        assert len(body) > 0
        self._body = body

    @property
    def body(self):
        return self._body

    @property
    def messageAttributes(self):
        return self._messageAttributes


class LambdaSqsTask(SqsMessage):

    def __init__(self, message=None):
        super(LambdaSqsTask, self).__init__(message)
        self.taskId = None

    @staticmethod
    def from_message(message):
        task = LambdaSqsTask(message)
        task.taskId = message.message_id
        return task


class LambdaSqsResult(SqsMessage):

    TASK_ID = 'TASK_ID'
    FRAGMENT_ID = 'FRAG_ID'
    FRAGMENT_CNT = 'FRAG_CT'

    def __init__(self, taskId=None, fragmentId=None,
                 numFragments=None, message=None):
        super(LambdaSqsResult, self).__init__(message)
        self.taskId = taskId
        self.fragmentId = fragmentId
        self.numFragments = numFragments

    @property
    def isFragmented(self):
        return self.fragmentId is not None

    @property
    def messageAttributes(self):
        ret = {
            LambdaSqsResult.TASK_ID : {
                'StringValue': self.taskId,
                'DataType': 'String'
            }
        }
        if self.fragmentId is not None:
            ret[LambdaSqsResult.FRAGMENT_ID] = {
                'StringValue': str(self.fragmentId),
                'DataType': 'Number'
            }
        if self.numFragments is not None:
            ret[LambdaSqsResult.FRAGMENT_CNT] = {
                'StringValue': str(self.numFragments),
                'DataType': 'Number'
            }
        ret.update(self._messageAttributes)
        return ret

    @staticmethod
    def from_message(message):
        taskId = message.message_attributes[LambdaSqsResult.TASK_ID]['StringValue']
        if LambdaSqsResult.FRAGMENT_ID in message.message_attributes:
            fragmentId = int(message.message_attributes[LambdaSqsResult.FRAGMENT_ID]['StringValue'])
            numFragments = int(message.message_attributes[LambdaSqsResult.FRAGMENT_CNT]['StringValue'])
            result = LambdaSqsResult(taskId, fragmentId=fragmentId,
                                     numFragments=numFragments,
                                     message=message)
        else:
            result = LambdaSqsResult(taskId, message=message)
        return result

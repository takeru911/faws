class SQSError(Exception):
    @property
    def message(self):
        raise NotImplementedError


class NonExistentQueue(SQSError):
    message = "The specified queue does not exist for this wsdl version."

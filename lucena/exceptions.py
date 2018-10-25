# -*- coding: utf-8 -*-


class LucenaException(Exception):
    """Lucena has raised an error."""

    def __str__(self):
        return self.__doc__


class UnexpectedParameterValue(LucenaException):
    def __init__(self, param_name):
        super(UnexpectedParameterValue, self).__init__()
        self.param_name = param_name

    def __str__(self):
        return "Unexpected value in parameter '{}'.".format(self.param_name)


class WorkerAlreadyStarted(LucenaException):
    """This Worker has already been started."""
    pass


class WorkerNotStarted(LucenaException):
    """This Worker has not been started."""
    pass


class ServiceAlreadyStarted(LucenaException):
    """This Service has already been started."""
    pass

# -*- coding: utf-8 -*-


class LucenaException(Exception):
    """Lucena has raised an error."""

    def __str__(self):
        if self.args or not self.__doc__:
            return super(LucenaException, self).__str__()
        return self.__doc__


class LookupHandlerError(LucenaException):
    """Unable to resolve this message."""
    pass


class WorkerAlreadyStarted(LucenaException):
    """This Worker has already been started."""
    pass


class WorkerNotStarted(LucenaException):
    """This Worker has not been started."""
    pass


class ServiceAlreadyStarted(LucenaException):
    """This Service has already been started."""
    pass


class ServiceNotStarted(LucenaException):
    """This Service has not been started."""
    pass

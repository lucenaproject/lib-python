# -*- coding: utf-8 -*-


class LucenaException(Exception):
    """Lucena has raised an error."""

    def __str__(self):
        return self.__doc__


class WorkerAlreadyStarted(LucenaException):
    """This Worker has already been started."""
    pass


class ServiceAlreadyStarted(LucenaException):
    """This Service has already been started."""
    pass

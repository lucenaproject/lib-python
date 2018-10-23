# -*- coding: utf-8 -*-


class LucenaException(Exception):
    """Lucena has raised an error."""

    def __str__(self):
        return self.__doc__


class AlreadyStarted(LucenaException):
    """This Worker or Service has already been started."""
    pass

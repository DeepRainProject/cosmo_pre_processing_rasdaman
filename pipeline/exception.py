class Error(Exception):
    """Base class for exceptions."""
    pass


class SlaveError(Error):
    """Exception raised for errors from the slave.

    Only the execution of the slave needs to be stopped, the other slaves can continue their job.

    Attributes:
        function -- the function in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, function, message):
        self.function = function
        self.message = message


class MainError(Error):
    """Exception raised from master or is a main error.

    The execution of the whole program must be stopped, since a main error occurred. All slaves need to be aborted.

    Attributes:
        function -- the function in which the error occurred
        critical -- explanation of the error
        info     -- used as info for the logger
    """

    def __init__(self, function, critical, info):
        self.function = function
        self.critical = critical
        self.info = info

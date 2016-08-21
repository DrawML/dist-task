class HeaderError(Exception):
    def __init__(self, msg):
        self._msg = msg

    def __str__(self):
        return "HeaderError : %s" % self._msg


class HandleError(Exception):
    def __init__(self, msg):
        self._msg = msg

    def __str__(self):
        return "HandleError : %s" % self._msg

class HeaderError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        print("HeaderError : %s" % msg)

class UnknownExtraError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        print("UnknownExtraError : %s" % msg)

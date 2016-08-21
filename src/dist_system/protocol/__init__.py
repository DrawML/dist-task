class ResultReceiverAddress(object):
    def __init__(self, type_ : str, ip : str, port : int):
        assert type_ == 'tcp'
        self._type = type_
        self._ip = ip
        self._port = port

    def to_dict(self):
        return {
            'type' : self._type,
            'ip' : self._ip,
            'port' : self._port
        }

    @staticmethod
    def from_dict(self, dict_ : dict):
        ResultReceiverAddress(dict_['type'], dict_['ip'], dict_['port'])

    def to_zeromq_addr(self):
        # need to refine.
        return "{0}//{1}:{2}".format(self._type, self._ip, self._port)
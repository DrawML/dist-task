from dist_system.library import SingletonMeta


class ClientSessionValueError(ValueError):
    def __init__(self, msg = ''):
        self._msg = msg

    def __str__(self):
        return "ClientSessionValueError : %s" % self._msg


class ClientSessionIdentity(object):
    def __init__(self, addr):
        self._addr = addr

    # I fire you if you override this.
    def __eq__(self, other):
        return self._addr == other._addr

    @property
    def addr(self):
        return self._addr

    def __repr__(self):
        return str(self._addr)


class ClientSession(ClientSessionIdentity):
    def __init__(self, addr, task):
        super().__init__(addr)
        self._task = task

    @property
    def task(self):
        return self._task

    @staticmethod
    def make_session_from_identity(client_session_identity, task):
        return ClientSession(client_session_identity.addr, task)


class ClientSessionManager(metaclass=SingletonMeta):
    def __init__(self):
        self._sessions = []

    @property
    def count(self):
        return len(self._sessions)

    def add_session(self, client_sesssion):
        if self.check_session_existence(client_sesssion):
            raise ClientSessionValueError("Duplicated Client Session")
        else:
            self._sessions.append(client_sesssion)

    def del_session(self, client_session_identity):
        self._sessions.remove(client_session_identity)

    def _from_generic_to_session(self, identity_or_session):
        if type(identity_or_session) == ClientSessionIdentity:
            session = self.find_session(identity_or_session)
        else:
            session = identity_or_session
        return session

    def check_session_existence(self, client_session_identity, find_flag=False):
        targets = [session for session in self._sessions if session == client_session_identity]
        ret = len(targets) > 0
        if find_flag:
            return (ret, targets)
        else:
            return ret

    def find_session(self, client_session_identity):
        exists, targets = self.check_session_existence(client_session_identity, find_flag=True)
        if exists:
            if len(targets) > 1:
                raise ClientSessionValueError("Same Client Sessions exist.")
            return targets[0]
        else:
            raise ClientSessionValueError("Non-existent Client Session.")

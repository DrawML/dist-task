from ..library import SingletonMeta


class SlaveValueError(ValueError):
    def __init__(self, msg):
        self._msg = msg

    def __str__(self):
        return "SlaveValueError : %s" % self._msg


class NotAvailableSlaveError(Exception):
    def __init__(self, msg):
        self._msg = msg

    def __str__(self):
        return "NotAvailableSlaveError : %s" % self._msg


class SlaveIdentity(object):

    def __init__(self, addr):
        self._addr = addr

    # I fire you if you override this.
    def __eq__(self, other):
        return self._addr == other._addr

    @property
    def addr(self):
        return self._addr


class Slave(SlaveIdentity):

    def __init__(self, addr):
        super().__init__(addr)
        self._tasks = []
        self._slave_information = None
        self.heartbeat()

    @property
    def tag(self):
        return self._tag

    @property
    def tasks_count(self):
        return len(self._tasks)

    def assign_task(self, task):
        self._tasks.append(task)

    def delete_task(self, task):
        self._tasks.remove(task)

    @property
    def tasks(self):
        return tuple(self._tasks)

    @staticmethod
    def make_slave_from_identity(slave_identity):
        return Slave(slave_identity.addr)

    def heartbeat(self):
        self._liveness = SlaveManager.HEARTBEAT_LIVENESS

    def live(self):
        self._liveness -= 1
        return self._liveness > 0

    @property
    def slave_information(self):
        return self._slave_information

    @slave_information.setter
    def slave_information(self, slave_information):
        self._slave_information = slave_information


class SlaveManager(metaclass=SingletonMeta):

    HEARTBEAT_LIVENESS = 3
    HEARTBEAT_INTERVAL = 1

    def __init__(self):
        self._slaves = []

    @property
    def count(self):
        return len(self._slaves)

    @property
    def slaves(self):
        return tuple(self._slaves)

    def add_slave(self, slave):
        if self.check_slave_existence(slave):
            raise SlaveValueError("Duplicated Slave.")
        else:
            self._slaves.append(slave)

    def del_slave(self, slave_identity):
        self._slaves.remove(slave_identity)

    def _from_generic_to_slave(self, identity_or_slave):
        if type(identity_or_slave) == SlaveIdentity:
            slave = self.find_slave(identity_or_slave)
        else:
            slave = identity_or_slave
        return slave

    def check_slave_existence(self, slave_identity, find_flag = False):
        targets = [slave for slave in self._slaves if slave == slave_identity]
        ret = len(targets) > 0
        if find_flag:
            return (ret, targets)
        else:
            return ret

    def find_slave(self, slave_identity):
        exists, targets = self.check_slave_existence(slave_identity, find_flag=True)
        if exists:
            if len(targets) > 1:
                raise SlaveValueError("Same Slaves exist.")
            return targets[0]
        else:
            raise SlaveValueError("Non-existent Slave.")

    def find_slave_having_task(self, task):
        for slave in self._slaves:
            if task in slave.tasks:
                return slave
        raise SlaveValueError("Non-existent Slave.")
    """
    # Get proper slave for task.
    def get_proper_slave(self, task):

        # some algorithms will be filled in here.
        proper_slave = None
        for slave in self._slaves:
            if slave.tasks_count >= 3:
                continue
            if proper_slave is None or proper_slave.tasks_count < slave.tasks_count:
                proper_slave = slave

        if proper_slave is None:
            raise NotAvailableSlaveError("Not available Slaves.")
        else:
            return proper_slave
    """
    def purge(self):
        expired_slaves = []
        leak_tasks = []
        for slave in self._slaves:
            alive = slave.live()
            if not alive:
                expired_slaves.append(slave)
                self.del_slave(slave)
                tasks = slave.tasks
                leak_tasks += tasks
        return expired_slaves, leak_tasks

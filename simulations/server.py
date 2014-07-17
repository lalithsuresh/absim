import SimPy.Simulation as Simulation


class Server():
    """A representation of a physical server that holds resources"""
    def __init__(self, id_, resourceCapacity, serviceTime):
        self.id = id_
        self.serviceTime = serviceTime
        self.queueResource = Simulation.Resource(capacity=resourceCapacity,
                                                 monitored=True)

    def enqueue_task(self, task):
        executor = Executor(self, task)
        Simulation.activate(executor, executor.run(), Simulation.now())


class Executor(Simulation.Process):

    def __init__(self, server, task):
        self.server = server
        self.task = task
        Simulation.Process.__init__(self, name='Executor')

    def run(self):
        yield Simulation.hold, self
        yield Simulation.request, self, self.server.queueResource
        yield Simulation.hold, self, self.server.serviceTime
        yield Simulation.release, self, self.server.queueResource

        self.task.sigTaskComplete()

import SimPy.Simulation as Simulation


class Task():
    """A simple Task. Applications may subclass this
       for holding specific attributes if need be"""
    def __init__(self, fullId, parentId, batchsize,
                 latencyMonitor, sendingClient):
        self.id = fullId    # Parent task + subtask Id
        self.parentId = parentId       # Parent task Id
        self.batchsize = batchsize
        self.sendingClient = sendingClient
        self.start = Simulation.now()
        self.completionEvent = Simulation.SimEvent("ClientToServerCompletion")
        self.latencyMonitor = latencyMonitor

    # Used as a notifier mechanism
    def sigTaskComplete(self, piggyBack=None):
        if (self.completionEvent is not None):
            self.completionEvent.signal(piggyBack)

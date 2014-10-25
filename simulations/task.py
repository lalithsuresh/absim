import SimPy.Simulation as Simulation


class Task():
    """A simple Task. Applications may subclass this
       for holding specific attributes if need be"""
    def __init__(self, id_, latencyMonitor, sendingClient):
        self.id = id_
        self.sendingClient = sendingClient
        self.start = Simulation.now()
        self.completionEvent = Simulation.SimEvent("ClientToServerCompletion")
        self.latencyMonitor = latencyMonitor

    # Used as a notifier mechanism
    def sigTaskComplete(self, piggyBack=None):
        if (self.completionEvent is not None):
            self.completionEvent.signal(piggyBack)

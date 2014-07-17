import SimPy.Simulation as Simulation


class Task():
    """A simple Task. Applications may subclass this
       for holding specific attributes if need be"""
    def __init__(self, id_, completionEvent=None):
        self.id = id_
        self.completionEvent = Simulation.SimEvent("ClientToServerCompletion")
        self.eventExtra = Simulation.SimEvent("WorkloadToClientCompletion")

    # Used as a notifier mechanism
    def sigTaskComplete(self):
        if (self.completionEvent is not None):
            self.completionEvent.signal(self)

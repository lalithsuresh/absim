import SimPy.Simulation as Simulation


class Task():
    """A simple Task. Applications may subclass this
       for holding specific attributes if need be"""
    def __init__(self, id_, latencyMonitor, start=False, completionEvent=False, receivedEvent=False):
        self.id = id_
        if not start:
            self.start = Simulation.now()
        else:
            self.start = start
        if not completionEvent:
            self.completionEvent = Simulation.SimEvent("ClientToServerCompletion")
        else:
            self.completionEvent = completionEvent
        if not receivedEvent:
            self.receivedEvent = Simulation.SimEvent("PacketReceipt")
        else:
            self.receivedEvent = receivedEvent   
        self.latencyMonitor = latencyMonitor

    # Used as a notifier mechanism
    def sigTaskComplete(self, piggyBack=None):
        if (self.completionEvent is not None):
            self.completionEvent.signal(piggyBack)

    def sigTaskReceived(self, piggyBack=None):
        if (self.receivedEvent is not None):
            self.receivedEvent.signal(piggyBack)
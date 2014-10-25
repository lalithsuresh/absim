import SimPy.Simulation as Simulation


class Task():
    """A simple Task. Applications may subclass this
       for holding specific attributes if need be"""
    def __init__(self, id_, latencyMonitor):
        self.id = id_
        self.start = Simulation.now()
        self.completionEvent = Simulation.SimEvent("ClientToServerCompletion")
        self.latencyMonitor = latencyMonitor
        self.replicaSet = []
        self.queueSizeEst = 0
        
    # Used as a notifier mechanism
    def sigTaskComplete(self, piggyBack=None):
        if (self.completionEvent is not None):
            self.completionEvent.signal(piggyBack)
            
    def addReplicaSet(self, replicaSet):
        self.replicaSet = replicaSet
        
    def addQueueSizeEst(self, queueSizeEst):
        self.queueSizeEst = queueSizeEst

import task
import SimPy.Simulation as Simulation

class DataTask(task.Task):
    """This is a data task which accounts for a task's size in bytes"""
    def __init__(self, id_, latencyMonitor, src=None, dst=None, size=1,\
                  response=False, seqN=0, count=1, start=False,\
                   completionEvent=False, receivedEvent=False, replicas=[]):
        task.Task.__init__(self, id_, latencyMonitor, start, completionEvent, receivedEvent)
        self.response = response
        self.size = size
        self.src = src
        self.dst = dst
        self.seqN = seqN
        self.count = count
        self.replicas = replicas
        
    def setDestination(self, dst):
        self.dst = dst
        
    def incSeq(self):
        self.seqN = self.seqN + 1
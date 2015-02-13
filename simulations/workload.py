import SimPy.Simulation as Simulation
import random
import task
import numpy


class Workload(Simulation.Process):

    def __init__(self, id_, latencyMonitor, clientList,
                 model, model_param, numRequests, batchSizeModel,
                 batchSizeParam):
        self.id_ = id_
        self.latencyMonitor = latencyMonitor
        self.clientList = clientList
        self.model = model
        self.model_param = model_param
        self.numRequests = numRequests
        self.batchSizeModel = batchSizeModel
        self.batchSizeParam = batchSizeParam
        self.total = sum(client.demandWeight for client in self.clientList)
        Simulation.Process.__init__(self, name='Workload' + str(id_))

    # TODO: also need non-uniform client access
    # Need to pin workload to a client
    def run(self):

        taskCounter = 0
        while(self.numRequests != 0):
            yield Simulation.hold, self,

            batchsize = 1
            if (self.batchSizeModel == "constant"):
                batchsize = self.batchSizeParam
            elif (self.batchSizeModel == "random.expovariate"):
                # TODO, not sure what an actual model looks like
                rand = random.expovariate(1/float(self.batchSizeParam))
                batchsize = min(1, int(rand))

            # Push out a task...
            clientNode = self.weightedChoice()
            subTaskCounter = 0

            i = batchsize
            while (i != 0):
                yield Simulation.hold, self,
                taskToSchedule = task.Task("%s-Task%s-%s"
                                           % (self.id_,
                                              str(taskCounter),
                                              str(subTaskCounter)),
                                           "%s-Task%s"
                                           % (self.id_,
                                              str(taskCounter)),
                                           batchsize,
                                           self.latencyMonitor,
                                           clientNode)
                clientNode.schedule(taskToSchedule)
                i -= 1
                subTaskCounter += 1

            self.numRequests -= 1
            taskCounter += 1

            # Simulate client delay
            if (self.model == "poisson"):
                yield Simulation.hold, self,\
                    numpy.random.poisson(self.model_param)

            # If model is gaussian, add gaussian delay
            # If model is constant, add fixed delay
            if (self.model == "constant"):
                yield Simulation.hold, self, self.model_param

    def weightedChoice(self):
        r = random.uniform(0, self.total)
        upto = 0
        for client in self.clientList:
            if upto + client.demandWeight > r:
                return client
            upto += client.demandWeight
        assert False, "Shouldn't get here"

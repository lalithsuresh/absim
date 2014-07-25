import SimPy.Simulation as Simulation
import random
import task
import numpy
import constants

numRequests = 100

class Workload(Simulation.Process):

    def __init__(self, id_, latencyMonitor):
        self.latencyMonitor = latencyMonitor
        Simulation.Process.__init__(self, name='Workload' + str(id_))

    # TODO: also need non-uniform client access
    # Need to pin workload to a client
    def run(self, clientList, model, model_param, numRequests):

        taskCounter = 0

        while(numRequests != 0):
            yield Simulation.hold, self,

            
            # delay = constants.NW_LATENCY_BASE + \
            #     random.normalvariate(constants.NW_LATENCY_MU,
            #                          constants.NW_LATENCY_SIGMA)
            # yield Simulation.hold, self, delay
            taskToSchedule = task.Task("Task" + str(taskCounter), self.latencyMonitor)
            taskCounter += 1

            # Push out a task...
            randomClientIndex = random.randint(0, len(clientList) - 1)
            clientNode = clientList[randomClientIndex]

            clientNode.schedule(taskToSchedule)

            # Simulate client delay
            if (model == "poisson"):
                yield Simulation.hold, self, numpy.random.poisson(model_param)

            # If model is gaussian, add gaussian delay
            # If model is constant, add fixed delay
            if (model == "constant"):
                yield Simulation.hold, self, model_param

            numRequests -= 1

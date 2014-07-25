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
    def run(self, clientList, model, poisson_param, numRequests):

        taskCounter = 0

        while(numRequests != 0):
            yield Simulation.hold, self,
            # Simulate client delay
            if(model == "poisson"):
                yield Simulation.hold, self, numpy.random.poisson(poisson_param)
            # If model is gaussian, add gaussian delay
            # If model is constant, add fixed delay

            start = Simulation.now()
            # delay = constants.NW_LATENCY_BASE + \
            #     random.normalvariate(constants.NW_LATENCY_MU,
            #                          constants.NW_LATENCY_SIGMA)
            # yield Simulation.hold, self, delay
            taskToSchedule = task.Task("Task" + str(taskCounter), None)
            taskCounter += 1

            # Push out a task...
            randomClientIndex = random.randint(0, len(clientList) - 1)
            clientNode = clientList[randomClientIndex]

            clientNode.schedule(taskToSchedule)

            # Wait until the task we sent out is complete.
            yield Simulation.waitevent, self, taskToSchedule.eventExtra
            self.latencyMonitor.observe(Simulation.now() - start)
            numRequests -= 1

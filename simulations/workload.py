import SimPy.Simulation as Simulation
import random
import sys
import datatask
import numpy
import constants
import math
from scipy.stats import genpareto

class Workload(Simulation.Process):

    def __init__(self, id_, latencyMonitor, clientList,
                 model, model_param, numRequests, valueSizeModel, initialNum, readFraction):
        self.latencyMonitor = latencyMonitor
        self.clientList = clientList
        self.model = model
        self.model_param = model_param
        self.numRequests = numRequests
        self.total = sum(client.demandWeight for client in self.clientList)
        self.backlogMonitor = Simulation.Monitor(name="BackLog")
        self.taskCounter = 0
        self.initialNum = initialNum
        self.valueSizeModel = valueSizeModel

        self.readFraction = readFraction
        Simulation.Process.__init__(self, name='Workload' + str(id_))

    # TODO: also need non-uniform client access
    # Need to pin workload to a client
    def run(self):

        while(self.numRequests != 0):
            yield Simulation.hold, self,

            taskToSchedule = datatask.DataTask("Task" + str(self.taskCounter + self.initialNum),
                                       self.latencyMonitor)
            self.taskCounter += 1

            # Push out a task...
            clientNode = self.weightedChoice()
            
            taskToSchedule.requestType = self.getRequestType()

            if  taskToSchedule.requestType == constants.WRITE:
                #generate request size
                requestSize = self.fbGenValueSize()
                taskToSchedule.requestPktCount = int(math.ceil(requestSize/float(constants.PACKET_SIZE*1e6)))
                #print "WRITE request with requestcount= ",taskToSchedule.requestPktCount 
                #generate response size
                taskToSchedule.count = 1
            elif taskToSchedule.requestType == constants.READ:
                #generate request size
                taskToSchedule.requestPktCount = 1
                #generate response size
                responseSize = self.fbGenValueSize()
                taskToSchedule.count = int(math.ceil(responseSize/float(constants.PACKET_SIZE*1e6)))
                #print "READ request with responsecount= ",taskToSchedule.count 
            else:
                print "Unknown request type!"
                sys.exit(-1)

            taskToSchedule.src = clientNode
            clientNode.schedule(taskToSchedule)

            # Simulate client delay
            
            #If model is poisson, add poisson delay
            if (self.model == "poisson"):
                delay = numpy.random.poisson(self.model_param*1000)/1000.0
                yield Simulation.hold, self, delay

            # If model is constant, add fixed delay
            if (self.model == "constant"):
                yield Simulation.hold, self, self.model_param

            self.numRequests -= 1

    def weightedChoice(self):
        r = random.uniform(0, self.total)
        upto = 0
        for client in self.clientList:
            if upto + client.demandWeight > r:
                return client
            upto += client.demandWeight
        assert False, "Shouldn't get here"

    def getRequestType(self):
        r = random.uniform(0, 1)
        if r <= self.readFraction:
            return constants.READ
        else:
            return constants.WRITE

    # Pareto distributed value sizes derived from Facebook key-value store workload
    def fbGenValueSize(self):
        #Probabilities for the first 14 values
        distr_1_14 = {1:0.00583, 2:0.17820, 3:0.09239, 4:0.00018,
                      5:0.02740, 6:0.00065, 7:0.00606, 8:0.00023,
                      9:0.00837, 10:0.00837, 11:0.08989, 12:0.00092,
                      13:0.00326, 14:0.01980}
        p_1_14 = sum(distr_1_14.values())
        choice = random.random()
        if (choice > p_1_14):
            r = 0
            numargs = genpareto.numargs
            [ c ] = [0.348238,]*numargs
            while int(r) <= 14:
                r = genpareto.rvs(c, loc=0, scale=214.476)
            return int(r)

        f_x = 0
        for i in distr_1_14.keys():
            f_x += distr_1_14[i]/p_1_14
            if choice <= f_x:
                r = i
                break
        size = int(r)
        assert size > 0
        return size

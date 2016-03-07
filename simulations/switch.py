import SimPy.Simulation as Simulation
import constants
from node import Node
import misc
from collections import defaultdict
import random
import datatask
import congestionTable as ct
import pathLookupTable as plu
import sys
import copy
import logger

class Switch(Node):
    """A representation of a physical switch that disperses requests and responses
    Note: We're assuming that switches aren't bottle-necked by processing times"""
    def __init__(self, id_, htype, procTime,
                 clientList, serverList, selectionStrategy,
                 forwardingStrategy, c4Weight, rateInterval,
                 cubicC, cubicSmax, cubicBeta, hysterisisFactor,
                 rateLimiterEnabled):
        Node.__init__(self, id_, htype)
        self.log = self.init_logger(id_)
        self.procTime = procTime
        self.connectedHosts = {} #Equivalent to switch routing table
        self.selectionStrategy = selectionStrategy  
        self.forwardingStrategy = forwardingStrategy
        self.c4Weight = c4Weight
        self.congestionTable = ct.CongestionTable()
        self.queueSizeMap = {node: 0 for node in serverList}
        self.serviceTimeMap = {node: 0 for node in serverList}
        self.cqueueSizeMap = {c: {node: 0 for node in serverList} for c in clientList}
        self.cserviceTimeMap = {c: {node: 0 for node in serverList} for c in clientList}

        #Only leaf switches are maintaining a per-path latency lookup table
        self.latency_lookup = plu.PathLookupTable(self) if htype == "leaf" else None
        
        # Rate limiters per replica
        self.rateLimiterEnabled = rateLimiterEnabled
        self.rateLimiters = {node: misc.RateLimiter("RL-%s" % node.id,
                                               self, 50, rateInterval)
                             for node in serverList}
        self.lastRateDecrease = {node: 0 for node in serverList}
        self.valueOfLastDecrease = {node: 10 for node in serverList}
        self.receiveRate = {node: misc.ReceiveRate("RL-%s" % node.id, rateInterval)
                            for node in serverList}
        self.lastRateIncrease = {node: 0 for node in serverList}
        self.rateInterval = rateInterval

        # Parameters for congestion control
        self.cubicC = cubicC
        self.cubicSmax = cubicSmax
        self.cubicBeta = cubicBeta
        self.hysterisisFactor = hysterisisFactor

        # Request status
        self.requestStatus = {} #request/response mappings to keep track of a request's status (in terms of received packets)

        # Server feedback aggregates
        self.queueSizeMap = {node: 0 for node in serverList}
        self.serviceTimeMap = {node: 0 for node in serverList}
        self.cqueueSizeMap = {c: {node: 0 for node in serverList} for c in clientList}
        self.cserviceTimeMap = {c: {node: 0 for node in serverList} for c in clientList}
        
        #Strategy function dictionary; Holds mappings between strats and corresponding funcs
        
    def init_logger(self, id_):
        return logger.getLogger("Switch:%s" % id_, constants.LOG_LEVEL)
            
    def addConnectedHosts(self, n, nHosts):
        for h in nHosts:         
            if h not in self.connectedHosts:
                self.connectedHosts[h] = [n]
            else:
                self.connectedHosts[h].append(n)

    def getConnectedHosts(self):
        return self.connectedHosts.keys()
          
    def getIntermediary(self, h):
        if h in self.connectedHosts.keys():
            if(self.connectedHosts[h][0] is False):
                return False
            return self.connectedHosts[h]
        else:
            return False

    #FIXME modify to track paths per endhost
    def trackPaths(self):
        servers = constants.TOPOLOGY.ServerList
        leaves = constants.TOPOLOGY.LeafSwitchList
        for s in servers:
            pps = self.getPossiblePaths(s)
            for pp in pps:
                pp.append(s)
                self.latency_lookup.addPath(pp)

                #Add backward paths as well
                ppReverse = pp.clone()
                ppReverse.reverse()
                self.latency_lookup.addPath(ppReverse)

        for l in leaves:
            if l == self:
                continue
            pps = self.getPossiblePaths(l)
            for pp in pps:
                pp.append(l)
                self.latency_lookup.addPath(pp)

    def enqueueTask(self, task):
        #if(self.isLeaf()):
            #task.swDebug.append(self)
            #if(len(task.swDebug)>2):
            #    print task.id, task.src.getUppers(), task.dst.getUppers(), task.response, task.swDebug
            #    assert False
        executor = Executor(self, task)
        Simulation.activate(executor, executor.run(), Simulation.now())

    def getServiceTime(self):
        return self.procTime
        
        #Dynamic Load-Balaning algorithm (DLB) from OpenFlow based Load Balancing for Fat-Tree Networks with Multipath Support
        #TODO this should be relocated to the topology class
        #TODO getNextHop should retrieve the entire path
    def getNextHop(self, src, dst, forwardingStrat = False):
        egressPort = False
        if(not forwardingStrat):
            forwardingStrat = self.forwardingStrategy
            if("oracle" in self.selectionStrategy):
                forwardingStrat = "oracle"
#            elif(self.selectionStrategy == "passive" or self.selectionStrategy == "switch_c3"):
#                forwardingStrat = "local"


        #check if I'm direct neighbors with dst
        #If I'm the destination leaf whilst not being the source leave as well
        if(self.isNeighbor(dst)):
            egressPort = self.getPort(dst)
        #check if I'm connected to dst through intermediary node
        else:
            if(self.getIntermediary(dst)):
                possible_hops = self.getIntermediary(dst)
            else: 
                possible_hops = self.getUppers()

            if(forwardingStrat == "ecmp"):
                egressPort = self.getPort(random.choice(possible_hops))                
            elif(forwardingStrat == "local"):
                egressPort = self.getPort(min(possible_hops, key=lambda n: self.getPort(n).getQueueSize()))
            elif(forwardingStrat == "CONGA"):
                egressPort, CE = self.congestionTable.getTo(self, [self.getPort(n) for n in possible_hops])
            elif(forwardingStrat == "oracle"):
                egressPort = self.getPort(min(possible_hops, key=lambda n: self.getLatency(src, n, dst, constants.PACKET_SIZE)))
            elif(forwardingStrat == "passive"):
                egressPort = self.getPort(random.choice(possible_hops))
        return egressPort

    def getHopCount(self, dst):
        #print 'DST', dst.id, dst.htype
        hopCount = 0
        nextNode = self
        while True:
            hopCount += 1
            nextPort = nextNode.getNextHop(dst)
            #print 'port', nextPort.__class__.__name__
            nextNode = nextPort.dst
            #print 'node', nextNode.__class__.__name__
            #print nextNode.id, dst.id, nextNode.htype, dst.htype
            if(nextNode.id == dst.id and nextNode.htype == dst.htype):
                #print 'returning..'
                return hopCount

    def getPossibleHops(self, dst):
        if(self.getIntermediary(dst)):
            possible_hops = self.getIntermediary(dst)
        else:
            possible_hops = self.getUppers()
        return possible_hops

    def getPossiblePaths(self, dst):
        if(self.isNeighbor(dst)):
            p = plu.Path()
            #print dst
            p.append(self)
            #print p
            return [p]
        allpps = []
        for nh in self.getPossibleHops(dst):
            pps = nh.getPossiblePaths(dst)
            for pp in pps:
                pp.prepend(self)
                #pp.append(dst)
            allpps.extend(pps)
        return allpps

    def getLatency(self, src, nextNode, dst, size):
        latency = 0
        nextPort = self.getPort(nextNode)
        count = 0
        while True:
            nextNode = nextPort.dst
            count += 1
            #print count, nextNode, "src:", src, "dst:", dst
            latency += self.getHopTxTime(nextPort, size)
            if(nextNode.id == dst.id and nextNode.htype == dst.htype):
                #print 'latency:', latency
                return latency
            #FIXME should the next hop be obtained using local?
            #FIXME set it to the forwarding strategy used by the selection strat
            nextPort = nextNode.getNextHop(src, dst, False)

    def updateRates(self, replica):
        #Update received rate
        self.receiveRate[replica].add(1)
        
        # Cubic Parameters go here
        # beta = 0.2
        # C = 0.000004
        # Smax = 10
        beta = self.cubicBeta
        C = self.cubicC
        Smax = self.cubicSmax
        hysterisisFactor = self.hysterisisFactor
        currentSendingRate = self.rateLimiters[replica].rate
        currentReceiveRate = self.receiveRate[replica].getRate()

        if (currentSendingRate < currentReceiveRate):
            # This means that we need to bump up our own rate.
            # For this, increase the rate according to a cubic
            # window. Rmax is the sending-rate at which we last
            # observed a congestion event. We grow aggressively
            # towards this point, and then slow down, stabilise,
            # and then advance further up. Every rate
            # increase is capped by Smax.
            T = Simulation.now() - self.lastRateDecrease[replica]
            self.lastRateIncrease[replica] = Simulation.now()
            Rmax = self.valueOfLastDecrease[replica]

            newSendingRate = C * (T - (Rmax * beta/C)**(1.0/3.0))**3 + Rmax

            if (newSendingRate - currentSendingRate > Smax):
                self.rateLimiters[replica].rate += Smax
            else:
                self.rateLimiters[replica].rate = newSendingRate
        elif (currentSendingRate > currentReceiveRate
              and Simulation.now() - self.lastRateIncrease[replica]
              > self.rateInterval * hysterisisFactor):
            # The hysterisis factor in the condition is to ensure
            # that the receive-rate measurements have enough time
            # to adapt to the updated rate.

            # So we're in here now, which means we need to back down.
            # Multiplicatively decrease the rate by a factor of beta.
            self.valueOfLastDecrease[replica] = currentSendingRate
            self.rateLimiters[replica].rate *= beta
            self.rateLimiters[replica].rate = \
                max(self.rateLimiters[replica].rate, 0.0001)
            self.lastRateDecrease[replica] = Simulation.now()

        assert (self.rateLimiters[replica].rate > 0)
    
    def __str__(self):
        return str(self.id)
    
    def __repr__(self):
        return self.__str__()

class Executor(Simulation.Process):

    def __init__(self, switch, task):
        self.switch = switch
        self.task = task
        self.log = self.switch.log
        Simulation.Process.__init__(self, name='Executor')

    def run(self):
        # Find next hop and forward packet
        #FIXME there might be a problem for writes since we can allocate them to a different replica
        #FIXME for clairvoyant reads, we need to update responses to remove task from lookup table
        #FIXME C4's replica selection logic shouldn't be tied to its load-balancing counterpart
        yield Simulation.hold, self, self.switch.procTime
        task_size = constants.PACKET_SIZE

        forwardingStrat = self.switch.forwardingStrategy
        if(self.task.trafficType == constants.BACKGROUND):
            forwardingStrat = "ECMP"

        if (self.switch.isNeighbor(self.task.src) and not self.task.response and self.task.trafficType == constants.APP\
            and self.switch.selectionStrategy == "C4"):
            #perform replica selection
            self.task.dst = self.getTaskDst(self.task)

        #if I'm a spine switch or a direct neighbor to both src and dst just forward packet along path
        src_and_dst = (self.switch.isNeighbor(self.task.src) and self.switch.isNeighbor(self.task.dst))
        if(self.switch.isSpine() or src_and_dst):
            egressPort = self.switch.getNextHop(self.task.src, self.task.dst, "local")
            #----CONGA----
            if(self.task.ce<(len(egressPort.buffer.waitQ) + 1)*egressPort.getTxTime(self.task)):
                self.task.setCE((len(egressPort.buffer.waitQ) + 1)*egressPort.getTxTime(self.task))
            if(src_and_dst and forwardingStrat == "C4"):
                #this is a request packet
                if(not self.task.response):
                    shortestPath = self.switch.latency_lookup.getShortestPath(self.task)
                    self.task.switchFB["srcLeafArrival"] = Simulation.now()
                    self.log.debug("Arrival %s isResponse:%s %s"%(self.task, self.task.response, self.switch))
                #this is a response packet
                else:
                    if("forwardLatency" in self.task.switchFB):
                        latency = self.task.switchFB["forwardLatency"]
                        path = self.switch.latency_lookup.history[self.task.id][0]
                        self.switch.latency_lookup.updatePath(latency, path)
                        self.log.debug("Forwardpath Latency %s %s %s %s %s"%(self.task, self.task.response, self.switch,\
                                        path, self.switch.latency_lookup.get(path)))
                    if("srvDeparture" in self.task.switchFB):
                        backpathLatency = Simulation.now() - self.task.switchFB["srvDeparture"]
                        path = plu.Path()
                        path.append(self.task.src)
                        path.append(self.switch)
                        self.switch.latency_lookup.updatePath(backpathLatency, path)
                        self.log.debug("Backwardpath Latency %s %s %s %s %s"%(self.task, self.task.response, self.switch, \
                                        path, self.switch.latency_lookup.get(path)))

        else:
            #this is a request packet
            if(not self.task.response):
                if(self.switch.isNeighbor(self.task.src)):
                    if(forwardingStrat == "CONGA"):
                        egressPort, CE = self.switch.congestionTable.getTo(self, [self.switch.getPort(n)\
                                        for n in self.switch.getPossibleHops(self.task.dst)])
                    elif(forwardingStrat == "ECMP"):
                        egressPort = self.switch.getPort(random.choice(self.switch.getPossibleHops(self.task.dst)))
                    elif(forwardingStrat == "C4"):
                        shortestPath = self.switch.latency_lookup.getShortestPath(self.task)
                        #print shortestPath
                        self.task.switchFB["forwardPath"] = shortestPath
                        self.task.switchFB["srcLeafArrival"] = Simulation.now()
                        egressPort = self.switch.getPort(shortestPath.getNextHop(self.switch))
                        self.log.debug("Arrival %s isResponse:%s %s"%(self.task, self.task.response, self.switch))
                    elif(forwardingStrat == "Clairvoyant"):
                        shortestPath = self.switch.latency_lookup.getShortestPath_oracle(self.task)
                        egressPort = self.switch.getPort(shortestPath.getNextHop(self.switch))
                else:
                    #calculate latency and update task
                    #the following condition might not be true if the packet is a drop notification
                    if(forwardingStrat == "C4"):
                        self.log.debug("Arrival %s isResponse:%s %s"%(self.task, self.task.response, self.switch))
                        #backpath from src leaf switch to dst leaf switch
                        if "srcLeafArrival" in self.task.switchFB and "forwardPath" in self.task.switchFB:
                            backpathLatency = Simulation.now() - self.task.switchFB["srcLeafArrival"]
                            backpath = self.task.switchFB["forwardPath"].clone()
                            #remove last node since we're only measuring latency up until dst leaf
                            backpath.removeLastNode()
                            self.switch.latency_lookup.updatePath(backpathLatency, backpath)
                            self.switch.log.debug("Backpath Latency %s %s %s %s %s"%(self.task, self.task.response, \
                                                    self.switch, backpath, self.switch.latency_lookup.get(backpath)))
                    egressPort = self.switch.getPort(self.task.dst)
            #this is a response packet
            else:
                if(self.switch.isNeighbor(self.task.src)):
                    #construct path and forward
                    if(forwardingStrat == "CONGA"):
                        egressPort, CE = self.switch.congestionTable.getTo(self, [self.switch.getPort(n)\
                                        for n in self.switch.getPossibleHops(self.task.dst)])
                    elif(forwardingStrat == "ECMP"):
                        egressPort = self.switch.getPort(random.choice(self.switch.getPossibleHops(self.task.dst)))
                    elif(forwardingStrat == "C4"):
                        #FIXME why are we setting history to false?
                        shortestPath = self.switch.latency_lookup.getShortestPathToLeaf(self.task, True, False)
                        if("forwardPath" in self.task.switchFB):
                            shortestPath = shortestPath.clone()
                            shortestPath.prepend(self.task.switchFB["forwardPath"].getLastNode())
                            self.task.switchFB["forwardPath"] = shortestPath
                        self.task.switchFB["srcLeafArrival"] = Simulation.now()
                        egressPort = self.switch.getPort(shortestPath.getNextHop(self.switch))
                        self.log.debug("Arrival %s isResponse:%s %s"%(self.task, self.task.response, self.switch))
                    elif(forwardingStrat == "Clairvoyant"):
                        shortestPath = self.switch.latency_lookup.getShortestPathToLeaf_oracle(self.task, True, False)
                        egressPort = self.switch.getPort(shortestPath.getNextHop(self.switch))

                else:
                    if(forwardingStrat == "C4"):
                        if("forwardLatency" in self.task.switchFB):
                            #update latency of forward path
                            latency = self.task.switchFB["forwardLatency"]
                            self.log.debug("Forwardpath Latency %s %s %s %s %s"%(self.task, self.task.response, \
                                           self.switch, self.switch.latency_lookup.history[self.task.id][0], \
                                           self.switch.latency_lookup.get(self.switch.latency_lookup.history[self.task.id][0])))
                            self.switch.latency_lookup.updatePathForTask(latency, self.task)
                        if("srvDeparture" in self.task.switchFB):
                            #update latency of reverse path
                            backpathLatency = Simulation.now() - self.task.switchFB["srvDeparture"]
                            self.switch.latency_lookup.updatePath(backpathLatency, self.task.switchFB["forwardPath"])
                            self.switch.log.debug("Backpath Latency %s %s %s %s %s %s"%(self.task, self.task.response,\
                                                self.switch, self.task.switchFB["forwardPath"], \
                                                self.switch.latency_lookup.get(self.task.switchFB["forwardPath"]), \
                                                self.task.switchFB["srvDeparture"]))
                            self.log.debug("Arrival %s isResponse:%s %s"%(self.task, self.task.response, self.switch))
                    egressPort = self.switch.getPort(self.task.dst)


        #----CONGA----
        #If I'm the source leaf, add congestion parameters
        if(self.switch.isNeighbor(self.task.src)):
            self.task.setCE(0, egressPort)
            port, metric = self.switch.congestionTable.getFrom(self.task.dst.getUppers()[0])
            if(port):
                self.task.fb = port
                self.task.fbMetric = metric

        #If I'm the destination leaf, update congestion Table and server aggregates
        if(self.switch.isNeighbor(self.task.dst)):
            self.switch.congestionTable.updateFrom(self.task.src.getUppers()[0], self.task.lbTag, self.task.ce)
            #if stats for the reverse path are piggy-backed, update To table as well
            if(self.task.fb is not None):
                self.switch.congestionTable.updateTo(self.task.src.getUppers()[0], self.task.fb, self.task.fbMetric)

            #If this a server response and this is the first packet (so we don't have to aggregate stats multiple times)
            if(self.task.dst.isClient() and self.task.seqN == 1 and self.task.trafficType == constants.APP and not self.task.isCut):
                #print '>>>>>>', self.task.serverFB["queueSizeAfter"], self.task.serverFB["serviceTime"]
                self.updateEma(self.task.src, self.task.serverFB["queueSizeAfter"], self.switch.queueSizeMap)
                self.updateEma(self.task.src, self.task.serverFB["serviceTime"], self.switch.serviceTimeMap)
                self.updateEma(self.task.src, self.task.serverFB["queueSizeAfter"], self.switch.cqueueSizeMap[self.task.dst])
                self.updateEma(self.task.src, self.task.serverFB["serviceTime"], self.switch.cserviceTimeMap[self.task.dst])

        egressPort.enqueueTask(self.task)


    def getTaskDst(self, task):
        self.log.debug("-----Choosing replica using %s-----"%self.switch.selectionStrategy)
        minTotalDelay = constants.MAXREQUESTDELAY
        bestDst = task.replicaSet[0]
        for replica in task.replicaSet:
            task.dst = replica

            if(self.switch.selectionStrategy == "C4"):
                shortestToPath = self.switch.latency_lookup.getShortestPath(task, True, False)
                latency1 = self.switch.latency_lookup.get(shortestToPath)
                latency2 = (self.switch.queueSizeMap[replica]+1) * self.switch.serviceTimeMap[replica]
                shortestBackPath = self.switch.latency_lookup.getShortestPath(task, False, False)
                latency3 = self.switch.latency_lookup.get(shortestBackPath)
            elif(self.switch.selectionStrategy == "Clairvoyant"):
                print 'Unleash the powers of foresight!'
                shortestToPath = self.switch.latency_lookup.getShortestPath_oracle(task, True, False)
                latency1 = self.switch.latency_lookup.getLatency_oracle(shortestToPath)
                latency2 = (len(replica.queueResource.waitQ)+1) * self.switch.getServiceTime()
                shortestBackPath = self.switch.latency_lookup.getShortestPath_oracle(task, False, False)
                latency3 = self.switch.latency_lookup.getLatency_oracle(shortestBackPath)

            #self.log.debug(shortestToPath)
            #self.log.debug(shortestBackPath)
            #self.log.debug(self.switch.latency_lookup.bck_path_latencies)
            #self.log.debug(self.switch.latency_lookup.fwd_path_latencies)
            self.log.debug("Choice -- %s %s : P1:%s P2:%s P3:%s"%(replica, replica.getUppers()[0], latency1, latency2, latency3))
            if(latency1 + latency2 + latency3 < minTotalDelay):
                bestDst = replica
                minTotalDelay = latency1 + latency2 + latency3
        return bestDst


    def updateEma(self, replica, metric, map):
        alpha = 0.9
        map[replica] = (alpha)*metric + (1-alpha)*map[replica]
        
    def calculateC4Congestion(self, server):
        ce = self.switch.congestionTable.getToCE(server.getUppers()[0])
        #print 'Get congestion To', self.switch.id
        #print self.switch.congestionTable.congestionTo
        
        #print ce, self.switch.queueSizeMap[server] * self.switch.serviceTimeMap[server]
        total = self.switch.c4Weight * self.switch.queueSizeMap[server] * self.switch.serviceTimeMap[server] + 2*(1-self.switch.c4Weight)*ce
        return total

    def calculateOracleExpDelay(self, client, server):
        if(self.switch.isNeighbor(server)):
            possible_hops = [server]
        else:
            possible_hops = self.switch.getUppers()

        latency = min(self.switch.getLatency(client, n, server, 1) for n in possible_hops)
        #total = latency + (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        total = latency
        self.log.debug("From client:%s to server:%s, Latency:%f, Server:%f, Total:%f"%(client, server, latency, total-latency, total))
        return total

    def calculateOracleExpDelay_client(self, client, server):
        if(self.switch.isNeighbor(server)):
            possible_hops = [server]
        else:
            possible_hops = self.switch.getUppers()
        latency = min(self.switch.getLatency(client, n, server, constants.PACKET_SIZE) for n in possible_hops)
        #total = self.switch.getLatency(server, constants.PACKET_SIZE) + (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        total = latency + (self.switch.cqueueSizeMap[client][server]+1) * self.switch.cserviceTimeMap[client][server]
        #total = (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        #total = self.switch.getLatency(server, constants.PACKET_SIZE)
        #print (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        return total
        
    def calculateOracleExpDelay_reverse(self, client, server, size):
        if(server.getUppers()[0].isNeighbor(client)):
            possible_hops = [client]
        else:
            possible_hops = server.getUppers()[0].getUppers()
        servToLeafPort = server.getPort(server.getUppers()[0])
        servToLeafLatency = server.getHopTxTime(servToLeafPort, size)
        latency = min(server.getUppers()[0].getLatency(server, n, client, size) for n in possible_hops)
        latency += servToLeafLatency
        total = latency + (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        #print latency, (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        return total

    def calculateOracleExpDelay_reverse_avg(self, client, server):
        size = 16
        if(server.getUppers()[0].isNeighbor(client)):
            possible_hops = [client]
        else:
            possible_hops = server.getUppers()[0].getUppers()
        servToLeafPort = server.getPort(server.getUppers()[0])
        servToLeafLatency = server.getHopTxTime(servToLeafPort, size)
        latency = min(server.getUppers()[0].getLatency(server, n, client, size) for n in possible_hops)
        latency += servToLeafLatency
        #total = self.switch.getLatency(server, constants.PACKET_SIZE) + (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        total = latency + (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        #total = (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        #total = self.switch.getLatency(server, constants.PACKET_SIZE)
        #print (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        return total

    def calculateOracleExpDelay_reverse_avg_client(self, client, server):
        size = 16
        if(server.getUppers()[0].isNeighbor(client)):
            possible_hops = [client]
        else:
            possible_hops = server.getUppers()[0].getUppers()
        servToLeafPort = server.getPort(server.getUppers()[0])
        servToLeafLatency = server.getHopTxTime(servToLeafPort, size)
        latency = min(server.getUppers()[0].getLatency(server, n, client, size) for n in possible_hops)
        latency += servToLeafLatency
        #total = self.switch.getLatency(server, constants.PACKET_SIZE) + (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        total = latency + (self.switch.cqueueSizeMap[client][server]+1) * self.switch.cserviceTimeMap[client][server]
        #total = (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        #total = self.switch.getLatency(server, constants.PACKET_SIZE)
        #print (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        return total
       
    def calculateOracleExpDelay_reverse_client(self, client, server, size):
        if(server.getUppers()[0].isNeighbor(client)):
            possible_hops = [client]
        else:
            possible_hops = server.getUppers()[0].getUppers()
        servToLeafPort = server.getPort(server.getUppers()[0])
        servToLeafLatency = server.getHopTxTime(servToLeafPort, size)
        latency = min(server.getUppers()[0].getLatency(server, n, client, size) for n in possible_hops)
        latency += servToLeafLatency
        #total = self.switch.getLatency(server, constants.PACKET_SIZE) + (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        total = latency + (self.switch.cqueueSizeMap[client][server]+1) * self.switch.cserviceTimeMap[client][server]
        #total = (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        #total = self.switch.getLatency(server, constants.PACKET_SIZE)
        #print (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        return total

    def calculateOracleExpDelay_all(self, client, server, size):
        if(server.getUppers()[0].isNeighbor(client)):
            possible_hops_rev = [client]
        else:
            possible_hops_rev = server.getUppers()[0].getUppers()
        if(self.switch.isNeighbor(server)):
            possible_hops = [server]
        else:
            possible_hops = self.switch.getUppers()
        servToLeafPort = server.getPort(server.getUppers()[0])
        servToLeafLatency = server.getHopTxTime(servToLeafPort, size)
        clientToServLatency = min(self.switch.getLatency(client, n, server, constants.PACKET_SIZE) for n in possible_hops)
        latency = min(server.getUppers()[0].getLatency(server, n, client, size) for n in possible_hops_rev)
        latency += (servToLeafLatency+clientToServLatency)
        total = latency + (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        #print latency, (self.switch.queueSizeMap[server]+1) * self.switch.serviceTimeMap[server]
        return total 

    def calculateOracleExpDelay_c3(self, client, server, size):
        pendingRequests = 0
        if(server.getUppers()[0].isNeighbor(client)):
            possible_hops_rev = [client]
        else:
            possible_hops_rev = server.getUppers()[0].getUppers()
        if(self.switch.isNeighbor(server)):
            possible_hops = [server]
        else:
            possible_hops = self.switch.getUppers()
        servToLeafPort = server.getPort(server.getUppers()[0])
        servToLeafLatency = server.getHopTxTime(servToLeafPort, size)
        clientToServLatency = min(self.switch.getLatency(client, n, server, constants.PACKET_SIZE) for n in possible_hops)
        latency = min(server.getUppers()[0].getLatency(server, n, client, size) for n in possible_hops_rev)
        latency += (servToLeafLatency+clientToServLatency)
        
        for h in self.switch.getDowners():
            if(h.isClient()):
                pendingRequests += h.pendingRequestsMap[server]
        
        theta = (1 + (pendingRequests/len(self.switch.getDowners()))
                 * constants.NUMBER_OF_CLIENTS
                 + self.switch.queueSizeMap[server])
        total = (theta ** 3) * self.switch.serviceTimeMap[server] + latency
        return total
    
    def calculateExpDelay_c3(self, client, server, size):
        #TODO calculate latency by subtracting service time from response time
        pendingRequests = 0
        latency = 0.0
        count = 0
        service = 0
        for h in self.switch.getDowners():
            if(h.isClient()):
                metricMap = h.expectedDelayMap.get(server)
                if(len(metricMap)>0):
                    latency += metricMap["nw"]
                    service += metricMap["serviceTime"]
                    pendingRequests += h.pendingRequestsMap[server]
                    count += 1
        if(count>0):
            latency = latency/count
            service = service/count
            pendingRequests = pendingRequests/count
        theta = (1 + pendingRequests
                 * constants.NUMBER_OF_CLIENTS
                 + self.switch.queueSizeMap[server])
        total = (theta ** 3) * service + latency
        return total

    def calculateExpDelay_c3_client(self, client, server, size, original_replica):
        latency = service = queueSize = pendingRequests = 0
        metricMap = client.expectedDelayMap.get(server)
        if(len(metricMap)>0):
            latency += metricMap["nw"]
            service += metricMap["serviceTime"]
            queueSize += metricMap["queueSizeAfter"]
            pendingRequests += client.pendingRequestsMap[server]
            if(original_replica == server):
                pendingRequests -= 1
        theta = (1 + pendingRequests
                 * constants.NUMBER_OF_CLIENTS
                 + queueSize)
        total = (theta ** 3) * service + latency
        #if(client.id == "Client23" and server.id == 5):
        #    print "SW", client.id, server.id, total
        return total

    def calculateOracleExpDelay_c3_latency(self, server):
        #Switch aggregation, similar to C3 but without latency
        pendingRequests = 0
        for h in self.switch.getDowners():
            if(h.isClient()):
                pendingRequests += h.pendingRequestsMap[server]
        
        theta = (1 + (pendingRequests/len(self.switch.getDowners()))
                 * constants.NUMBER_OF_CLIENTS
                 + self.switch.queueSizeMap[server])
        total = (theta ** 3) * self.switch.serviceTimeMap[server] + self.switch.getLatency(server, constants.PACKET_SIZE)
        return total
    
class DREUpdater(Simulation.Process):
    
    def __init__(self, switch):
        self.switch = switch
        Simulation.Process.__init__(self, name='DREUpdater')
    def run(self):
        while not constants.END_SIMULATION:
            yield Simulation.hold, self, constants.CE_UPDATE_PERIOD
            for p in self.switch.neighbors.values():
                p.updateDRE()

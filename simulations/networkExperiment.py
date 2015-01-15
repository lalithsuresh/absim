import SimPy.Simulation as Simulation
import server
import client
import workload
import argparse
import random
import constants
import numpy
import sys
import muUpdater
import statcollector
import topology
import simpletopo
import numpy as np
import leafspineTopo
#from scipy.stats.kde import gaussian_kde
#from matplotlib import pyplot as plt
#from matplotlib.font_manager import FontProperties

def printMonitorTimeSeriesToFile(fileDesc, prefix, monitor):
    for entry in monitor:
        fileDesc.write("%s %s %s\n" % (prefix, entry[0], entry[1]))


# class WorkloadUpdater(Simulation.Process):
#     def __init__(self, workload, value, clients, servers):
#         self.workload = workload
#         self.value = value
#         self.clients = clients
#         self.servers = servers
#         Simulation.Process.__init__(self, name='WorkloadUpdater')

#     def run(self):
#         # while(1):
#             yield Simulation.hold, self,
#             # self.workload.model_param = random.uniform(self.value,
#                                                        # self.value * 40)
#             # old = self.workload.model_param
#             # self.workload.model_param = self.value
#             # self.workload.clientList = self.clients
#             # self.workload.total = \
#             #     sum(client.demandWeight for client in self.clients)
#             # yield Simulation.hold, self, 1000
#             # self.workload.model_param = old
#             self.servers[0].serviceTime = 1000


class ClientAdder(Simulation.Process):
    def __init__(self,):
        Simulation.Process.__init__(self, name='ClientAdder')

    def run(self, clientToAdd):
        yield Simulation.hold, self,

#misc statistics functions
def movingAverage (values, window):
    weights = numpy.repeat(1.0, window)/window
    sma = numpy.convolve(values, weights, 'valid')
    return sma

def runExperiment(args):

    # Set the random seed
    random.seed(args.seed)
    numpy.random.seed(args.seed)

    Simulation.initialize()

    servers = []
    clients = []
    workloadGens = []
    
    constants.NW_LATENCY_BASE = args.nwLatencyBase
    constants.NW_LATENCY_MU = args.nwLatencyMu
    constants.NW_LATENCY_SIGMA = args.nwLatencySigma
    constants.NUMBER_OF_CLIENTS = args.numClients
    constants.SWITCH_BUFFER_SIZE = args.switchBufferSize
    constants.PACKET_SIZE = args.packetSize
    
    assert args.expScenario != ""

    serviceRatePerServer = []
    if (args.expScenario == "base"):
        # Start the servers
        for i in range(args.numServers):
            serv = server.Server(i,
                                 resourceCapacity=args.serverConcurrency,
                                 serviceTime=(args.serviceTime),
                                 serviceTimeModel=args.serviceTimeModel)
            servers.append(serv)
    elif(args.expScenario == "multipleServiceTimeServers"):
        # Start the servers
        for i in range(args.numServers):
            serv = server.Server(i,
                                 resourceCapacity=args.serverConcurrency,
                                 serviceTime=((i + 1) * args.serviceTime),
                                 serviceTimeModel=args.serviceTimeModel)
            servers.append(serv)
    elif(args.expScenario == "heterogenousStaticServiceTimeScenario"):
        baseServiceTime = args.serviceTime

        assert args.slowServerFraction >= 0 and args.slowServerFraction < 1.0
        assert args.slowServerSlowness >= 0 and args.slowServerSlowness < 1.0
        assert not (args.slowServerSlowness == 0
                    and args.slowServerFraction != 0)
        assert not (args.slowServerSlowness != 0
                    and args.slowServerFraction == 0)

        if(args.slowServerFraction > 0.0):
            slowServerRate = (args.serverConcurrency *
                              1/float(baseServiceTime)) *\
                args.slowServerSlowness
            numSlowServers = int(args.slowServerFraction * args.numServers)
            slowServerRates = [slowServerRate] * numSlowServers

            numFastServers = args.numServers - numSlowServers
            totalRate = (args.serverConcurrency *
                         1/float(args.serviceTime) * args.numServers)
            fastServerRate = (totalRate - sum(slowServerRates))\
                / float(numFastServers)
            fastServerRates = [fastServerRate] * numFastServers
            serviceRatePerServer = slowServerRates + fastServerRates
        else:
            serviceRatePerServer = [args.serverConcurrency *
                                    1/float(args.serviceTime)] * args.numServers

        random.shuffle(serviceRatePerServer)
        # print sum(serviceRatePerServer), (1/float(baseServiceTime)) * args.numServers
        assert sum(serviceRatePerServer) > 0.99 *\
            (1/float(baseServiceTime)) * args.numServers
        assert sum(serviceRatePerServer) <=\
            (1/float(baseServiceTime)) * args.numServers

        # Start the servers
        for i in range(args.numServers):
            st = 1/float(serviceRatePerServer[i])
            serv = server.Server(i,
                                 resourceCapacity=args.serverConcurrency,
                                 serviceTime=st,
                                 serviceTimeModel=args.serviceTimeModel)
            servers.append(serv)
    elif(args.expScenario == "timeVaryingServiceTimeServers"):
        assert args.intervalParam != 0.0
        assert args.timeVaryingDrift != 0.0

        # Start the servers
        for i in range(args.numServers):
            serv = server.Server(i,
                                 resourceCapacity=args.serverConcurrency,
                                 serviceTime=(args.serviceTime),
                                 serviceTimeModel=args.serviceTimeModel)
            mup = muUpdater.MuUpdater(serv, args.intervalParam,
                                      args.serviceTime,
                                      args.timeVaryingDrift)
            Simulation.activate(mup, mup.run(), at=0.0)
            servers.append(serv)
    else:
        print "Unknown experiment scenario"
        sys.exit(-1)

    baseDemandWeight = 1.0
    clientWeights = []
    assert args.highDemandFraction >= 0 and args.highDemandFraction < 1.0
    assert args.demandSkew >= 0 and args.demandSkew < 1.0
    assert not (args.demandSkew == 0 and args.highDemandFraction != 0)
    assert not (args.demandSkew != 0 and args.highDemandFraction == 0)

    if(args.highDemandFraction > 0.0 and args.demandSkew >= 0):
        heavyClientWeight = baseDemandWeight *\
            args.demandSkew/args.highDemandFraction
        numHeavyClients = int(args.highDemandFraction * args.numClients)
        heavyClientWeights = [heavyClientWeight] * numHeavyClients

        lightClientWeight = baseDemandWeight *\
            (1 - args.demandSkew)/(1 - args.highDemandFraction)
        numLightClients = args.numClients - numHeavyClients
        lightClientWeights = [lightClientWeight] * numLightClients
        clientWeights = heavyClientWeights + lightClientWeights
    else:
        clientWeights = [baseDemandWeight] * args.numClients
    #TODO commenting this for now
    #assert sum(clientWeights) > 0.99 * args.numClients
    assert sum(clientWeights) <= args.numClients
    # Start the clients
    for i in range(args.numClients):
        c = client.Client(id_="Client%s" % (i),
                          serverList=servers,
                          replicaSelectionStrategy=args.selectionStrategy,
                          accessPattern=args.accessPattern,
                          replicationFactor=args.replicationFactor,
                          backpressure=args.backpressure,
                          shadowReadRatio=args.shadowReadRatio,
                          rateInterval=args.rateInterval,
                          cubicC=args.cubicC,
                          cubicSmax=args.cubicSmax,
                          cubicBeta=args.cubicBeta,
                          hysterisisFactor=args.hysterisisFactor,
                          demandWeight=clientWeights[i])
        clients.append(c)

    # Start workload generators (analogous to YCSB)
    latencyMonitor = Simulation.Monitor(name="Latency")

    #Construct topology and connect nodes
    #topo = topology.Topology(args, clients, servers)
    print "chosen selection strategy:", args.switchSelectionStrategy
    topo = leafspineTopo.LeafSpinTopology(args.iSpine, args.iLeaf, args.hostsPerLeaf, args.spineLeafBW,
                             args.leafHostBW, args.procTime, clients, servers, args.switchSelectionStrategy,
                             args.switchForwardingStrategy, args.c4Weight, args.rateInterval, args.cubicC,
                             args.cubicSmax, args.cubicBeta, args.hysterisisFactor, args.switchRateLimiter)
    #topo = simpletopo.SimpleTopology(args, clients, servers)
    topo.createTopo()
    #topo.draw()

    for i in range(args.numWorkload):
        w = workload.Workload(i, latencyMonitor,
                              clients,
                              args.workloadModel,
                              args.interarrivalTime * args.numWorkload,
                              args.numRequests/args.numWorkload, args.valueSizeModel,
                              i*args.numRequests/args.numWorkload)
        Simulation.activate(w, w.run(),
                            at=0.0),
        workloadGens.append(w)
    
    sc = statcollector.StatCollector(clients, servers, topo.getSwitches(), workloadGens)
    Simulation.activate(sc, sc.run(0.1), at=0.0)
    
    # Begin simulation
    Simulation.simulate(until=args.simulationDuration)

    #
    # Print a bunch of timeseries
    #
    pendingRequestsFD = open("../%s/%s_PendingRequests" %
                             (args.logFolder,
                              args.expPrefix), 'w')
    waitMonFD = open("../%s/%s_WaitMon" % (args.logFolder,
                                           args.expPrefix), 'w')
    actMonFD = open("../%s/%s_ActMon" % (args.logFolder,
                                         args.expPrefix), 'w')
    latencyFD = open("../%s/%s_Latency" % (args.logFolder,
                                           args.expPrefix), 'w')
    latencyTrackerFD = open("../%s/%s_LatencyTracker" %
                            (args.logFolder, args.expPrefix), 'w')
    rateFD = open("../%s/%s_Rate" % (args.logFolder,
                                     args.expPrefix), 'w')
    tokenFD = open("../%s/%s_Tokens" % (args.logFolder,
                                        args.expPrefix), 'w')
    receiveRateFD = open("../%s/%s_ReceiveRate" % (args.logFolder,
                                                   args.expPrefix), 'w')
    edScoreFD = open("../%s/%s_EdScore" % (args.logFolder,
                                           args.expPrefix), 'w')
    serverRRFD = open("../%s/%s_serverRR" % (args.logFolder,
                                             args.expPrefix), 'w')
    clientQErr = open("../%s/%s_clientQErr" % (args.logFolder,
                                           args.expPrefix), 'w') 
    clientSelError = open("../%s/%s_clientSelErr" % (args.logFolder,
                                           args.expPrefix), 'w')
    backlogFD = open("../%s/%s_backlog" % (args.logFolder,
                                           args.expPrefix), 'w')   
    reqResDiffFD = open("../%s/%s_reqResDiff" % (args.logFolder,
                                           args.expPrefix), 'w')
    printMonitorTimeSeriesToFile(reqResDiffFD,
                                 "0", sc.reqResDiff)  
     
    for clientNode in clients:
        printMonitorTimeSeriesToFile(clientQErr,
                                     clientNode.id,
                                     clientNode.qErrorMonitor)
        printMonitorTimeSeriesToFile(backlogFD,
                                     clientNode.id,
                                     clientNode.backlogMonitor)
        printMonitorTimeSeriesToFile(clientSelError,
                                     clientNode.id,
                                     clientNode.selErrorMonitor)
        printMonitorTimeSeriesToFile(pendingRequestsFD,
                                     clientNode.id,
                                     clientNode.pendingRequestsMonitor)
        printMonitorTimeSeriesToFile(latencyTrackerFD,
                                     clientNode.id,
                                     clientNode.latencyTrackerMonitor)
        printMonitorTimeSeriesToFile(rateFD,
                                     clientNode.id,
                                     clientNode.rateMonitor)
        printMonitorTimeSeriesToFile(tokenFD,
                                     clientNode.id,
                                     clientNode.tokenMonitor)
        printMonitorTimeSeriesToFile(receiveRateFD,
                                     clientNode.id,
                                     clientNode.receiveRateMonitor)
        printMonitorTimeSeriesToFile(edScoreFD,
                                     clientNode.id,
                                     clientNode.edScoreMonitor)
    for serv in servers:
        printMonitorTimeSeriesToFile(waitMonFD,
                                     serv.id,
                                     serv.queueResource.waitMon)
        printMonitorTimeSeriesToFile(actMonFD,
                                     serv.id,
                                     serv.queueResource.actMon)
        printMonitorTimeSeriesToFile(serverRRFD,
                                     serv.id,
                                     serv.serverRRMonitor)
        #print "------- Server:%s %s ------" % (serv.id, "WaitMon")
        #print "Mean:", serv.queueResource.waitMon.mean()

        #print "------- Server:%s %s ------" % (serv.id, "ActMon")
        #print "Mean:", serv.queueResource.actMon.mean()

    print "------- Latency ------"
    print "Mean Latency:",\
      sum([float(entry[1].split()[0]) for entry in latencyMonitor])/float(len(latencyMonitor))
    a = np.array([float(entry[1].split()[0]) for entry in latencyMonitor])
    p50 = np.percentile(a, 50) # return 50th percentile, e.g. median.
    p99 = np.percentile(a, 99) # return 99th percentile.
    print "Percentile 50:", p50
    print "Percentile 99:", p99
    printMonitorTimeSeriesToFile(latencyFD, "0",
                                 latencyMonitor)
    assert args.numRequests == len(latencyMonitor)
    
#===============================================================================
#     data1 = [] #queue size errors
#     data2 = [] #selection errors
#     data3 = [] #backlog queue size
#     for c in clients:
#         if(not len(c.qErrorMonitor) == 0):
#             data1.append((c.qErrorMonitor.tseries(), c.qErrorMonitor.yseries()))
#             data2.append((c.selErrorMonitor.tseries(), c.selErrorMonitor.yseries()))
#             data3.append((c.backlogMonitor.tseries(), c.backlogMonitor.yseries()))
#     
#     #Get max and min points for the tseries
#     tmin = min(min(clients, key=lambda c: min(c.qErrorMonitor.tseries())).qErrorMonitor.tseries())
#     tmax = max(max(clients, key=lambda c: max(c.qErrorMonitor.tseries())).qErrorMonitor.tseries())
#     steps = 100
# 
#     #100 points evenly spaced along the x axis
#     x_points = numpy.linspace(tmin,tmax,steps)
# 
#     #interpolate values to the evenly spaced points
#     bsize_interpolated = [numpy.interp(x_points,d[0],d[1]) for d in data3]
#     selerr_interpolated = [numpy.interp(x_points,d[0],d[1]) for d in data2]
#     qerr_interpolated = [numpy.interp(x_points,d[0],[float(y.split()[2]) for y in d[1]]) for d in data1]
#     qest_interpolated = [numpy.interp(x_points,d[0],[float(y.split()[1]) for y in d[1]]) for d in data1]
#     qact_interpolated = [numpy.interp(x_points,d[0],[float(y.split()[0]) for y in d[1]]) for d in data1]
#     
#     #Find the corresponding averages and STDs.
#     selerr_averages = [numpy.average(x) for x in zip(*selerr_interpolated)]
#     selerr_moving_average = movingAverage(selerr_averages, 10)
#     qerr_averages = [numpy.average(x) for x in zip(*qerr_interpolated)]
#     qest_sd = [numpy.std(x) for x in zip(*qest_interpolated)]
#     bsize_averages = [numpy.average(x) for x in zip(*bsize_interpolated)]
#     bsize_sd = [numpy.std(x) for x in zip(*bsize_interpolated)]
#     qest_averages = [numpy.average(x) for x in zip(*qest_interpolated)]
#     qact_averages = [numpy.average(x) for x in zip(*qact_interpolated)]
#     
#     #Plot results
#     fontP = FontProperties()
#     fontP.set_size('small')
#     
#     plt.subplot2grid((3,3), (0,0), colspan=3)
#     plt.plot(x_points, qact_averages, label='q_act')
#     plt.errorbar(x_points, qest_averages, qest_sd, label='q_est')
#     plt.legend(prop = fontP)
#     plt.title("Queue Sizes")
#     plt.xlabel("Simulation time")
#     plt.ylabel("Avg. queue size")
#     plt.ylim(ymin=0)
#     
#     plt.subplot2grid((3,3), (1,0))
#     plt.plot(x_points, qerr_averages)
#     plt.title("Queue Size Error")
#     plt.xlabel("Simulation time")
#     plt.ylabel("Mean squared error")
#     
#     plt.subplot2grid((3,3), (1,1))
#     plt.plot(x_points, selerr_averages, label='error distance')
#     plt.plot(x_points[len(x_points)-len(selerr_moving_average):], selerr_moving_average, label='moving avg.')
#     plt.legend(prop = fontP, loc=9, bbox_to_anchor=(0.5, -0.1))
#     plt.title("Selection Error")
#     plt.xlabel("Simulation time")
#     plt.ylabel("Distance")
#     
#     plt.subplot2grid((3,3), (1,2))
#     plt.errorbar(x_points, bsize_averages, bsize_sd)
#     plt.plot(x_points, bsize_averages)
#     plt.title("Backlog Queue Size")
#     plt.xlabel("Simulation time")
#     plt.ylabel("Avg. queue size")
#     plt.ylim(ymin=0)
#     
#     plt.subplot2grid((3,3), (2,0), colspan=3)
#     plt.plot(sc.reqResDiff.tseries(), sc.reqResDiff.yseries())
#     plt.title("Workload gen - resp completed")
#     plt.xlabel("Simulation time")
#     plt.ylabel("Requests #")
# 
#     plt.tight_layout()
#     
#     plt.savefig('../plotting/simresults.png')
#     plt.show()
#===============================================================================
    
    #print "------- Outstanding Requests (Per Server) -----"
    #data = {node: [] for node in servers}
    #for s in servers:
    #    for c in clients:
            #print c.pendingRequestsPerServer[s]
    #        if not len(c.pendingRequestsPerServer[s]) == 0:
    #            data[s].append(c.pendingRequestsPerServer[s].mean())
    #    kde = gaussian_kde(data[s])
    #    dist_space = numpy.linspace(min(data[s]), max(data[s]), 100)
        # plot the results
        #plt.plot(dist_space, kde(dist_space))
        #plt.show()
    #print "------- Queue Size Distribution (Per Server) -----"
    #data = {node: [] for node in servers}
    #for s in servers:
    #    for c in clients:
    #        if not len(c.queueSizePerServer[s]) == 0:
    #            data[s].append(c.queueSizePerServer[s].mean())
    #    kde = gaussian_kde(data[s])
    #    dist_space = numpy.linspace(min(data[s]), max(data[s]), 100)
        # plot the results
        #plt.plot(dist_space, kde(dist_space))
        #plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Absinthe sim.')
    parser.add_argument('--numClients', nargs='?',
                        type=int, default=1)
    parser.add_argument('--numServers', nargs='?',
                        type=int, default=1)
    parser.add_argument('--numWorkload', nargs='?',
                        type=int, default=1)
    parser.add_argument('--serverConcurrency', nargs='?',
                        type=int, default=1)
    parser.add_argument('--serviceTime', nargs='?',
                        type=float, default=1)
    parser.add_argument('--workloadModel', nargs='?',
                        type=str, default="constant")
    parser.add_argument('--interarrivalTime', nargs='?',
                        type=float, default=0.05)
    parser.add_argument('--serviceTimeModel', nargs='?',
                        type=str, default="constant")
    parser.add_argument('--replicationFactor', nargs='?',
                        type=int, default=1)
    parser.add_argument('--selectionStrategy', nargs='?',
                        type=str, default="pending")
    parser.add_argument('--shadowReadRatio', nargs='?',
                        type=float, default=0.10)
    parser.add_argument('--rateInterval', nargs='?',
                        type=int, default=10)
    parser.add_argument('--cubicC', nargs='?',
                        type=float, default=0.000004)
    parser.add_argument('--cubicSmax', nargs='?',
                        type=float, default=10)
    parser.add_argument('--cubicBeta', nargs='?',
                        type=float, default=0.2)
    parser.add_argument('--hysterisisFactor', nargs='?',
                        type=float, default=2)
    parser.add_argument('--backpressure', action='store_true',
                        default=False)
    parser.add_argument('--switchRateLimiter', action='store_true',
                        default=False)
    parser.add_argument('--accessPattern', nargs='?',
                        type=str, default="uniform")
    parser.add_argument('--nwLatencyBase', nargs='?',
                        type=float, default=0.960)
    parser.add_argument('--nwLatencyMu', nargs='?',
                        type=float, default=0.040)
    parser.add_argument('--nwLatencySigma', nargs='?',
                        type=float, default=0.0)
    parser.add_argument('--expPrefix', nargs='?',
                        type=str, default="")
    parser.add_argument('--seed', nargs='?',
                        type=int, default=25072014)
    parser.add_argument('--simulationDuration', nargs='?',
                        type=int, default=500)
    parser.add_argument('--numRequests', nargs='?',
                        type=int, default=100)
    parser.add_argument('--switchBufferSize', nargs='?',
                        type=int, default=10)
    parser.add_argument('--logFolder', nargs='?',
                        type=str, default="logs")
    parser.add_argument('--expScenario', nargs='?',
                        type=str, default="")
    parser.add_argument('--demandSkew', nargs='?',
                        type=float, default=0)
    parser.add_argument('--highDemandFraction', nargs='?',
                        type=float, default=0)
    parser.add_argument('--slowServerFraction', nargs='?',
                        type=float, default=0)
    parser.add_argument('--slowServerSlowness', nargs='?',
                        type=float, default=0)
    parser.add_argument('--intervalParam', nargs='?',
                        type=float, default=0.0)
    parser.add_argument('--timeVaryingDrift', nargs='?',
                        type=float, default=0.0)
    parser.add_argument('--iNumber', nargs='?',
                        type=int, default=4)
    parser.add_argument('--iSpine', nargs='?',
                        type=int, default=4)
    parser.add_argument('--iLeaf', nargs='?',
                        type=int, default=4)
    parser.add_argument('--hostsPerLeaf', nargs='?',
                        type=int, default=4)
    parser.add_argument('--coreAggrBW', nargs='?',
                        type=int, default=100)
    parser.add_argument('--aggrEdgeBW', nargs='?',
                        type=int, default=10)
    parser.add_argument('--edgeHostBW', nargs='?',
                        type=int, default=1)
    parser.add_argument('--leafHostBW', nargs='?',
                        type=float, default=1)
    parser.add_argument('--spineLeafBW', nargs='?',
                        type=float, default=1)
    parser.add_argument('--procTime', nargs='?',
                        type=float, default=0.002)
    parser.add_argument('--valueSizeModel', nargs='?',
                        type=str, default="blabla")
    parser.add_argument('--packetSize', nargs='?',
                        type=float, default=0.00057)
    parser.add_argument('--switchSelectionStrategy', nargs='?',
                        type=str, default="passive")
    parser.add_argument('--switchForwardingStrategy', nargs='?',
                        type=str, default="local")
    parser.add_argument('--c4Weight', nargs='?',
                        type=float, default=0.5) 
    args = parser.parse_args()

    runExperiment(args)

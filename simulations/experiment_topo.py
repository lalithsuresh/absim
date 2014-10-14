import SimPy.Simulation as Simulation
import server
import client
import workload
import argparse
import random
import constants
import numpy
from topology import Topology

def printMonitorTimeSeriesToFile(fileDesc, prefix, monitor):
    for entry in monitor:
        fileDesc.write("%s %s %s\n" % (prefix, entry[0], entry[1]))


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
    parser.add_argument('--workloadParam', nargs='?',
                        type=float, default=1)
    parser.add_argument('--serviceTimeModel', nargs='?',
                        type=str, default="constant")
    parser.add_argument('--replicationFactor', nargs='?',
                        type=int, default=1)
    parser.add_argument('--selectionStrategy', nargs='?',
                        type=str, default="pending")
    parser.add_argument('--shadowReadRatio', nargs='?',
                        type=float, default=0.0)
    parser.add_argument('--backpressure', action='store_true',
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
    parser.add_argument('--iNumber', nargs='?',
                        type=int, default=4)
    parser.add_argument('--coreAggrBW', nargs='?',
                        type=int, default=100)
    parser.add_argument('--aggrEdgeBW', nargs='?',
                        type=int, default=10)
    parser.add_argument('--edgeHostBW', nargs='?',
                        type=int, default=1)
    parser.add_argument('--procTime', nargs='?',
                        type=float, default=1)
    parser.add_argument('--interarrivalModel', nargs='?',
                        type=str, default="constant")
    parser.add_argument('--valueSizeModel', nargs='?',
                        type=str, default="blabla")
    parser.add_argument('--switchBuffer', nargs='?',
                        type=int, default=5)
    parser.add_argument('--placementStrategy', nargs='?',
                        type=str, default="interleave")    
    args = parser.parse_args()

    # Set the random seed
    random.seed(args.seed)
    numpy.random.seed(args.seed)

    Simulation.initialize()

    workloadGens = []

    constants.NW_LATENCY_BASE = args.nwLatencyBase
    constants.NW_LATENCY_MU = args.nwLatencyMu
    constants.NW_LATENCY_SIGMA = args.nwLatencySigma
    constants.SWITCH_BUFFER_SIZE = args.switchBuffer

    #Construct topology and start clients/servers
    topo = Topology(args)
    topo.createTopo()
    
    # Start workload generators (analogous to YCSB)
    latencyMonitor = Simulation.Monitor(name="Latency")
    for i in range(args.numWorkload):
        #print 'client list (before):', topo.ClientList
        w = workload.Workload(i, latencyMonitor)
        Simulation.activate(w, w.run(topo.ClientList,
                                     args.interarrivalModel,
                                     args.workloadParam,
                                     args.numRequests/args.numWorkload),
                            at=0.0),
        workloadGens.append(w)

    # Begin simulation
    Simulation.simulate(until=args.simulationDuration)

    #
    # Print a bunch of timeseries
    #
    pendingRequestsFD = open("../logs/%s_PendingRequests" %
                             (args.expPrefix), 'w')
    waitMonFD = open("../logs/%s_WaitMon" % (args.expPrefix), 'w')
    actMonFD = open("../logs/%s_ActMon" % (args.expPrefix), 'w')
    latencyFD = open("../logs/%s_Latency" % (args.expPrefix), 'w')
    latencyTrackerFD = open("../logs/%s_LatencyTracker" % (args.expPrefix), 'w')

    for clientNode in topo.ClientList:
        printMonitorTimeSeriesToFile(pendingRequestsFD,
                                     clientNode.id,
                                     clientNode.pendingRequestsMonitor)
        printMonitorTimeSeriesToFile(latencyTrackerFD,
                                     clientNode.id,
                                     clientNode.latencyTrackerMonitor)
    for serv in topo.ServerList:
        printMonitorTimeSeriesToFile(waitMonFD,
                                     serv.id,
                                     serv.queueResource.waitMon)
        printMonitorTimeSeriesToFile(actMonFD,
                                     serv.id,
                                     serv.queueResource.actMon)
        print "------- Server:%s %s ------" % (serv.id, "WaitMon")
        print "Mean:", serv.queueResource.waitMon.mean()

        print "------- Server:%s %s ------" % (serv.id, "ActMon")
        print "Mean:", serv.queueResource.actMon.mean()

    print "------- Latency ------"
    print "Mean Latency:", latencyMonitor.mean()

    printMonitorTimeSeriesToFile(latencyFD, "0",
                                 latencyMonitor)
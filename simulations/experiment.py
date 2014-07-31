import SimPy.Simulation as Simulation
import server
import client
import workload
import argparse
import random
import constants
import numpy


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
                        type=float, default=0.10)
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
    args = parser.parse_args()

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

    # Start the servers
    for i in range(args.numServers):
        serv = server.Server(i,
                             resourceCapacity=args.serverConcurrency,
                             serviceTime=((i+1) * args.serviceTime),
                             serviceTimeModel=args.serviceTimeModel)
        servers.append(serv)

    # Start the clients
    for i in range(args.numClients):
        c = client.Client(id_="Client%s" % (i),
                          serverList=servers,
                          replicaSelectionStrategy=args.selectionStrategy,
                          accessPattern=args.accessPattern,
                          replicationFactor=args.replicationFactor,
                          backpressure=args.backpressure,
                          shadowReadRatio=args.shadowReadRatio)
        clients.append(c)

    # Start workload generators (analogous to YCSB)
    latencyMonitor = Simulation.Monitor(name="Latency")

    for i in range(args.numWorkload):
        w = workload.Workload(i, latencyMonitor)
        Simulation.activate(w, w.run(clients,
                                     "constant",
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

    for clientNode in clients:
        printMonitorTimeSeriesToFile(pendingRequestsFD,
                                     clientNode.id,
                                     clientNode.pendingRequestsMonitor)
        printMonitorTimeSeriesToFile(latencyTrackerFD,
                                     clientNode.id,
                                     clientNode.latencyTrackerMonitor)
    for serv in servers:
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

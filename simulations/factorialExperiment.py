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


def printMonitorTimeSeriesToFile(fileDesc, prefix, monitor):
    for entry in monitor:
        fileDesc.write("%s %s %s\n" % (prefix, entry[0], entry[1]))


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

    assert args.expScenario != ""

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
    elif(args.expScenario == "expovariateServiceTimeServers"):
        # Start the servers
        for i in range(args.numServers):
            st = random.expovariate(1/float(args.serviceTime))
            serv = server.Server(i,
                                 resourceCapacity=args.serverConcurrency,
                                 serviceTime=st,
                                 serviceTimeModel=args.serviceTimeModel)
            servers.append(serv)
    elif(args.expScenario == "timeVaryingServiceTimeServers"):
        # Start the servers
        for i in range(args.numServers):
            serv = server.Server(i,
                                 resourceCapacity=args.serverConcurrency,
                                 serviceTime=(args.serviceTime),
                                 serviceTimeModel=args.serviceTimeModel)
            mup = muUpdater.MuUpdater(serv, args.serviceTime,
                                      args.intervalParam,
                                      args.rangeParam)
            Simulation.activate(mup, mup.run(), at=0.0)
            servers.append(serv)
    else:
        print "Unknown experiment scenario"
        sys.exit(-1)

    # Start the clients
    for i in range(args.numClients):
        demandWeight = 1.0
        assert args.demandSkew >= 0
        if(args.demandSkew > 0.0):
            demandWeight += random.expovariate(1/float(args.demandSkew))

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
                          demandWeight=demandWeight)
        clients.append(c)

    # Start workload generators (analogous to YCSB)
    latencyMonitor = Simulation.Monitor(name="Latency")

    for i in range(args.numWorkload):
        w = workload.Workload(i, latencyMonitor,
                              clients,
                              args.workloadModel,
                              args.workloadParam,
                              args.numRequests/args.numWorkload)
        Simulation.activate(w, w.run(),
                            at=0.0),
        workloadGens.append(w)

    # Begin simulation
    Simulation.simulate(until=args.simulationDuration)

    #
    # Print a bunch of timeseries
    #
    # pendingRequestsFD = open("../%s/%s_PendingRequests" %
    #                          (args.logFolder,
    #                           args.expPrefix), 'w')
    # waitMonFD = open("../%s/%s_WaitMon" % (args.logFolder,
    #                                        args.expPrefix), 'w')
    # actMonFD = open("../%s/%s_ActMon" % (args.logFolder,
    #                                      args.expPrefix), 'w')
    latencyFD = open("../%s/%s_Latency" % (args.logFolder,
                                           args.expPrefix), 'w')
    # latencyTrackerFD = open("../%s/%s_LatencyTracker" %
    #                         (args.logFolder, args.expPrefix), 'w')
    # rateFD = open("../%s/%s_Rate" % (args.logFolder,
    #                                  args.expPrefix), 'w')
    # tokenFD = open("../%s/%s_Tokens" % (args.logFolder,
    #                                     args.expPrefix), 'w')
    # receiveRateFD = open("../%s/%s_ReceiveRate" % (args.logFolder,
    #                                                args.expPrefix), 'w')

    # for clientNode in clients:
    #     printMonitorTimeSeriesToFile(pendingRequestsFD,
    #                                  clientNode.id,
    #                                  clientNode.pendingRequestsMonitor)
    #     printMonitorTimeSeriesToFile(latencyTrackerFD,
    #                                  clientNode.id,
    #                                  clientNode.latencyTrackerMonitor)
    #     printMonitorTimeSeriesToFile(rateFD,
    #                                  clientNode.id,
    #                                  clientNode.rateMonitor)
    #     printMonitorTimeSeriesToFile(tokenFD,
    #                                  clientNode.id,
    #                                  clientNode.tokenMonitor)
    #     printMonitorTimeSeriesToFile(receiveRateFD,
    #                                  clientNode.id,
    #                                  clientNode.receiveRateMonitor)
    # for serv in servers:
    #     printMonitorTimeSeriesToFile(waitMonFD,
    #                                  serv.id,
    #                                  serv.queueResource.waitMon)
    #     printMonitorTimeSeriesToFile(actMonFD,
    #                                  serv.id,
    #                                  serv.queueResource.actMon)
    #     print "------- Server:%s %s ------" % (serv.id, "WaitMon")
    #     print "Mean:", serv.queueResource.waitMon.mean()

    #     print "------- Server:%s %s ------" % (serv.id, "ActMon")
    #     print "Mean:", serv.queueResource.actMon.mean()

    # print "------- Latency ------"
    # print "Mean Latency:",\
    #   sum([float(entry[1].split()[0]) for entry in latencyMonitor])/float(len(latencyMonitor))

    printMonitorTimeSeriesToFile(latencyFD, "0",
                                 latencyMonitor)
    assert args.numRequests == len(latencyMonitor)


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
    parser.add_argument('--logFolder', nargs='?',
                        type=str, default="logs")
    parser.add_argument('--expScenario', nargs='?',
                        type=str, default="")
    parser.add_argument('--demandSkew', nargs='?',
                        type=int, default=0)
    parser.add_argument('--intervalParam', nargs='?',
                        type=float, default=0.0)
    parser.add_argument('--rangeParam', nargs='?',
                        type=float, default=0.0)
    args = parser.parse_args()

    runExperiment(args)

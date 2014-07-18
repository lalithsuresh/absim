import SimPy.Simulation as Simulation
import server
import client
import workload
import argparse


def printMonitorTimeSeriesToFile(filename, prefix, monitor):
    outputFile = open(filename, 'w')
    for entry in monitor:
        outputFile.write("%s %s %s\n" % (prefix, entry[0], entry[1]))

    outputFile.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Absinthe sim.')
    parser.add_argument('--numClients', nargs='?',
                        type=int, default=1)
    parser.add_argument('--numServers', nargs='?',
                        type=int, default=1)
    parser.add_argument('--numWorkload', nargs='?',
                        type=int, default=1)
    parser.add_argument('--numClients', nargs='?',
                        type=int, default=1)
    parser.add_argument('--serverQueueCapacity', nargs='?',
                        type=int, default=1)
    parser.add_argument('--serviceTime', nargs='?',
                        type=int, default=1)
    parser.add_argument('--replicationFactor', nargs='?',
                        type=int, default=1)
    parser.add_argument('--selectionStrategy', nargs='?',
                        type=str, default="pending")
    parser.add_argument('--accessPattern', nargs='?',
                        type=str, default="uniform")
    parser.add_argument('--nwLatencyBase', nargs='?',
                        type=float, default=0.960)
    parser.add_argument('--nwLatencyMu', nargs='?',
                        type=float, default=0.040)
    parser.add_argument('--nwLatencySigma', nargs='?',
                        type=float, default=0.0)
    args = parser.parse_args()

    Simulation.initialize()

    servers = []
    clients = []
    workloadGens = []
    NW_LATENCY_BASE = args.nwLatencyBase
    NW_LATENCY_MU = args.nwLatencyMu
    NW_LATENCY_SIGMA = args.nwLatencySigma

    # Start the servers
    for i in range(args.numServers):
        serv = server.Server(i,
                             resourceCapacity=args.serverQueueCapacity,
                             serviceTime=args.serviceTime)
        servers.append(serv)

    # Start the clients
    for i in range(args.numClients):
        client = client.Client(id_="Client1",
                               serverList=servers,
                               replicaSelectionStrategy=args.selectionStrategy,
                               accessPattern=args.accessPattern,
                               replicationFactor=args.replicationFactor)
        clients.append(client)

    # Start workload generators (analogous to YCSB)
    latencyMonitor = Simulation.Monitor(name="Latency")

    for i in range(args.numWorkload):
        w = workload.Workload(i, latencyMonitor)
        Simulation.activate(w, w.run([client],
                                     "poisson",
                                     None), at=0.0)
        workloadGens.append(w)

    # Begin simulation
    Simulation.simulate(until=100)

    #
    # Print a bunch of timeseries
    #
    for client in clients:
        printMonitorTimeSeriesToFile("logs/PendingRequests", client.id,
                                     client.pendingRequestsMonitor)
    for serv in servers:
        printMonitorTimeSeriesToFile("logs/WaitMon", serv.id,
                                     serv.queueResource.waitMon)
        printMonitorTimeSeriesToFile("logs/ActMon", serv.id,
                                     serv.queueResource.actMon)
        print "------- Server:%s %s ------" % (serv.id, "WaitMon")
        print "Mean:", serv.queueResource.waitMon.mean()

        print "------- Server:%s %s ------" % (serv.id, "ActMon")
        print "Mean:", serv.queueResource.actMon.mean()

    print "------- Latency ------"
    print "Mean Latency:", latencyMonitor.mean()
    printMonitorTimeSeriesToFile("logs/Latency", "0",
                                 latencyMonitor)

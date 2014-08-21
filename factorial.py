import itertools
import subprocess
import os
import sys


numClients = [10]
numServers = [3]
numWorkload = [1]
workloadModel = ["poisson"]
serverConcurrency = [1]
serviceTime = [4]
workloadParam = [2.8818181818]
serviceTimeModel = ["random.expovariate"]
replicationFactor = [3]
rateInterval = [10, 20, 30, 40]
cubicC = [0.0004, 0.000004, 0.0000004]
cubicSmax = [1, 10, 20]
cubicBeta = [0.2, 0.5, 0.8]
hysterisisFactor = [1, 2, 4]
shadowReadRatio = [0.0, 0.1]
accessPattern = ["uniform", "zipfian"]
nwLatencyBase = [0.0, 2.0]
nwLatencyMu = [0]
nwLatencySigma = [0]
simulationDuration = [100000]
seed = range(1, 5)
numRequests = [10000]

LIST = [numClients,
        numServers,
        numWorkload,
        workloadModel,
        serverConcurrency,
        serviceTime,
        workloadParam,
        serviceTimeModel,
        replicationFactor,
        rateInterval,
        cubicC,
        cubicSmax,
        cubicBeta,
        hysterisisFactor,
        shadowReadRatio,
        accessPattern,
        nwLatencyBase,
        nwLatencyMu,
        nwLatencySigma,
        simulationDuration,
        seed,
        numRequests
        ]
PARAM_COMBINATIONS = list(itertools.product(*LIST))

basePath = os.getcwd()

for combination in PARAM_COMBINATIONS:
        numClients, numServers, numWorkload, \
            workloadModel, serverConcurrency, \
            serviceTime, workloadParam, \
            serviceTimeModel, replicationFactor, \
            rateInterval, cubicC, cubicSmax, \
            cubicBeta, hysterisisFactor, shadowReadRatio, \
            accessPattern, nwLatencyBase, \
            nwLatencyMu, nwLatencySigma, \
            simulationDuration, seed, \
            numRequests = combination

        os.chdir(basePath + "/simulations")
        cmd = "python experiment.py  \
                --numClients %s\
                --numServers %s\
                --numWorkload %s\
                --workloadModel %s\
                --serverConcurrency %s\
                --serviceTime %s\
                --workloadParam %s\
                --serviceTimeModel %s\
                --replicationFactor %s\
                --selectionStrategy pendingXserviceTimeOracle\
                --shadowReadRatio %s\
                --accessPattern %s\
                --nwLatencyBase %s\
                --nwLatencyMu %s\
                --nwLatencySigma %s\
                --expPrefix oracle\
                --simulationDuration %s\
                --seed %s\
                --numRequests %s\
                --logFolder factorialLogs"\
                    % (numClients,
                       numServers,
                       numWorkload,
                       workloadModel,
                       serverConcurrency,
                       serviceTime,
                       workloadParam,
                       serviceTimeModel,
                       replicationFactor,
                       shadowReadRatio,
                       accessPattern,
                       nwLatencyBase,
                       nwLatencyMu,
                       nwLatencySigma,
                       simulationDuration,
                       seed,
                       numRequests)
        proc = subprocess.Popen(cmd.split(),
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        result = proc.stdout.readlines()

        os.chdir(basePath + "/plotting")
        cmd = "Rscript factorialResults.r oracle"
        proc = subprocess.Popen(cmd.split(),
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()
        for line in out.split("\n"):
            if ("oracle" in line):
                parts = line.split()
                for i in range(len(parts)):
                    parts[i] = parts[i][1:-1]
                print ' '.join(map(lambda x: str(x), combination))\
                    + " " + ' '.join(parts)
                sys.stdout.flush()

        os.chdir(basePath + "/simulations")
        cmd = "python experiment.py  \
                --numClients %s\
                --numServers %s\
                --numWorkload %s\
                --workloadModel %s\
                --serverConcurrency %s\
                --serviceTime %s\
                --workloadParam %s\
                --serviceTimeModel %s\
                --replicationFactor %s\
                --selectionStrategy expDelay \
                --backpressure\
                --shadowReadRatio %s\
                --rateInterval %s\
                --cubicC %s\
                --cubicSmax %s\
                --cubicBeta %s\
                --hysterisisFactor %s\
                --accessPattern %s\
                --nwLatencyBase %s\
                --nwLatencyMu %s\
                --nwLatencySigma %s\
                --expPrefix expDelay\
                --simulationDuration %s\
                --seed %s\
                --numRequests %s\
                --logFolder factorialLogs"\
                  % (numClients,
                     numServers,
                     numWorkload,
                     workloadModel,
                     serverConcurrency,
                     serviceTime,
                     workloadParam,
                     serviceTimeModel,
                     replicationFactor,
                     shadowReadRatio,
                     rateInterval,
                     cubicC,
                     cubicSmax,
                     cubicBeta,
                     hysterisisFactor,
                     accessPattern,
                     nwLatencyBase,
                     nwLatencyMu,
                     nwLatencySigma,
                     simulationDuration,
                     seed,
                     numRequests)
        proc = subprocess.Popen(cmd.split(),
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        result = proc.stdout.readlines()

        os.chdir(basePath + "/plotting")
        cmd = "Rscript factorialResults.r expDelay"
        proc = subprocess.Popen(cmd.split(),
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()
        for line in out.split("\n"):
            if ("expDelay" in line):
                parts = line.split()
                for i in range(len(parts)):
                    parts[i] = parts[i][1:-1]
                print ' '.join(map(lambda x: str(x), combination))\
                    + " " + ' '.join(parts)
                sys.stdout.flush()
        os.chdir(basePath)

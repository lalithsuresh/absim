import itertools
import subprocess
import os
import sys

uniqId = sys.argv[1]

numClients = [5, 10, 20, 30]
numServers = [10]
numWorkload = [1]
workloadModel = ["poisson"]
serverConcurrency = [1]
serviceTime = [4]
workloadParam = [0.9818181818]
serviceTimeModel = ["random.expovariate"]
replicationFactor = [3]
rateInterval = [20]
cubicC = [0.000004]
cubicSmax = [10]
cubicBeta = [0.2]
hysterisisFactor = [2]
shadowReadRatio = [0.0]
accessPattern = ["uniform", "zipfian"]
nwLatencyBase = [0.0, 2.0]
nwLatencyMu = [0]
nwLatencySigma = [0]
simulationDuration = [100000]
seed = [int(uniqId)]
numRequests = [10000]
expScenario = ["expovariateServiceTimeServers"]
demandSkew = [0, 50, 100]
intervalParam = [10]
rangeParam = [20]


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
        numRequests,
        expScenario,
        demandSkew,
        intervalParam,
        rangeParam,
        ]
PARAM_COMBINATIONS = list(itertools.product(*LIST))

# print len(PARAM_COMBINATIONS)

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
            numRequests, expScenario, \
            demandSkew, intervalParam, \
            rangeParam, = combination

        logFolder = "timeVaryingSweep" + uniqId
        if not os.path.exists(logFolder):
                os.makedirs(logFolder)

        os.chdir(basePath + "/simulations")
        cmd = "python factorialExperiment.py  \
                --numClients %s\
                --numServers %s\
                --numWorkload %s\
                --workloadModel %s\
                --serverConcurrency %s\
                --serviceTime %s\
                --workloadParam %s\
                --serviceTimeModel %s\
                --replicationFactor %s\
                --selectionStrategy pending\
                --shadowReadRatio %s\
                --accessPattern %s\
                --nwLatencyBase %s\
                --nwLatencyMu %s\
                --nwLatencySigma %s\
                --expPrefix pending\
                --simulationDuration %s\
                --seed %s\
                --numRequests %s\
                --expScenario %s\
                --demandSkew %s\
                --intervalParam %s\
                --rangeParam %s\
                --logFolder %s"\
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
                       numRequests,
                       expScenario,
                       demandSkew,
                       intervalParam,
                       rangeParam,
                       logFolder)
        proc = subprocess.Popen(cmd.split(),
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()

        if (proc.returncode != 0):
            print ' '.join(map(lambda x: str(x), combination)) + " ERROR"
        else:
            os.chdir(basePath + "/plotting")
            cmd = "Rscript factorialResults.r pending %s" % (logFolder)
            proc = subprocess.Popen(cmd.split(),
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            out, err = proc.communicate()
            for line in out.split("\n"):
                if ("pending" in line):
                    parts = line.split()
                    for i in range(len(parts)):
                        parts[i] = parts[i][1:-1]
                    print ' '.join(map(lambda x: str(x), combination))\
                        + " " + ' '.join(parts)
                    sys.stdout.flush()

        if not os.path.exists(logFolder):
            os.makedirs(logFolder)
        os.chdir(basePath + "/simulations")

        cmd = "python factorialExperiment.py \
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
                --expScenario %s\
                --demandSkew %s\
                --intervalParam %s\
                --rangeParam %s\
                --logFolder %s"\
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
                     numRequests,
                     expScenario,
                     demandSkew,
                     intervalParam,
                     rangeParam,
                     logFolder)
        proc = subprocess.Popen(cmd.split(),
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()

        if (proc.returncode != 0):
            print ' '.join(map(lambda x: str(x), combination)) + " ERROR"
        else:
            os.chdir(basePath + "/plotting")
            cmd = "Rscript factorialResults.r expDelay %s" % (logFolder)
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

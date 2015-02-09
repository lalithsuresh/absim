import itertools
import subprocess
import os
import sys

uniqId = sys.argv[1]

numClients = [150, 300]
numServers = [50]
numWorkload = [200]
workloadModel = ["poisson"]
serverConcurrency = [4]
serviceTime = [4]
# utilization = [0.4, 0.45]
utilization = [0.99]
serviceTimeModel = ["random.expovariate"]
replicationFactor = [3]
selectionStrategy = ["clairvoyant"]
rateInterval = [20]
cubicC = [0.000004]
cubicSmax = [10]
cubicBeta = [0.2]
hysterisisFactor = [2]
shadowReadRatio = [0.1]
accessPattern = ["uniform"]
nwLatencyBase = [2.0]
nwLatencyMu = [0]
nwLatencySigma = [0]
simulationDuration = [600000]
seed = [int(uniqId)]
numRequests = [600000]
# expScenario = ["heterogenousStaticServiceTimeScenario"]
expScenario = ["timeVaryingServiceTimeServers"]
demandSkew = [0.0]
highDemandFraction = [0.0]
# slowServerFraction = [0.3, 0.5, 0.7]
# slowServerSlowness = [0.8, 0.5, 0.3]
# intervalParam = [0]
# timeVaryingDrift = [0]
slowServerFraction = [0]
slowServerSlowness = [0]
intervalParam = [10, 50, 100, 200, 300, 500]
timeVaryingDrift = [1, 3, 5]


logFolder = "new-D-ora" + uniqId
# logFolder = "paperSkewSweep" + uniqId

if not os.path.exists(logFolder):
        os.makedirs(logFolder)

LIST = [numClients,
        numServers,
        numWorkload,
        workloadModel,
        serverConcurrency,
        serviceTime,
        utilization,
        serviceTimeModel,
        replicationFactor,
        selectionStrategy,
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
        highDemandFraction,
        slowServerFraction,
        slowServerSlowness,
        intervalParam,
        timeVaryingDrift,
        ]
PARAM_COMBINATIONS = list(itertools.product(*LIST))

print len(PARAM_COMBINATIONS)

basePath = os.getcwd()

for combination in PARAM_COMBINATIONS:
        numClients, numServers, numWorkload, \
            workloadModel, serverConcurrency, \
            serviceTime, utilization, \
            serviceTimeModel, replicationFactor, selectionStrategy, \
            rateInterval, cubicC, cubicSmax, \
            cubicBeta, hysterisisFactor, shadowReadRatio, \
            accessPattern, nwLatencyBase, \
            nwLatencyMu, nwLatencySigma, \
            simulationDuration, seed, \
            numRequests, expScenario, \
            demandSkew, highDemandFraction, \
            slowServerFraction, \
            slowServerSlowness, intervalParam, \
            timeVaryingDrift, = combination

        os.chdir(basePath + "/simulations")
        backpressure = ""

        if (selectionStrategy == "expDelay"
           or selectionStrategy == "round_robin"
           or selectionStrategy == "random"):
            backpressure = "--backpressure"

        cmd = "python factorialExperiment.py \
                --numClients %s\
                --numServers %s\
                --numWorkload %s\
                --workloadModel %s\
                --serverConcurrency %s\
                --serviceTime %s\
                --utilization %s\
                --serviceTimeModel %s\
                --replicationFactor %s\
                --selectionStrategy %s \
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
                --expPrefix %s\
                --simulationDuration %s\
                --seed %s\
                --numRequests %s\
                --expScenario %s\
                --demandSkew %s\
                --highDemandFraction %s\
                --slowServerFraction %s\
                --slowServerSlowness %s\
                --intervalParam %s\
                --timeVaryingDrift %s\
                --logFolder %s %s"\
                  % (numClients,
                     numServers,
                     numWorkload,
                     workloadModel,
                     serverConcurrency,
                     serviceTime,
                     utilization,
                     serviceTimeModel,
                     replicationFactor,
                     selectionStrategy,
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
                     selectionStrategy,
                     simulationDuration,
                     seed,
                     numRequests,
                     expScenario,
                     demandSkew,
                     highDemandFraction,
                     slowServerFraction,
                     slowServerSlowness,
                     intervalParam,
                     timeVaryingDrift,
                     logFolder,
                     backpressure)
        proc = subprocess.Popen(cmd.split(),
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()

        if (proc.returncode != 0):
            print ' '.join(map(lambda x: str(x), combination)) + " ERROR"
        else:
            os.chdir(basePath + "/plotting")
            cmd = "Rscript factorialResults.r %s %s"\
                % (selectionStrategy, logFolder)
            proc = subprocess.Popen(cmd.split(),
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
            out, err = proc.communicate()
            for line in out.split("\n"):
                if (selectionStrategy in line):
                    parts = line.split()
                    for i in range(len(parts)):
                        parts[i] = parts[i][1:-1]
                    print ' '.join(map(lambda x: str(x), combination))\
                        + " " + ' '.join(parts)
                    sys.stdout.flush()
        os.chdir(basePath)

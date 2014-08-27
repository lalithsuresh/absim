import itertools
import subprocess
import os
import sys


numClients = [20, 30]
numServers = [10]
numWorkload = [1]
workloadModel = ["poisson"]
serverConcurrency = [1]
serviceTime = [4]
workloadParam = [1.6818181818]
serviceTimeModel = ["random.expovariate"]
replicationFactor = [3]
rateInterval = [20]
cubicC = [0.000004, 0.00004, 0.0000004]
cubicSmax = [5, 10, 20]
cubicBeta = [0.2, 0.5]
hysterisisFactor = [2, 4]
shadowReadRatio = [0.0]
accessPattern = ["uniform", "zipfian"]
nwLatencyBase = [2.0]
nwLatencyMu = [0]
nwLatencySigma = [0]
simulationDuration = [1000000]
seed = range(1, 5)
numRequests = [20000]
expScenario = ["expovariateServiceTimeServers", "timeVaryingServiceTimeServers"]
demandSkew = [0, 50, 100]
intervalParam = [10, 40, 80]
rangeParam = [10, 20]

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

        # os.chdir(basePath + "/simulations")
        # cmd = "python factorialExperiment.py  \
        #         --numClients %s\
        #         --numServers %s\
        #         --numWorkload %s\
        #         --workloadModel %s\
        #         --serverConcurrency %s\
        #         --serviceTime %s\
        #         --workloadParam %s\
        #         --serviceTimeModel %s\
        #         --replicationFactor %s\
        #         --selectionStrategy pending\
        #         --shadowReadRatio %s\
        #         --accessPattern %s\
        #         --nwLatencyBase %s\
        #         --nwLatencyMu %s\
        #         --nwLatencySigma %s\
        #         --expPrefix pending\
        #         --simulationDuration %s\
        #         --seed %s\
        #         --numRequests %s\
        #         --logFolder cubicLogs\
        #         --expScenario %s\
        #         --demandSkew %s\
        #         --intervalParam %s\
        #         --rangeParam %s"\
        #             % (numClients,
        #                numServers,
        #                numWorkload,
        #                workloadModel,
        #                serverConcurrency,
        #                serviceTime,
        #                workloadParam,
        #                serviceTimeModel,
        #                replicationFactor,
        #                shadowReadRatio,
        #                accessPattern,
        #                nwLatencyBase,
        #                nwLatencyMu,
        #                nwLatencySigma,
        #                simulationDuration,
        #                seed,
        #                numRequests,
        #                expScenario,
        #                demandSkew,
        #                intervalParam,
        #                rangeParam)
        # proc = subprocess.Popen(cmd.split(),
        #                         stdin=subprocess.PIPE,
        #                         stdout=subprocess.PIPE,
        #                         stderr=subprocess.PIPE)
        # out, err = proc.communicate()

        # if (proc.returncode != 0):
        #     print ' '.join(map(lambda x: str(x), combination)) + " ERROR"
        # else:
        #     os.chdir(basePath + "/plotting")
        #     cmd = "Rscript factorialResults.r pending"
        #     proc = subprocess.Popen(cmd.split(),
        #                             stdin=subprocess.PIPE,
        #                             stdout=subprocess.PIPE,
        #                             stderr=subprocess.PIPE)
        #     out, err = proc.communicate()
        #     for line in out.split("\n"):
        #         if ("pending" in line):
        #             parts = line.split()
        #             for i in range(len(parts)):
        #                 parts[i] = parts[i][1:-1]
        #             print ' '.join(map(lambda x: str(x), combination))\
        #                 + " " + ' '.join(parts)
        #             sys.stdout.flush()

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
                --logFolder cubicLogs\
                --expScenario %s\
                --demandSkew %s\
                --intervalParam %s\
                --rangeParam %s"\
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
                     rangeParam)
        proc = subprocess.Popen(cmd.split(),
                                stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        out, err = proc.communicate()

        if (proc.returncode != 0):
            print ' '.join(map(lambda x: str(x), combination)) + " ERROR"
        else:
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

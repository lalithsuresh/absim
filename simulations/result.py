

statDict = {}
filePath = "/Users/jingli/project/absim/simulations/result.txt"
outputFile = "/Users/jingli/project/absim/simulations/result_processed.txt"
with open(filePath) as f:
    for line in f:
        fields = line.split()
        if len(fields) < 2:
            continue
        id = int(fields[0])
        time = int(fields[1])
        if id in statDict:
            if statDict[id] > time:
                statDict[id] = time
        else:
            statDict[id] = time
for id in statDict:
    print statDict[id]
with open(filePath) as f:
    for line in f:
        fields = line.split("   ")
        if len(fields) < 2:
            continue
        id = int(fields[0])
        time = int(fields[1])
        if id in statDict:
            if statDict[id] > time:
                statDict[id] = time
        else:
            statDict[id] = time
for id in statDict:
    print statDict[id]

with open(outputFile, "w") as outFile:
    for id in statDict:
        outFile.write(str(statDict[id]) + "\n")
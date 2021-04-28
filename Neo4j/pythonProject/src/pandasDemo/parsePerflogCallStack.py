import os
import pandas as pd
import json

def parsePerfLogCallstack(rootEntry):
    records = []
    level = 1
    print("start to parse perflog call stack ", rootEntry)
    if rootEntry["n"] and rootEntry["i"] and rootEntry["t"]:
        record = {}
        record["name"] = rootEntry["n"]
        record["i"] = rootEntry["i"]
        record["t"] = rootEntry["t"]
        record["totalTime"] = rootEntry["t"]
        records.append(record)

        unknownTime = rootEntry["t"];
        if rootEntry["sub"] :
            nextLevel = level + 1
            for subEntry in rootEntry["sub"]:
                parseEntry(records, subEntry, rootEntry["t"], nextLevel);
                unknownTime = unknownTime - subEntry["t"]

            if unknownTime < rootEntry["t"] and unknownTime > 0:
                otherrecord = {}
                otherrecord["name"] = "others"
                otherrecord["i"] = ''
                otherrecord["t"] = unknownTime
                otherrecord["totalTime"] = rootEntry["t"]
                records.append(otherrecord)

    return records

def parseEntry(records, entry, totalTime, level):
    print("start to parseEntry")
    if entry["n"] and entry["i"] and entry["t"]:
        record = {}
        record["name"] = entry["n"]
        record["i"] = entry["i"]
        record["t"] = entry["t"]
        record["totalTime"] = totalTime
        record["level"] = level
        records.append(record)

    unknownTime = entry["t"]

    if (entry.sub) {
    var nextLevel = level+1;

    for (let subEntry of entry.sub) {
    parseEntry(records, subEntry, totalTime, nextLevel);
    unknownTime -= subEntry.t;
    }

    if (unknownTime < entry.t & & unknownTime > 0){
    records.push({
    name: 'Others',
          invocations: '',
    time: unknownTime,
    totalTime,
    level: nextLevel
    });
    }
    }
    











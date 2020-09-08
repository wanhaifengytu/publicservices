import os


fileName = 'gcverbose_to1400_36gFull.log'
startTime = '[2019-04-08T04:32:00'
endTime = '[2019-04-08T05:02:01'
outputFile = 'output1.log'

fileFullLog = open('./' + fileName, 'r')

line = fileFullLog.readline()
cnt = 1

startWriting = False
endWriting = False
fileOut = open('./' + outputFile, 'a')
	
while line:
	line = fileFullLog.readline()
	
	if line.startswith(startTime):
		startWriting = True
	
	if line.startswith(endTime):
		endWriting = True
	
	if startWriting and not endWriting:
		fileOut.write(line)
	cnt += 1
	
	
print cnt
fileFullLog.close
fileOut.close

originalFile = 'perf.log'
extracedFile = originalFile + '.extracted'

subStr = 'SN=PERFVERIHANA_SiemensPEN'

outputfile = open(extracedFile, "w")

with open('perf.log') as bigfile:
	for lineno, line in enumerate(bigfile):
		if line.find(subStr) == -1:
			continue
		else:
			outputfile.write(line)

import math

def variance(s):
	avg = average(s)
	return map(lambda x: (x - avg)**2, s)

def average(s):
	return sum(s) * 1.0 / len(s)

def std_dev(s):
	return math.sqrt(average(variance(s)))

data = [-0.005223039,
-0.012741452,
-0.015952931,
-0.021770192,
-0.061792106,
-0.071028556,
0.032825677,
-0.023547377,
0.001075632,
0.093016528,
-0.03545262,
-0.035850267,
-0.012778232,
-0.004893386,
-0.067624287,
-0.003041868,
0.044147299,
-0.016603883,
-0.026261502,
-0.043474436,
-0.095056546,
-0.00828041,
0.015549837,
0.018722566,
0.060610566,
0.023037435,
-0.008608137,
0.003153879,
0.043458077,
0.037606123,
0.027087764]

print "Variance ", variance(data)
print "Average ", average(data)
print "Average Variance ", average(variance(data))
print "Standard Deviation ", std_dev(data)
import urllib2
import gzip

import os

def fetch_bitcoinchart_data(filename):
	# fetch bitstamp data from the url
	#filename = 'bitstampUSD.csv.gz'
	url = "http://api.bitcoincharts.com/v1/csv/{0}.csv.gz".format(filename)
	resp = urllib2.urlopen(url)
	data = resp.read()
	resp.close()

	# write file to disk
	gzf = '{0}.csv.gz'.format(filename)
	open(gzf, 'wb').write(data)

	# Read gzip file
	f = gzip.open(gzf, 'rb')
	data = f.read()
	cf = '{0}.csv'.format(filename)

	open(cf, 'w').write(data)

	# print data
	# for line in f.read().split('\n'):
	# 	print line

if __name__ == '__main__':
	#print 'wooo'
	bitstamp_path = 'bitstampUSD'#.csv'
	fetch_bitcoinchart_data(bitstamp_path)
	
	# bitstamp_path = 'bitstampUSD.csv'
	# f = open(bitstamp_path, 'r')
	# btcpx = f.readlines()
	# f.close()
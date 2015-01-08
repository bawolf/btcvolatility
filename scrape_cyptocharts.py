#!/usr/bin/env python

from bhscripting.BhTaskMaster import *
from bhscripting.BhIniReader import *
from bhscripting.BhTradeDateHandler import *
from bhscripting.BhFtp import *
from bhscripting.BhDbConnect import *

import os
import re
import urllib2
#from BeautifulSoup import BeautifulSoup
import time
import datetime

from bs4 import BeautifulSoup

master = BhTaskMaster(None,None,None)#Uncomment this line and comment previous to run in wondow and watch output

db = BhDbConnect()

binDir  = "\\\\bluefish\\d$\\ops_btc\\" #master.getIniValue('duxOpsUsBinBtc')
bitcoinDir = master.getIniValue('duxOpsUsBtc')

def remove_extra_spaces(data):
    p = re.compile(r'\s+')
    return p.sub(' ', data)

def remove_html_tags(data):
    p = re.compile(r'<.*?>')
    return remove_extra_spaces(str(p.sub('', data)).rstrip().lstrip()).replace('&nbsp;','').rstrip(',')

def open_cryptochart_page(period, resolution, pair, market):
    url = "http://www.cryptocoincharts.info/period-charts.php?period={0}&resolution={1}&pair={2}&market={3}".format(period, resolution, pair, market)
    return urllib2.urlopen(url)

def GetLastDate(symbol, market, resolution):
    query = 'select max(dt_tradedate) from bitcoin.dbo.src_cryptocharts where symbol = \'{0}\' and market = \'{1}\' and res_type = \'{2}\''.format(symbol, market, resolution)
    qresults = db.quickQueryToList('tuna', 'bitcoin', query)
    if(qresults!=None and qresults[0] !=None):
        lastDate = qresults[0][0]
        if(lastDate != 'None'):
            print('Found last date in database: {0} {1}'.format(symbol, str(lastDate)))
            sdt = time.strptime(str(lastDate), "%m/%d/%y %H:%M:%S")
            return datetime.datetime(*sdt[:6])
    print'Couldnt find last date...'
    return None

def uploadFile(inputFile):
    firstRow = 0
    targetTable = 'bitcoin.dbo.src_cryptocharts'
    delimiter = ';'
    sqlQuery = 'BULK INSERT ' + targetTable + ' from \'' + inputFile + "\' WITH (CHECK_CONSTRAINTS, DATAFILETYPE = 'CHAR', FIRSTROW =" + str(firstRow) + ", FIRE_TRIGGERS, FIELDTERMINATOR = '" + delimiter + "', MAXERRORS =0)"
    server = 'tuna'
    dBase = 'bitcoin'
    runStatement = "sqlcmd -S {0} -d {1} -E -Q \"{2}\" -l 60".format(server, dBase, sqlQuery)
    #print sqlQuery
    #print runStatement
    retcode = subprocess.call(runStatement, shell=True)
    
print'Collecting EOD Data...'
outputPath = "results.txt"
output=open(outputPath,"w")

securities = [
    ['ltc-btc', 'btc-e', 'day'],
    ['ltc-usd', 'btc-e', 'day'],
    ['btc-usd', 'btc-e', 'day'],
    ['btc-usd', 'mtgox', 'day'],
    ['btc-usd', 'bitstamp', 'day']
]

for sec in securities:
    pair = sec[0]
    market = sec[1]
    resolution = sec[2]
    period = 'alltime'
    
    # Get Last Date
    lastDate = GetLastDate(pair, market, resolution)
    
    # Fetch from website
    page= open_cryptochart_page(period, resolution, pair, market)
    soup=BeautifulSoup(page)

    count = 0
    # Find tables
    for table in soup.findAll("tbody"):
        for row in table.findAll("tr"):
            items = row.findAll("td")
            
            if(resolution=='hour'):
                dt = time.strptime(items[0].string[:-1], "%Y-%m-%d %H")
            else:
                dt = time.strptime(items[0].string, "%Y-%m-%d")
            dt = datetime.datetime(*dt[:6])
            
            if(lastDate != None and dt <= lastDate):
                continue
            
            data = ""
            data += str(dt) + ";"
            data += pair + ";"
            data += market + ";"
            data += resolution + ";"
            data += str(items[1].string)[:-3].rstrip().replace(',','') + ";"
            data += str(items[2].string)[:-3].rstrip().replace(',','') + ";"
            data += str(items[3].string)[:-3].rstrip().replace(',','') + ";"
            data += str(items[4].string)[:-3].rstrip().replace(',','') + ";"
            data += str(items[5].string).replace(',','')[:-3].rstrip() + "\n"
            
            output.write(data)
            count+=1
    print'Found {0} results for {1}'.format(count, pair)
output.close()    

uploadFile(binDir + outputPath)
import sqlite3
import csv
import sys
from datetime import datetime
from sys import argv
import math
from bhscripting.BhEmail import *

def create_tables():
	cursor.execute('''CREATE TABLE IF NOT EXISTS day_vol(datetime TEXT PRIMARY KEY, currency TEXT, log REAL)''')
	# cursor.execute('''CREATE TABLE IF NOT EXISTS hour_vol(datetime TEXT PRIMARY KEY, currency TEXT, log REAL)''')
	# cursor.execute('''CREATE TABLE IF NOT EXISTS minute_vol(datetime TEXT PRIMARY KEY, currency TEXT, log REAL)''')
	print "Tables created..."

def close_database():
	db.commit()
	print "Database Committed..."

	db.close()
	print "Database Closed"

def variance(s):
	avg = average(s)
	return map(lambda x: (x - avg)**2, s)

def average(s):
	return sum(s) * 1.0 / len(s)

def std_dev(s):
	return math.sqrt(average(variance(s)))

def add_data(cursor, database, value_range):
	body = "<br><br>Volatility as calculated by %s:<br><br>" % (database)
	
	time_ranges = {30:0,60:0,90:0,180:0,365:0}

	if database == "hour":
		cursor.execute('''SELECT log FROM hour_vol ORDER BY datetime DESC LIMIT ?''', (value_range,))
	elif database =="minute": 
		cursor.execute('''SELECT log FROM minute_vol ORDER BY datetime DESC LIMIT ?''', (value_range,))
	else:
		cursor.execute('''SELECT log FROM day_vol ORDER BY datetime DESC LIMIT ?''', (value_range,))


	raw_data = cursor.fetchall()
	rows = []

	for row in raw_data:
		#print row[0]
		rows.append(row[0])

	# print "Rows:"
	# print rows

	for key in sorted(time_ranges.iterkeys()):
		if rows:
			close_data = rows[0:key]#len(rows) - key*(value_range/365):len(rows) - 1]
			print "close data", close_data
			print "Std Dev", std_dev(close_data)
			print "Value Range", value_range
			print "Sqrt", math.sqrt(value_range)
			time_ranges[key] = std_dev(close_data)*math.sqrt(value_range)
			# print time_ranges[key]

	for item in sorted(time_ranges.iterkeys()):
		body += "%s day volatility: %s<br>" % (item, time_ranges[item])
	# print body
	return body
if __name__ == '__main__':
	
	# script, db_name = argv
	# db = sqlite3.connect(db_name)
	db = sqlite3.connect("volatility.db")
	cursor = db.cursor()

	#Should check that hour and minute vols are up to date, if they're not, it should run the other scripts to update


	#email settings
	subject = "Daily Volatility Numbers: %s" % (datetime.now().date())

	recipients = []#list of emails here
	
	create_tables()

	body = "Volatility by range:"
	body += add_data(cursor, "day", 365)
	# body += add_data(cursor, "hour", 365*24)
	# body += add_data(cursor, "minute", 365*24*60)
	print body
	sendFhcmReport(subject, recipients, body)
	print "Email sent."
	close_database()
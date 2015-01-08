import sqlite3
import csv
import sys
from datetime import datetime
from sys import argv
import math


'''
choose a minutebar and grab the close value, find the closest minutebar before this one with a close and grab it's number
calculate the natural log of close over close.

range = [vol for each day]
sqrt(range*365*24*60)
'''

def create_tables():
	cursor.execute('''CREATE TABLE IF NOT EXISTS minutebars(datetime TEXT PRIMARY KEY, currency TEXT,
		 high REAL, low REAL, open REAL, close REAL, volume REAL, price REAL)''')

	cursor.execute('''CREATE TABLE IF NOT EXISTS hourbars(datetime TEXT PRIMARY KEY, currency TEXT,
		 high REAL, low REAL, open REAL, close REAL, volume REAL, price REAL)''')
	
	cursor.execute('''CREATE TABLE IF NOT EXISTS voldata(datetime TEXT PRIMARY KEY, currency TEXT, log REAL)''')
	print "Tables created..."

def close_database():
	db.commit()
	print "Database Committed..."

	db.close()
	print "Database Closed"

def set_first():
	cursor.execute('''SELECT datetime FROM voldata ORDER BY datetime DESC LIMIT 1''')
	c = cursor.fetchone()
	if c is None:
		print "no data was found in voldata"
		cursor.execute('''SELECT datetime FROM minutebars ORDER BY datetime LIMIT 1''')
		c = cursor.fetchone()
		if c is None:
			print "There are no values in voldata or minutebars..."
			print "Exiting..."
			sys.exit()
	first = datetime.strptime(c[0], '%Y-%m-%d %H:%M:%S')
	return first

def set_first():
	#Check last date of minutebars
	#Make sure there are values in minutebars
	#If not start from the first value in transactions
	#if no values in either, Exit
	#first and last are now the range for your minute bars
	cursor.execute('''SELECT datetime FROM voldata ORDER BY datetime DESC LIMIT 1''')
	c = cursor.fetchone()
	if c is None:
		print "No data was found in voldata..."
		cursor.execute('''SELECT datetime FROM minutebars LIMIT 1''')
		c = cursor.fetchone()
		if c is None:
			print "There are no values in minutebars either..."
			print "Exiting..."
			sys.exit()
		else:
			print "Some data was found in minutebars..."
	first = datetime.strptime(c[0], '%Y-%m-%d %H:%M:%S')
	return first	

def get_last_close(cursor, date):
	cursor.execute('''SELECT close FROM minutebars 
	WHERE close NOT NULL AND datetime < ? ORDER BY datetime 
	DESC LIMIT 1''', [date])
	row = cursor.fetchone()
	if row:
		if row[0]:
			return row[0]
		else:
			return None;
	else:
		return None;

def update_voldata():
	# cursor.execute('''SELECT datetime FROM minutebars ORDER BY datetime DESC LIMIT 1''')
	# end = datetime.strptime(cursor.fetchone()[0], '%Y-%m-%d %H:%M:%S')

	num = []
	num.append(first)
	cursor.execute('''SELECT * FROM minutebars WHERE datetime >= ?''', [first])
	raw_data = cursor.fetchall()

	for row in raw_data:
		date = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
		currency = row[1]
		log = None
		last_close = get_last_close(cursor, date)

		if row[5] and last_close:
			close = row[5]
			print close
			log = math.log(close/last_close)
		cursor.execute('''INSERT or REPLACE INTO voldata(datetime, currency, log)
				VALUES(?,?,?)''', (date, currency, log))


if __name__ == '__main__':
	script, db_name = argv
	#Create DB and Insert
	db = sqlite3.connect(db_name)
	cursor = db.cursor()
	
	create_tables()

	first = set_first()
	update_voldata()
	close_database()

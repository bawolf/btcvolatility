import sqlite3
import csv
import sys
from datetime import datetime
from sys import argv
import math

def create_tables():
	cursor.execute('''CREATE TABLE IF NOT EXISTS hourbars(datetime TEXT PRIMARY KEY, currency TEXT,
		 high REAL, low REAL, open REAL, close REAL, volume REAL, price REAL)''')

	cursor.execute('''CREATE TABLE IF NOT EXISTS hour_vol(datetime TEXT PRIMARY KEY, currency TEXT, log REAL)''')
	print "Tables created..."

def close_database():
	db.commit()
	print "Database Committed..."

	db.close()
	print "Database Closed"

def set_first_hour(cursor):
	cursor.execute('''SELECT datetime FROM hour_vol ORDER BY datetime DESC LIMIT 1''')
	c = cursor.fetchone()
	if c is None:
		print "no data was found in hour_vol"
		cursor.execute('''SELECT datetime FROM hourbars ORDER BY datetime LIMIT 1''')
		c = cursor.fetchone()
		if c is None:
			print "There are no values in hour_vol or hourbars..."
			print "Exiting..."
			sys.exit()
	first = datetime.strptime(c[0], '%Y-%m-%d %H:%M:%S')
	return first

# def get_last_close(cursor, date):
# 	cursor.execute('''SELECT close FROM hourbars 
# 	WHERE close NOT NULL AND datetime < ? ORDER BY datetime 
# 	DESC LIMIT 1''', [date])
# 	row = cursor.fetchone()
# 	if row:
# 		if row[0]:
# 			return row[0]
# 		else:
# 			return None;
# 	else:
# 		return None;

def update_hour_vol(cursor, first):
	# cursor.execute('''SELECT datetime FROM hourbars ORDER BY datetime DESC LIMIT 1''')
	# end = datetime.strptime(cursor.fetchone()[0], '%Y-%m-%d %H:%M:%S')

	num = []
	num.append(first)
	cursor.execute('''SELECT datetime, currency, close, open FROM hourbars WHERE datetime >= ?''', [first])
	raw_data = cursor.fetchall()

	for row in raw_data:
		date = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
		currency = row[1]
		log = 0
		last_close = row[3]#get_last_close(cursor, date)

		if row[2] and last_close:
			close = row[2]
			log = math.log(close/last_close)
			#print log
		cursor.execute('''INSERT or REPLACE INTO hour_vol(datetime, currency, log)
				VALUES(?,?,?)''', (date, currency, log))


if __name__ == '__main__':
	# script, db_name = argv
	# #Create DB and Insert
	# db = sqlite3.connect(db_name)
	db = sqlite3.connect("volatility.db")
	cursor = db.cursor()
	
	create_tables()

	first = set_first_hour(cursor)
	update_hour_vol(cursor, first)
	close_database()

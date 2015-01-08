import sys
import sqlite3
import csv
from datetime import datetime
from datetime import timedelta
import time
import calendar
from sys import argv

def create_tables():
	cursor.execute('''CREATE TABLE IF NOT EXISTS transactions(id INTEGER PRIMARY KEY, currency TEXT,
		 datetime TEXT, price REAL, quantity REAL, unixdate INTEGER)''')

	#consider adding id of first and last in transactions for easy viewing
	cursor.execute('''CREATE TABLE IF NOT EXISTS minutebars(datetime TEXT PRIMARY KEY, currency TEXT,
		 high REAL, low REAL, open REAL, close REAL, volume REAL, price REAL)''')

	cursor.execute('''CREATE TABLE IF NOT EXISTS hourbars(datetime TEXT PRIMARY KEY, currency TEXT,
		 high REAL, low REAL, open REAL, close REAL, volume REAL, price REAL)''')

	cursor.execute('''CREATE TABLE IF NOT EXISTS daybars(datetime TEXT PRIMARY KEY, currency TEXT,
		 high REAL, low REAL, open REAL, close REAL, volume REAL, price REAL)''')
	
	print "Tables Opened..."

def close_database():
	db.commit()
	print "Database Committed..."

	db.close()
	print "Database Closed"

def set_first(cursor):
	#Check last date of minutebars
	#Make sure there are values in minutebars
	#If not start from the first value in transactions
	#if no values in either, Exit
	#first and last are now the range for your minute bars
	cursor.execute('''SELECT datetime FROM minutebars ORDER BY datetime DESC LIMIT 1''')
	c = cursor.fetchone()
	if c is None:
		print "No data was found in minutebars..."
		cursor.execute('''SELECT datetime FROM transactions LIMIT 1''')
		c = cursor.fetchone()
		if c is None:
			print "There are no values in minutebars or transactions..."
			print "Exiting..."
			sys.exit()
		else:
			print "Some data was found in transactions..."
	first = datetime.strptime(c[0], '%Y-%m-%d %H:%M:%S')
	first = first.replace( second=0, microsecond=0)
	return first

def set_last(first):
	last = first + timedelta(seconds = 59)
	return last

def set_first_hour(cursor):
	cursor.execute('''SELECT datetime
		FROM hourbars
		ORDER BY datetime
		DESC LIMIT 1''')
	c = cursor.fetchone()
	if c is None:
		print "No data was found in hourbars..."
		cursor.execute('''SELECT datetime 
			FROM minutebars
			ORDER BY datetime
			LIMIT 1''')
		c = cursor.fetchone()
		if c is None:
			print "There are no values in minutebars either..."
			print "Exiting..."
			sys.exit()
		else:
			print "Some data was found in minutebars..."
	first_hour = datetime.strptime(c[0], '%Y-%m-%d %H:%M:%S')
	first_hour = first_hour.replace( minute=0,second=0, microsecond=0)
	return first_hour

def set_last_hour(first_hour):
	last_hour = first_hour + timedelta(minutes = 59)
	return last_hour

def set_first_day(cursor):
	cursor.execute('''SELECT datetime
		FROM daybars
		ORDER BY datetime
		DESC LIMIT 1''')
	c = cursor.fetchone()
	if c is None:
		print "No data was found in daybars..."
		cursor.execute('''SELECT datetime 
			FROM hourbars
			ORDER BY datetime
			LIMIT 1''')
		c = cursor.fetchone()
		if c is None:
			print "There are no values in hourbars either..."
			print "Exiting..."
			sys.exit()
		else:
			print "Some data was found in hourbars..."
			# cursor.execute('''SELECT datetime
			# 	FROM minutebars
			# 	ORDER BY datetime
			# 	LIMIT 1''')
	first_day = datetime.strptime(c[0], '%Y-%m-%d %H:%M:%S')
	first_day = first_day.replace( hour=0, minute=0,second=0, microsecond=0)
	return first_day

def set_last_day(first_day):
	last_day = first_day + timedelta(hours = 23)
	return last_day

def set_open_price(cursor):
	open_price = None
	
	cursor.execute('''SELECT close FROM minutebars ORDER BY datetime DESC LIMIT 1''')
	r = cursor.fetchone()
	if r is not None:
		if r[0] is not None:
			open_price = r[0]
	return open_price

def set_open_price_hour(cursor):
	open_price = None
	
	cursor.execute('''SELECT close FROM hourbars 
		WHERE close NOT NULL ORDER BY datetime 
		DESC LIMIT 1''')
	r = cursor.fetchone()
	if r is not None:
		if r[0] is not None:
			open_price = r[0]
	return open_price

def set_close_price_hour(cursor, first_hour,last_hour):
	close_price = None

	cursor.execute('''SELECT close
		FROM minutebars
		Where close NOT NULL
		AND datetime
		BETWEEN ? AND ?
		''', (first_hour, last_hour))
	r = cursor.fetchall()
	if r:
		close_price = r[len(r)-1][0]
	return close_price

def set_open_price_day(cursor):
	open_price = None
	
	cursor.execute('''SELECT close FROM daybars 
		WHERE close NOT NULL ORDER BY datetime 
		DESC LIMIT 1''')
	r = cursor.fetchone()
	if r is not None:
		if r[0] is not None:
			open_price = r[0]
	return open_price

def set_close_price_day(cursor, first_day,last_day):
	close_price = None

	cursor.execute('''SELECT close
		FROM hourbars
		Where close NOT NULL
		AND datetime
		BETWEEN ? AND ?
		''', (first_day, last_day))
	r = cursor.fetchall()
	if r:
		close_price = r[len(r)-1][0]
	return close_price

	#if there is nothing in the minutebar table
	#check the transactions table and take the first time
def create_minutebar(cursor, raw_data, date, open_price):
	#open price is the close price of the LAST minute bar, or 0 if there is no previous bar
	#open_price = 0
	#close price is the price of the last transaction in the interval
	#currency, datetime, price, quantity

	close_price = raw_data[len(raw_data)-1][2]
	low = 0
	high = 0
	volume = 0
	total_spent = 0
	price = 0

	for row in raw_data:
		currency = row[0]
		price = row[2]
		quantity = row[3]

		volume += quantity
		total_spent += price*quantity
		if price < low or low == 0:
			low = price
		if price > high:
			high = price
	if volume > 0:
		price = total_spent/volume
	cursor.execute('''INSERT or REPLACE INTO minutebars(datetime, currency,  high, low, open,close,volume,price)
		VALUES(?,?,?,?,?,?,?,?)''', (date, currency, high,low,open_price,close_price,volume,price))

def create_empty_minutebar(cursor, date, open_price):
	volume = 0
	price = 0

	cursor.execute('''INSERT or REPLACE INTO minutebars(datetime, currency, open, volume,price)
		VALUES(?,?,?,?,?)''', (date, "BTC", open_price, volume, price))

# cursor.execute('''CREATE TABLE IF NOT EXISTS hourbars(datetime TEXT PRIMARY KEY, currency TEXT,
# 		 high REAL, low REAL, open REAL, close REAL, volume REAL, price REAL)''')

def create_hourbar(cursor, raw_data, date, open_price, close_price):
	high = raw_data[0][0]
	low = raw_data[0][1]
	volume = raw_data[0][2]
	price = raw_data[0][3]
	
	cursor.execute('''INSERT or REPLACE INTO hourbars(datetime, currency, high, low, open,close,volume,price)
		VALUES(?,?,?,?,?,?,?,?)''', (date, "BTC", high,low,open_price,close_price,volume,price))

def create_empty_hourbar(cursor, date, open_price):
	volume = 0
	price = 0

	cursor.execute('''INSERT or REPLACE INTO hourbars(datetime, currency, open, volume,price)
		VALUES(?,?,?,?, ?)''', (date, "BTC", open_price, volume, price))

def create_daybar(cursor, raw_data, date, open_price, close_price):
	high = raw_data[0][0]
	low = raw_data[0][1]
	volume = raw_data[0][2]
	price = raw_data[0][3]
	
	cursor.execute('''INSERT or REPLACE INTO daybars(datetime, currency, high, low, open,close,volume,price)
		VALUES(?,?,?,?,?,?,?,?)''', (date, "BTC", high,low,open_price,close_price,volume,price))

def create_empty_daybar(cursor, date, open_price):
	volume = 0
	price = 0

	cursor.execute('''INSERT or REPLACE INTO hourbars(datetime, currency, open, volume,price)
		VALUES(?,?,?,?, ?)''', (date, "BTC", open_price, volume, price))

def update_minutebars(cursor):
	
	first = set_first(cursor)
	last = set_last(first)

	cursor.execute('''SELECT datetime FROM transactions ORDER BY datetime DESC LIMIT 1''')
	end = datetime.strptime(cursor.fetchone()[0], '%Y-%m-%d %H:%M:%S')
	print end

	cursor.execute('''SELECT currency, datetime, price, quantity FROM transactions WHERE datetime >= ?''', [first])
	raw_data = cursor.fetchall()
	if not raw_data:
		print "Minute bars are up to date..."
		return
	i = 0
	print "adding transaction records..."
	while first <= end:
		minutebar_data = []
		open_price = set_open_price(cursor)
		# datetime.utcfromtimestamp(float(c[0]))
		while datetime.strptime(raw_data[i][1], '%Y-%m-%d %H:%M:%S') < last and i < len(raw_data)-1:
			minutebar_data.append(raw_data[i])
			#print minutebar_data[len(minutebar_data)-1]
			i +=1
		if minutebar_data:
			create_minutebar(cursor, minutebar_data, first, open_price)
		else:
			create_empty_minutebar(cursor, first, open_price)
		
		first += timedelta(seconds = 60)
		last += timedelta(seconds = 60)
	print "Database Committed..."

def update_hourbars(cursor):
	first_hour = set_first_hour(cursor)
	last_hour = set_last_hour(first_hour)
	
	cursor.execute('''SELECT datetime FROM minutebars ORDER BY datetime DESC LIMIT 1''')
	end = datetime.strptime(cursor.fetchone()[0], '%Y-%m-%d %H:%M:%S')

	while first_hour <= end:
		#print first_hour
		close_price = set_close_price_hour(cursor, first_hour,last_hour)
		open_price = set_open_price_hour(cursor)
		cursor.execute('''SELECT Max(high), Min(low), Sum(volume), Sum(volume*price)/Sum(volume)
			FROM minutebars
			WHERE datetime BETWEEN ? AND ?''', (first_hour, last_hour))
		raw_data = cursor.fetchall()

		if raw_data:
			create_hourbar(cursor, raw_data, first_hour, open_price, close_price)
		else:
			create_empty_hourbar(cursor, first_hour, open_price)
		first_hour += timedelta(minutes = 60)
		last_hour += timedelta(minutes = 60)
	print "Database Committed..."

def update_daybars(cursor):
	first_day = set_first_day(cursor)
	last_day = set_last_day(first_day)
	
	cursor.execute('''SELECT datetime FROM hourbars ORDER BY datetime DESC LIMIT 1''')
	end = datetime.strptime(cursor.fetchone()[0], '%Y-%m-%d %H:%M:%S')

	while first_day <= end:
		#print first_day
		close_price = set_close_price_day(cursor, first_day,last_day)
		open_price = set_open_price_day(cursor)
		cursor.execute('''SELECT Max(high), Min(low), Sum(volume), Sum(volume*price)/Sum(volume)
			FROM hourbars
			WHERE datetime BETWEEN ? AND ?''', (first_day, last_day))
		raw_data = cursor.fetchall()

		if raw_data:
			create_daybar(cursor, raw_data, first_day, open_price, close_price)
		else:
			create_empty_daybar(cursor, first_day, open_price)
		first_day += timedelta(hours = 24)
		last_day += timedelta(hours = 24)
	print "Database Committed..."

if __name__ == '__main__':
	#***choose a file and DB from command line***
	# script, db_name = argv
	# #Create DB and Insert
	# db = sqlite3.connect(db_name)
	db = sqlite3.connect("test.db")

	cursor = db.cursor()
	
	create_tables()

	print "Getting table count..."
	update_minutebars(cursor)
	print "Minute bars done..."
	db.commit()
	
	update_hourbars(cursor)
	print "Hour bars done..."
	db.commit()	
	
	update_daybars(cursor)
	print "Day bars done..."
	db.commit()

	close_database()
import sqlite3
import csv
import sys
from datetime import datetime
from sys import argv

#***choose a file and DB from command line***
# script, filename, db_name = argv
# #Create DB and Insert
# db = sqlite3.connect(db_name)

# # #Read .CSV file
# raw_data = open(filename, 'rU')
def make_transactions():
	db = sqlite3.connect("volatility.db")
	raw_data = open("bitstampUSD.csv", 'rU')

	print "File Opened..."

	# test = csv.reader(raw_data, dialect=csv.excel_tab, delimiter=',')
	# row_count = sum(1 for row in test)

	# print "Rows in bitstampUSD: %d" % (row_count)

	cursor = db.cursor()

	cursor.execute('''CREATE TABLE IF NOT EXISTS transactions(id INTEGER PRIMARY KEY, currency TEXT,
		 datetime text, price REAL, quantity REAL, unixdate INTEGER)
	''')

	#Check the current table:
	#if there are items in it, grab the unixdate, price and quantity of the last one.
	#while iterating on the list, 
		#if the unix date is less than the last one ignore it.
		#if the unixdate is equal to the last one... NEED HELP
		#if the unix date is greater than the last one, add it.

	#Read each line of the file and add it to the table

	#store unix time, find the highest, and only insert from csv if the Unix time is higher than the last stored.

	transactionreader = csv.reader(raw_data, dialect=csv.excel_tab, delimiter=',')

	cursor.execute('''SELECT unixdate FROM transactions ORDER BY unixdate DESC LIMIT 1''')
	results = cursor.fetchall()
	if not results or not results[0]:
		last_date = [0,]
	else:
		last_date = results[0]
		
	print "last date in table = %d" % (last_date[0])
	i = 0


	for row in transactionreader:
		unixdate = row[0]
		price = row[1]
		quantity = row[2]
		unique = True
		#parse and add to DB.
		#print "Added: %s, %s, %s" % (row[0], row[1], row[2])
		# print row[0]
		# print last_date
		if int(unixdate) == int(last_date[0]):
			cursor.execute('''SELECT unixdate, price, quantity from transactions where unixdate = ?''', [unixdate])
			same_second = cursor.fetchall()
			for row in same_second:
				if price == row[1] and quantity == row[2]:
					unique = False
			if unique:
				cursor.execute('''INSERT INTO transactions(datetime, currency, price, quantity, unixdate)
					VALUES(?, ?, ?, ?,?)''',(datetime.utcfromtimestamp(float(unixdate)), "BTC" ,price, quantity, unixdate))
				i+=1
		elif int(unixdate) > int(last_date[0]):
			print row
			cursor.execute('''INSERT INTO transactions(datetime, currency, price, quantity, unixdate)
					VALUES(?, ?, ?, ?,?)''',(datetime.utcfromtimestamp(float(unixdate)), "BTC" ,price, quantity, unixdate))
			i+=1

	print "%d Values inserted..." % (i)

	# Sample fetch
	# cursor.execute('''SELECT id, unixdate, price, quantity FROM transactions
	# 	WHERE unixdate BETWEEN 1315922016 AND 1315942379''')
	# print cursor.fetchall()

	db.commit()
	print "Database Committed..."

	db.close()
	print "Database Closed"

if __name__ == '__main__':
	make_transactions()
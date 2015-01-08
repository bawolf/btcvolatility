from fetch_data import *
from make_transactions import *
from make_bars import *
from calc_vol_minute import *
from calc_vol_hour import *
from calc_vol_day import *
from mail_vol import *


#email settings
subject = "Daily Volatility Numbers: %s" % (datetime.now().date())
recipients = ["bryant.wolf@bayhillcap.com", "julien.seguin@bayhillcap.com", "alec.petro@bayhillcap.com"]


print "Fetching Data..."
fetch_bitcoinchart_data('bitstampUSD')
print "bitstampUSD updated..."

make_transactions()

db = sqlite3.connect("volatility.db")
cursor = db.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS transactions(id INTEGER PRIMARY KEY, currency TEXT,
		 datetime TEXT, price REAL, quantity REAL, unixdate INTEGER)''')

#consider adding id of first and last in transactions for easy viewing
cursor.execute('''CREATE TABLE IF NOT EXISTS minutebars(datetime TEXT PRIMARY KEY, currency TEXT,
	 high REAL, low REAL, open REAL, close REAL, volume REAL, price REAL)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS hourbars(datetime TEXT PRIMARY KEY, currency TEXT,
	 high REAL, low REAL, open REAL, close REAL, volume REAL, price REAL)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS daybars(datetime TEXT PRIMARY KEY, currency TEXT,
	 high REAL, low REAL, open REAL, close REAL, volume REAL, price REAL)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS minute_vol(datetime TEXT PRIMARY KEY, currency TEXT, log REAL)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS hour_vol(datetime TEXT PRIMARY KEY, currency TEXT, log REAL)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS day_vol(datetime TEXT PRIMARY KEY, currency TEXT, log REAL)''')

print "Tables Opened..."

#Create Bars
print "Creating bars..."
update_minutebars(cursor)
db.commit()
print "Minute bars done..."
update_hourbars(cursor)
db.commit()
print "Hour bars done..."
update_daybars(cursor)
db.commit()
print "Day bars done..."

#Calculate Volatility
print "Calculating Volatility..."
first = set_first_minute(cursor)
update_minute_vol(cursor, first)
db.commit()
print "Minute volatility done..."

first = set_first_hour(cursor)
update_hour_vol(cursor, first)
db.commit()
print "Hour volatility done..."

first = set_first_day(cursor)
update_day_vol(cursor, first)
db.commit()
print "Day volatility done..."

#Email Volatility
print "Creating Email..."
body = "Volatility by range:"
body += add_data(cursor, "day", 365)
# body += add_data(cursor, "hour", 365*24)
# body += add_data(cursor, "minute", 365*24*60)
print body
sendFhcmReport(subject, recipients, body)
print "Email sent."

db.commit()
print "Database Committed..."

db.close()
print "Database Closed"
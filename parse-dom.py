from xml.dom.minidom import parse, parseString
import sys
import csv
import re
import os
import mysql.connector

#insert top 100
def insert(cursor, row):
	query = 'INSERT INTO stocks(exchange, symbol, company, volume, price, stocks.change) VALUES (%s,%s,%s,%s,%s,%s)'
	cursor.execute(query, (row['exchange'],row['symbol'],row['company'], row['volume'], row['price'], row['change']))

#if it exists do an update
def update(cursor, row, currentId):
	query = 'UPDATE stocks SET volume=%s, price=%s, stocks.change=%s WHERE id=%s;'
	cursor.execute(query, (row['volume'], row['price'], row['change'], currentId))

# #check if it exists
def select(cursor, row):
	symbol = row['symbol']
	query = 'select * from stocks where symbol=%s'
	cursor.execute(query, (row['symbol'],))

def hitDB(row):
	print('hit db instead of writing to csv')
	try:
		cnx = mysql.connector.connect(host='localhost', user='root', password='root', database='288-hw7', port=32769)
		cursor = cnx.cursor()
		insert(cursor, row)
		cnx.commit()

	except mysql.connector.Error as err:
		print(err)
	finally:
		try:
			cnx
		except NameError:
			pass
		else:
			cnx.close()
#END OF HITDB FUNC



# parse an XML file by name
dom1 = parse(sys.argv[1])

#find table with classname mdcTable
tables = dom1.getElementsByTagName("table")

table = dom1.createElement("foo")
for t in tables:
	if 'class' in t.attributes:
		if t.attributes['class'].nodeValue == 'mdcTable':
			table = t
			break


#get exchange name
spans = dom1.getElementsByTagName("span")

exchangeName = ''
for sp in spans:
	if 'class' in sp.attributes:
		if sp.attributes['class'].nodeValue == 'mdcTblName':
			pattern = re.compile(r"\w*\s")
			match = pattern.search(sp.firstChild.data)
			exchangeName = match.group(0)[:-1]
			break


#get col labels
labelRow = table.firstChild
labels = ["exchange", "symbol", "company", "volume", "price", "change"]
addHeaders = not os.path.isfile('stocks.csv') #if it exists, dont add rows

with open("stocks.csv", "a") as csvfile:
	writer = csv.writer(csvfile, delimiter=',')
	# reader = csv.reader(csvfile)
	if addHeaders: #if first time (file creation), write header rowss
		writer.writerow(labels)
	colArr = []
	for n, row in enumerate(table.childNodes, start=0):
		if n == 0: #skip header row
			continue
		#add exchange name to each row
		colArr.append(exchangeName)
		for i, col in enumerate(row.childNodes, start=0):

			if i != 0 and i!=5: #ignore first and last col

				if i == 1: #name col behaves different
					if n!= 0:
						name = col.childNodes[1].firstChild.data
						name = name[:-1] #ignore last item \n

						#extract symbol
						pattern = re.compile('\(\w*\)')
						match = pattern.search(name)
						symbol = match.group(0)[1:-1]

						#remove symbol from name
						end = match.regs[0][0] #start index of regex match
						name = name[0:(end - 1)] #accounting for space
						colArr.append(symbol)
						colArr.append(name)

				elif i == 2: #remove commas from volume col
					if n != 0: #ignore the header row
						num = float(col.firstChild.data.replace(',', ''))
						colArr.append(num)
				
				elif i == 3:
					print('format the price')
					num = float(col.firstChild.data.replace('$', ''))
					colArr.append(num)
				# elif i == 3
				elif i == 4:
					print('format the change')
					num = float(col.firstChild.data)
					colArr.append(num)
				else:
					colArr.append(col.firstChild.data)
		#finished row loop
		writer.writerow(colArr)
		obj = { 'exchange': colArr[0], 'symbol': colArr[1], 'company': colArr[2], 'volume': colArr[3], 'price': colArr[4], 'change': colArr[5] }
		hitDB(obj)
		colArr = []
	#finished table loop

#end of script
sys.exit()
import csv
import random

#determine how many data points to geneate
rows_to_generate = 502

#return a date, different depending on row number
def get_date(inx):
	if inx < int(rows_to_generate/2):
		return '2020-14-12'
	else:
		return '2020-13-12'

#generate data
all_rows = [[random.randint(1,10000),get_date(x),x,random.randint(0,50),x/4] for x in range(1,rows_to_generate)]
#optionally, set a header
all_rows[0] = ["id","date","time","val1","val2"]

#open a csv file, write out each row to the file
with open('toy_data.csv','w') as f:
	writer = csv.writer(f,delimiter=',')
	for csv_row in all_rows:
		writer.writerow(csv_row)
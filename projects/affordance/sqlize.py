"""Convert extracted output into sqlite files"""

import sqlite3

def prob_txt_to_sql(filename, idx=0):
	"""change text file to sqlite file """
	# FIXME: support start from random line (not yet tested )
	# FIXME: current code is not subject to sql injection
	# FIXME: current code save the output to first level folder, save to output folder
	
	# Create a sql database
	conn = sqlite3.connect('%s.db' % filename)
	c = conn.cursor()
	# prepare the names for columns
	c_name = filename.rsplit("_")

	# Create a table with the column names
	try: 
		c.execute("""CREATE TABLE %s(
					%s text,
					%s text,
					%s real 
					)""" %(filename, c_name[2], c_name[1],c_name[0]))
	# if file exist, return
	except sqlite3.OperationalError:
		print("the sqlite file for %s already exist" % filename)
		return 

	# open the text file and write to db
	with open("data/output/trial_5/%s.txt" % filename) as f:
		# iterate through each line
		for i, line in enumerate(f): 
			# start to write from the indicated line (idx) to avoid duplicate
			if i > idx: 
				line = line.rsplit(" ")
				assert len(line) == 3, "this line does not \
					conform to the triplet format at file line %d" % i 
				c.execute("INSERT INTO %s VALUES (?, ?, ?)" % filename, (line[0], line[1], line[2]))
			else: 
				continue
			# commit every 100 times
			if (i % 100) == 0: 
				conn.commit()
			# print on screen to check progress every 100000 times
			if (i % 100000) == 0:
				print("commited %d lines" % i)


def get_entry_by_noun(filename, noun, items=0):
	"""utility function that obtain noun from database with a descending order of probability

	    :param filename: name of the db
	    	   noun: the noun to extract
	    	   items: if 0 -> fetch all
	    	   		  else: fetch items many items
    	:return: a list of tuple in the db
    """
	conn = sqlite3.connect('data/prob_sql/%s.db' % filename)
	c = conn.cursor()
	c.execute("SELECT * FROM %s WHERE noun='%s' ORDER BY prob DESC" % (filename, noun))

	if items == 0:
		return c.fetchall()
	else: 
		return c.fetchmany(items)
	

def get_entry_by_verb(filename, verb, items=0):
	"""utility function that obtain verb from database with a descending order of probability

    :param filename: name of the db
    	   verb: the verb to extract
    	   items: if 0 -> fetch all
    	   		  else: fetch items many items
	:return: a list of tuple in the db
    """
	conn = sqlite3.connect('data/prob_sql/%s.db' % filename)
	c = conn.cursor()
	c.execute("SELECT * FROM %s WHERE verb='%s' ORDER BY prob DESC" % (filename, verb))

	if items == 0:
		return c.fetchall()
	else: 
		return c.fetchmany(items)


def get_entry_by_adj(filename, adj, items=0):
	"""utility function that obtain adj from database with a descending order of probability

    :param filename: name of the db
    	   adj: the adj to extract
    	   items: if 0 -> fetch all
    	   		  else: fetch items many items
	:return: a list of tuple in the db
    """
	conn = sqlite3.connect('data/prob_sql/%s.db' % filename)
	c = conn.cursor()
	c.execute("SELECT * FROM %s WHERE adj='%s' ORDER BY prob DESC" % (filename, adj))

	if items == 0:
		return c.fetchall()
	else: 
		return c.fetchmany(items)


def pipeline():
	"""This pipeline encode listed files to sqlite form"""
	from time import time

	files = ["prob_adj_noun", "prob_noun_adj", "prob_noun_verb", "prob_verb_adj_3", "prob_verb_noun"]

	for filename in files: 
		start = time()
		prob_txt_to_sql(filename)
		end = time()
		print("%s total use time %s s" % (filename, (end - start)))


def main():
	# transfer files to sqlite darabase
	pipeline()
	
	## Running trials
	# filename = "prob_verb_adj_3"
	# print(get_entry_by_verb(filename, "kill"))
	# print(get_entry_by_noun(filename, "brick"))
	# print(get_entry_by_adj("prob_verb_adj_3", "sharp", 10))


if __name__ == "__main__":
	main()
#! /usr/bin/env python

# Copyright 2013 Jtmorgan

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import MySQLdb



def getRows():
	editors = []
# 	cursor.execute('''(SELECT user_id, user_name, sample_date sd FROM twa_invitees WHERE twa_played = 1 and has_subsequent_edit = 1) UNION (SELECT user_id, user_name, twa_completed_date sd FROM twa_invitees WHERE twa_played = 1)
# 	''')
	cursor.execute('''SELECT user_id, user_name, user_registration FROM twa_invitees_May2014 WHERE twa_played is not null and has_subsequent_edit = 1
	''')
	rows = cursor.fetchall()
	for row in rows:
		e = {'user_id' : row[0], 'user_name' : MySQLdb.escape_string(row[1]), 'timestamp' : row[2]}
		editors.append(e)
	return editors

#get cumulative post_invite edits
def getCumEdits(e):
	cursor.execute('''SELECT count(rev_id) AS cum_revs FROM enwiki.revision r inner join enwiki.page p on r.rev_page = p.page_id WHERE rev_user = %d AND rev_timestamp > %s AND replace(p.page_title, "_"," ") not like "%s"''' % (e['user_id'], e['timestamp'], e['user_name'] + '%', ))
	if cursor.rowcount != 0:
		row = cursor.fetchone()
		e['all'] = row[0]
	else:
		e['all'] = 0
# 		print "user " + e['user_name'] + " does not have any subsequent edits after all"
	return e

def getNs0Edits(e):
	cursor.execute('''SELECT count(rev_id) AS cum_revs FROM enwiki.revision r inner join enwiki.page p on r.rev_page = p.page_id WHERE rev_user = %d AND p.page_namespace IN (0) AND rev_timestamp > %s''' % (e['user_id'], e['timestamp']))
	if cursor.rowcount != 0:
		row = cursor.fetchone()
		e['ns0'] = row[0]
	else:
		e['ns0'] = 0
# 		print "user " + e['user_name'] + " does not have subsequent article edits after all"
	return e

def getTalkNsEdits(e):
	cursor.execute('''SELECT count(rev_id) AS cum_revs FROM enwiki.revision r inner join enwiki.page p on r.rev_page = p.page_id WHERE rev_user = %d AND p.page_namespace IN (1, 3, 5) AND rev_timestamp > %s AND replace(p.page_title, "_"," ") not like "%s"''' % (e['user_id'], e['timestamp'], e['user_name'] + "%", ))
	if cursor.rowcount != 0:
		row = cursor.fetchone()
		e['talk'] = row[0]
	else:
		e['talk'] = 0
# 		print "user " + e['user_name'] + " does not have subsequent talk edits"
	return e

#gets num articles edited
def getArticlesEdited(e):
	cursor.execute('''SELECT count(distinct rev_page) FROM enwiki.revision r inner join enwiki.page p on r.rev_page = p.page_id WHERE rev_user = %d AND p.page_namespace IN (0) AND rev_timestamp > %s''' % (e['user_id'], e['timestamp'],))
	if cursor.rowcount != 0:
		row = cursor.fetchone()
		e['articles'] = row[0]
	else:
		e['articles'] = 0
# 		print "user " + e['user_name'] + " didn't edit any more articles"
	return e

# def updateEditCounts(e):
# 	cursor.execute('''UPDATE twa_invitees_May2014 set edits_ns0 = %d WHERE user_id = %d''' % (e['ns0'], e['user_id'],))
# 	conn.commit()

def updateEditCounts(e):
	cursor.execute('''UPDATE twa_invitees_May2014 set edits_all = %d, edits_ns0 = %d, edits_talk = %d, articles = %d WHERE user_id = %d''' % (e['all'], e['ns0'], e['talk'], e['articles'], e['user_id'],))
	conn.commit()

	
##main##
conn = MySQLdb.connect(host = 'db1047.eqiad.wmnet', db = 'jmorgan', read_default_file = '~/.my.cnf' )
cursor = conn.cursor()
editors = getRows()
for e in editors:
	e = getCumEdits(e)
	e = getNs0Edits(e)
	e = getTalkNsEdits(e)
	e = getArticlesEdited(e)
	e = updateEditCounts(e)

cursor.close()
conn.close()
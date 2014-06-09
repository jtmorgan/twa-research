#! /usr/bin/env python

# Copyright 2012 Jtmorgan

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
from random import shuffle
import sys
from warnings import filterwarnings

###FUNCTIONS####
# def updateActiveStatus(cursor):
# 	cursor.execute('''SELECT user_id, sample_date from twa_invitees_May2014 where has_subsequent_edit IS NULL''')
# 	rows = cursor.fetchall()
# 	for row in rows:
# 		cursor.execute('''UPDATE twa_invitees_May2014 t, (SELECT rev_user, MAX(rev_timestamp) ts FROM enwiki.revision WHERE rev_user = %d) tmp SET t.has_subsequent_edit = CASE
# 		WHEN tmp.ts > %s THEN 1
# 		ELSE 0
# 		END
# 		WHERE t.user_id = tmp.rev_user''' % (row[0], row[1],))
# 		conn.commit()
# 
# def addPlayers(cursor):
# 	cursor.execute('''insert ignore into twa_eval_sample_May2014 select user_id, sample_date, twa_played from twa_invitees_May2014 where sample_group = "exp" and invited = 1 and twa_played = 1 and has_subsequent_edit = 1''')
# 	conn.commit()

def getDates(cursor):
	sample_dates = []
	cursor.execute('''select sample_date, count(user_id) from twa_eval_sample_May2014 where grp = 1 group by sample_date order by sample_date asc''')
	rows = cursor.fetchall()
	for row in rows:
		s = (row[0], row[1])
		sample_dates.append(s)
	return sample_dates

def getControls(cursor, sample_dates, group, invited):
	rmvs = []
	sample = []
	for s in sample_dates:
		cursor.execute('''SELECT user_id from twa_invitees_May2014 where has_subsequent_edit = 1 and blocked = 0 and skipped = 0 and sample_date = %s and sample_group = "%s" and invited = %d and twa_played is null order by rand() limit %d''' % (s[0], group, invited, s[1],))
		rows = cursor.fetchall()
		num = cursor.rowcount
		if num < s[1]:
# 			print s
# 			print cursor.rowcount
			r_num = s[1] - num
			rm = (s[0], r_num)
			rmvs.append(rm)
		else:
			pass

		for row in rows:
			sample.append(row[0])
	return (sample, rmvs)


def insertSample(cursor, sample, grp):
	for s in sample:
		cursor.execute('''INSERT IGNORE INTO twa_eval_sample_May2014 (user_id, grp) VALUES (%d, %d)''' % (s, grp,))
		conn.commit()

def updateDates(cursor):
	cursor.execute('''update twa_eval_sample_May2014 es inner join twa_invitees_May2014 twa on es.user_id = twa.user_id set es.sample_date = twa.sample_date where es.sample_date is null''')
	conn.commit()

def deleteExtras(cursor, rmvs, grp):
	for r in rmvs:
		cursor.execute('''DELETE FROM twa_eval_sample_May2014 WHERE grp = %d and sample_date = %s ORDER BY RAND() LIMIT %d''' % (grp, r[0], r[1]))
		conn.commit()

###MAIN###
filterwarnings('ignore', category = MySQLdb.Warning)
conn = MySQLdb.connect(host = 'db1047.eqiad.wmnet', db = 'jmorgan', read_default_file = '~/.my.cnf' )
cursor = conn.cursor()
# updateActiveStatus(cursor)

# addPlayers(cursor)
sample_dates = getDates(cursor)
control_a = getControls(cursor, sample_dates, "exp", 1)
invitees = control_a[0]
in_rmvs = control_a[1]
insertSample(cursor, invitees, 2)
control_b = getControls(cursor, sample_dates, "con", 0)
true_controls = control_b[0]
cn_rmvs = control_b[1]
insertSample(cursor, true_controls, 0)
updateDates(cursor)
# print "There are " + str(len(cn_rmvs)) + " controls in the remove list"
deleteExtras(cursor, cn_rmvs, 1)
deleteExtras(cursor, cn_rmvs, 2)

cursor.close()
conn.close()

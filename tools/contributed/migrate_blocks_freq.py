#!/usr/bin/python
#############################################
# migrate_blocks_freq.py                    #
#                                           #
# A simple python program that retrieves    #
# btier block placement metadata            #
#############################################
#                                           #
# v.0.2.6                                   #
#                                           #
# originally by Mark Ruijter                #
# additional work by Stephan Budach         #
#############################################
#           D I S C L A I M E R             #
#                                           #
# This script is provided "as is" and the   #
# author is not held to be responsible for  #
# any damage or data loss, that might occur #
# due to using this script.                 #
# The purpose of this script is to offer    #
# an option for anyone to build a migration #
# on his or her own!                        #
#                                           #
#############################################
import os
import sys,errno
import stat
import time
import datetime
import curses
import sqlite3 as sqlite
import subprocess


from stat import *
from sys import argv
from thread import start_new_thread, allocate_lock
from optparse import OptionParser



RET_OK=0
RET_NODEV=-1
RET_SYSFS=-2
RET_SYSERR=-3
RET_MIGERR=-4
numMigrateThreads=0
maxMigrateThreads=10
lock = allocate_lock()
dbfile = ""
# Move to a faster tier when the block is hit 1.5 times more then avg on that tier
THRESHOLD_DOWN=1.5
# Move to a slower tier when the block is hit 0.5 times less then avg on that tier
# Always move blocks to a higher tier if the device is below tierDeviceCapacity %-
THRESHOLD_UP=0.5
# set tierDeviceCapacity factor, 1.00 == 100%
tierDeviceCapacity = 0.85
# MAXAGE in seconds
MAXAGE=14400
# for the sake of readability, use blockGracePeriod
blockGracePeriod = MAXAGE

# placeholder for AVG reads and write for each resp. device
AVGread = {}
AVGwrite = {}
thresholdReadUp = {}
thresholdReadDown = {}
thresholdWriteUp = {}
thresholdWriteDown = {}
tierUsedBlocks = {}
tierReadCounts = {}
tierWriteCounts = {}
tierBlockDevice = {}
tierBlockOffset = {}
tierBlockAtime = {}
tierBlockReads = {}
tierBlockWrites = {}
tierDeviceSize = {}
tierBlockList = {}
tierBlocksLeft = {}

# globals for curses
hpos = 0
vpos = 0

def sql_drop_create(cur):
	print "resetDB: %d" % (resetDB)
	try:
		if resetDB:
			# create table meta with the following columns
			# blocknr, atime, hits
			cur.execute("DROP TABLE IF EXISTS meta")
			con.commit()
			cur.execute("CREATE TABLE meta( \
				blocknr LONG LONG INT PRIMARY KEY, \
				atime UNSIGNED INT, \
				tier INT, \
				hits UNSIGNED INT, \
				grace INT DEFAULT 0)")
			con.commit()
		
		# create table blockmove with the following columns
		# blocknr,currentTier,newTier
		cur.execute("DROP TABLE IF EXISTS blockmove")
		con.commit()
		cur.execute("CREATE TABLE blockmove( \
			blocknr LONG LONG INT PRIMARY KEY, \
			freq REAL, \
			currentTier INT)" )
		cur.execute("CREATE INDEX blockmove_device_idx ON blockmove (currentTier)")
		con.commit()
	except:
		print "SQL Error!\n"
		return
	else:
		return

def sql_check_tables(cur):
	try:
		cur.execute("SELECT * FROM sqlite_master WHERE name ='meta' and type='table'")
		record = cur.fetchone()
		if record == None:
			return 1
		else:
			return 0
	except sqlite.Error, e:
		print "Error %s:" % e.args[0]
		exit(DB_ERR)
	
def con_open(DBfile):
	try:
		con = sqlite.connect(DBfile)
	except sqlite.Error, e:
		print "Error %s:" % e.args[0]
		exit(DB_ERR)
	else:
		return con

def write_sql(cur,blocknr,blockinfo):
	binfo=blockinfo.split( ',' )
	device=binfo.pop(0)
	offset=int(binfo.pop(0))
	atime=int(binfo.pop(0))
	readcount=int(binfo.pop(0))
	writecount=int(binfo.pop(0))
	cur.execute("INSERT INTO meta VALUES(?, ?, ?, ?, ?, ?)", \
		(blocknr,device,offset,atime,readcount,writecount))

def sql_open(con):
	try:
		with con:
			cur = con.cursor()
	except sqlite.Error, e:
		print "Error %s:" % e.args[0]
		exit(DB_ERR)
	else:
		return cur

def checkPID():
	# checks if another migration is currently running
	myPID = str(os.getpid())+"\n"
	pidFile = "/var/run/migrate_blocks_threaded.pid"	
	if os.path.exists(pidFile):
		pFile = open(pidFile,"r+")
		otherPID = pFile.read()
		otherPID = otherPID.rstrip()
		cmd = "ps aux | grep %s | grep -v grep" % (otherPID)
		migrationRunning = str(os.system(cmd))
		if len(migrationRunning) < 3:
   			print "There is already a migration running... exiting!\n"
   			pFile.close()
   			exit(RET_OK)
   		else:
   			print "Found stale PID - removing it...\n"
   			pFile.close()
   			os.system("echo myPID > " + pidFile)
   			pFile = open(pidFile,"w+")
   			pFile.write(myPID)
   			pFile.close()
	else:
		os.system("echo myPID > " + pidFile)
		pFile = open(pidFile,"w+")
		pFile.write(myPID)
		pFile.close()
			
def print_file(fp):
    fp.seek(0);
    print fp.read()

def get_ctime():
	t=datetime.datetime.now()
	return time.mktime(t.timetuple())

def usage():
	print "Usage : %s -b [sdtierX] [-m | -migrate] |-v | --verbose] [-l | --log] [-h | --help]" % argv.pop(0)
	pidFile = "/var/run/migrate_blocks_threaded.pid"
	os.remove(pidFile)
	exit(RET_OK)

def exitScript():
	# make a beep
	print "\a"
	if verbose:
		screen.refresh()
	
	printText = "Finished processing... Press key\n"
	vpos = 25
	hpos = 0
	if verbose:
		printScreen(vpos,hpos,printText)
		# Warten auf Tastendruck
		c = screen.getch()
		curses.doupdate()
		curses.endwin()
	pidFile = "/var/run/migrate_blocks_threaded.pid"
	os.remove(pidFile)
	
	if sql:
		con.close()
		
	exit(RET_OK)

def printScreen(vpos,hpos,printText):
	try:
		if verbose:
			screen.addstr(vpos,hpos,printText)
			screen.refresh()
	except:
		pass

def savestat(device):
    try:
       statinfo=os.stat(device)
    except:
       return -1
    else:
       return 0

def check_valid_device(device):
    res=savestat(device)
    if res != 0:
       print "Not a valid tier device : %s" % device
       exit(RET_NODEV)

def read_total_blocks():
    blocks=0
    try:
        fp=open("/sys/block/"+basename+"/tier/size_in_blocks","r")
        blocks=fp.read()
        fp.close()
    except:
        return int(int(blocks)/1)
    else:
        return int(int(blocks)/1)

def read_maxdev():
    apipath="/sys/block/"+basename+"/tier/attacheddevices"
    try:
       fp=open(apipath,"r")
       devstr=fp.read()
       fp.close()
    except:
       print "Failed to determine attached devices"
       exit(RET_MIGERR)
    else:
       return int(devstr)

def get_tier_device_sizes():
	maxdev = read_maxdev()
	cmd = "cat /sys/block/%s/tier/device_usage | grep -v -i tier | awk 'NF > 2 {print $3}'" % (basename)
	output = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, shell=True)
	result = output.communicate()[0]
	return result.split( '\n' )
	
def get_tier_blocks_left():
	# calculate the blocks left on each tier, taking the tierDeviceCapacity
	device = 0
	maxdev = read_maxdev()
	tierDeviceSize = get_tier_device_sizes()
	while device <= maxdev-2:
		tierBlocksLeft[device] = int(int(tierDeviceSize[device])*tierDeviceCapacity) - \
			tierUsedBlocks[device]
		device += 1
	# set blocks left in slowest tier to 100 % - currently used blocks
	tierBlocksLeft[maxdev-1] = int(tierDeviceSize[maxdev-1]) - tierUsedBlocks[maxdev-1]

def migrate_block(blocknr,newdev):
    apipath="/sys/block/"+basename+"/tier/migrate_block"
    try:
       fp=open(apipath,"w")
       command=str(blocknr)+"/"+str(newdev)
       fp.write(command)
       fp.close()
    except IOError as e:
       if e.errno == errno.EAGAIN:
          return errno.EAGAIN
       print "Failed to migrate block %d to device %d" % (blocknr, newdev)
       exit(RET_MIGERR)
    else:
       return 0

def move_block(blocknr,newdev):
    global numMigrateThreads
    # increase thread counter
    lock.acquire()
    numMigrateThreads += 1
    localThread = numMigrateThreads
    lock.release()

    while True:
          res=migrate_block(blocknr, newdev)
          if res != errno.EAGAIN:
             break
          time.sleep (.1)

    lock.acquire()
    numMigrateThreads -= 1
    lock.release()
    return 0

def do_migration(blockCount):
	printText = "Migration starts..."
	vpos = 0
	hpos = 40
	logfile = "migration.log"
	if logs:
		logf = open(logfile,"w+")
		logf.write("Migration Log:\n")
	printScreen(vpos,hpos,printText)
	# get average reads and writes per device and store them
	# in a hash
	maxdev=read_maxdev()
	device = 0
	while device < maxdev:
		thresholdReadUp[device] = (int(AVGread[device] * THRESHOLD_UP))
		thresholdReadDown[device] = int(AVGread[device] * THRESHOLD_DOWN) + 1
		thresholdWriteUp[device] = (int(AVGwrite[device] * THRESHOLD_UP))
		thresholdWriteDown[device] = int(AVGwrite[device] * THRESHOLD_DOWN) + 1
		printText = "tier%d migration thresholds" % (device)
		vpos = 4 + (device * 5)
		printScreen(vpos,hpos,printText)
		printText = "Reads  up: %d:  down: %d" % (thresholdReadUp[device],thresholdReadDown[device])
		vpos = 6 + (device * 5)
		printScreen(vpos,hpos,printText)
		printText = "Writes up: %d:  down: %d" % (thresholdWriteUp[device],thresholdWriteDown[device])
		vpos = 7 + (device * 5)
		printScreen(vpos,hpos,printText)
		device = device + 1
	blocknr=0
	percentBlocks = int(blockCount/100)
	printText = "Considering %d blocks for migration" % (blockCount)
	vpos = 20
	hpos = 0
	migBlocknr = 0
	migDevice = 0
	migOffset = 0
	migAtime = 0
	migReads = 0
	migWrites = 0
	migBusy = 0
	migBlockMigrated = 0
	printScreen(vpos,hpos,printText)
	while blocknr < blockCount:
		migBlockMigrated = 0
		if (blocknr % percentBlocks == 0):
			printText = "Migration progress: %d %%" % (int(blocknr/percentBlocks))
			vpos = 21
			printScreen(vpos,hpos,printText)
		# get the block meta data from used blocks only
		migBlocknr = tierBlockList[blocknr]
		migDevice = tierBlockDevice[migBlocknr]
		migOffset = tierBlockOffset[migBlocknr]
		migAtime = tierBlockAtime[migBlocknr]
		migReads = tierBlockReads[migBlocknr]
		migWrites = tierBlockWrites[migBlocknr]
		migBusy = migReads + migWrites

		# check/wait for available threads
		while numMigrateThreads >= maxMigrateThreads:
			time.sleep(100)

		# calculate the grace period
		migGrace=MAXAGE*(migDevice + 1)
		
		# check if a block's atime is 4 x time lower than the grace-poriod
		# up-migrate it
		if migAtime < (CTIME - (migGrace*4)) and migDevice < maxdev-1:
			# if a block hasn't been used for 4 x grace period up-migrate it
			move_block(migBlocknr,migDevice+1)
			if logs:
				logfString = "Up: %d,%d,%d,%d,%d,%d\n" % (migBlocknr,migDevice,migDevice+1, \
					migAtime,migReads,migWrites)
				logf.write(logfString)
			migBlockMigrated = 1
		
		busyDown = int(thresholdReadDown[migDevice]+thresholdWriteDown[migDevice])
		# consider block for down-migration if it's block count exceeds the threshold
		if migBusy > busyDown and migBlockMigrated ==0:
			# if the block is not on the lowest tier migrate it one tier down
			if migDevice > 0:
				move_block(migBlocknr,migDevice-1)
				if logs:
					logfString = "Down: %d,%d,%d,%d,%d,%d\n" % (migBlocknr,migDevice,migDevice-1, \
					migAtime,migReads,migWrites)
					logf.write(logfString)
				migBlockMigrated = 1
			elif migDevice == 0:
				# if the block is already on tier 0 mark it as migrated
				migBlockMigrated = 1

		# if the block has not been migrated it might be a candidate for up-migration
		# if the block is not already on the highest tier,move it one tier up
		if migDevice < maxdev-1 and migBlockMigrated == 0:
			# if the block's atime is out of the grace period check it's hit counts
			if migAtime < (CTIME - migGrace):
				busyUp = int(thresholdReadDown[migDevice+1]+thresholdWriteDown[migDevice+1])
				if migBusy < busyUp + 1:
					move_block(migBlocknr,migDevice+1)
					if logs:
						logfString = "Up: %d,%d,%d,%d,%d,%d\n" % (migBlocknr,migDevice,migDevice+1, \
							migAtime,migReads,migWrites)
						logf.write(logfString)
					migBlockMigrated = 1
		blocknr+=1

def do_sql_migration():
	# perform block migration based on the blockmove data
	get_tier_blocks_left()
	maxdev = read_maxdev()
	migratedBlocks = 0
	blockMoveRate = 0
	guiUpdate = 5
	guiCTIME = 0
	deltaCTIME = 0
	previousCTIME = 0
	previousMigratedBlocks = 0
	deltaMigratedBlocks = 0
	blockMoveETA = 0
	guiUpdateBlock = 0
	hrsETA = 0
	minETA = 0
	secETA = 0
	
	printText = "SQL-based Migration starts..."
	vpos = 0
	hpos = 40
	logfile = "sql_migration.log"
	if logs:
		logf = open(logfile,"w+",0)
		logf.write("Migration Log:\n")
	printScreen(vpos,hpos,printText)

	# iterate over the blocklist, until it's empty
	cur.execute("SELECT count(*) from blockmove")
	record = cur.fetchone()
	blocksToMove = record[0]
	percentBlocks = int(blocksToMove/100)
	printText = "Blocks to move: %d " % (blocksToMove)
	vpos = 1
	printScreen(vpos,hpos,printText)
	
	while record[0] > 0:
		cur.execute("SELECT * from blockmove ORDER by freq")
		blockList = cur.fetchall()
		if logs:
			logString = "Querying blockmove...\n"
			logf.write(logString)
		for moveBlock in blockList:
			guiCTIME = get_ctime()
			# print statistics at guiUpdate intervals
			if ((guiCTIME % guiUpdate == 0) and ( guiUpdateBlock != guiCTIME)):
				guiUpdateBlock = guiCTIME
				deltaMigratedBlocks = migratedBlocks - previousMigratedBlocks
				previousMigratedBlocks = migratedBlocks
				if previousCTIME != 0:
					deltaCTIME = guiCTIME - previousCTIME
					previousCTIME = guiCTIME
				else:
					deltaCTIME = guiUpdate
					previousCTIME = guiCTIME
				if deltaCTIME != 0:
					blockMoveRate = int(deltaMigratedBlocks / deltaCTIME)
				else:
					blockMoveRate = 0
				if blockMoveRate != 0:
					secETA = int(blocksToMove - migratedBlocks)/blockMoveRate
					hrsETA = int(secETA/3600)
					minETA = int((secETA - (hrsETA*3660))/60)
					secETA -= ((minETA*60) + (hrsETA*3600))
				else:
					secETA = 0
					minETA = 0
					hrsETA = 0
				vpos = 3
				printText = "Block move rate: %d b/s" % (blockMoveRate)
				printScreen(vpos,hpos,printText)
				vpos = 4
				printText = "Migration will finish in (ETA): %02d hrs %02d mins" % \
					(hrsETA,minETA)
				printScreen(vpos,hpos,printText)
				exit
			if logs:
				logString = "Block: %d, freq: %f, curTier: %d\n" % \
					(moveBlock[0],moveBlock[1],moveBlock[2])
				logf.write(logString)
			# if the block's freq is > 0, it's an 'active' block
			if moveBlock[1] > 0:
				# check the block's current tier
				if moveBlock[2] == 0:
					# if the current block is already on tier 0
					# simply remove it from the blockmove table
					if logs:
						logString = "Block stays: %d, freq: %f, curTier: %d\n" % \
							(moveBlock[0],moveBlock[1],moveBlock[2])
						logf.write(logString)
					try:
						cur.execute("DELETE from blockmove where blocknr = ?",(moveBlock[0],))
						con.commit()
					except sqlite.Error, e:
						print "Error deleting from blockmove - %s:" % e.args[0]
						exit(DB_ERR)
				elif moveBlock[2] > 0:
					# if the current block is not on tier 0, determine where the block
					# can be stored, by checking the blocks left on each tier, starting at tier0
					tier = 0
					while tier < moveBlock[2]:
						# don't choose a tier that is slower than the one where
						# the current block is stored on
						if tierBlocksLeft[tier] > 0:
							# check/wait for available threads
							while numMigrateThreads >= maxMigrateThreads:
								time.sleep(1/100)
							# move the block directly to new tier
							if logs:
								logString = "Block move down: %d, freq: %f, curTier: %d, newTier: %d\n" % \
									(moveBlock[0],moveBlock[1],moveBlock[2],tier)
								logf.write(logString)
							move_block(moveBlock[0],tier)
							# remove block from blockmove table
							try:
								cur.execute("DELETE from blockmove where blocknr = ?", \
									(moveBlock[0],))
							except sqlite.Error, e:
								print "Error deleting from blockmove - %s:" % e.args[0]
								exit(DB_ERR)
							# Update block's grace flag in meta table
							try:
								cur.execute("UPDATE meta SET hits=0,grace = 1  WHERE blocknr = ?", \
									(moveBlock[0],))
							except sqlite.Error, e:
								print "Error updating meta - %s:" % e.args[0]
								exit(DB_ERR)
							con.commit()
							# update free blocks on tiers
							tierBlocksLeft[tier] -= 1
							tierBlocksLeft[moveBlock[2]] += 1
							break
						tier += 1
			elif moveBlock[1] == 0:
				# if the block's freq is 0, it's an 'inactive' block
				# hence migrate it up one tier
				# check/wait for available threads
				while numMigrateThreads >= maxMigrateThreads:
					if logs:
						logString = "Waiting for free thread...\n"
						logf.write(logString)
					time.sleep(1/100)
				tier = moveBlock[2] + 1
				while (tier <= maxdev - 1):
					# search for a tier to hold the block
					if logs:
						logString = "Search tier: %d\n" % (tier)
						logf.write(logString)
					if tierBlocksLeft[tier] > 0:
						move_block(moveBlock[0],tier)
						if logs:
							logString = "Block move up: %d, freq: %f, curTier: %d, newTier: %d\n" % \
								(moveBlock[0],moveBlock[1],moveBlock[2], tier)
							logf.write(logString)
						# remove block from blockmove table
						try:
							if logs:
								logString = "Deleting from blockmove table...\n"
								logf.write(logString)
							cur.execute("DELETE from blockmove where blocknr = ?",(moveBlock[0],))
						except sqlite.Error, e:
							print "Error deleting from blockmove - %s:" % e.args[0]
							exit(DB_ERR)
						# Update block's grace flag in meta table
						try:
							if logs:
								logString = "Updating meta table...\n"
								logf.write(logString)
							cur.execute("UPDATE meta SET hits=0,grace = 1 WHERE blocknr = ?", \
								(moveBlock[0],))
						except sqlite.Error, e:
							print "Error updating meta - %s:" % e.args[0]
							exit(DB_ERR)
						# update free blocks on tiers
						tierBlocksLeft[tier] -= 1
						tierBlocksLeft[moveBlock[2]] += 1
						# block move was successful, so break this loop
						break
					if logs:
						logString = "Sqlite: start committing\n"
						logf.write(logString)
						con.commit()
						logString = "Sqlite: end committing\n"
						logf.write(logString)
					else:
						con.commit()
					tier += 1
				
			migratedBlocks += 1
			if (percentBlocks != 0):
				if (migratedBlocks % percentBlocks == 0):
					printText = "Migration progress: %d %%" % (int(migratedBlocks/percentBlocks))
					vpos = 5
					printScreen(vpos,hpos,printText)
					printText = "Blocks still to move: %d " % (blocksToMove - migratedBlocks)
					vpos = 6
					printScreen(vpos,hpos,printText)
			
		# check remaining number of record in blockmove table
		cur.execute("SELECT count(*) from blockmove")
		record = cur.fetchone()		
					
def retrieve_blockinfo(total_blocks):
	printText = "Get block info for %d blocks..." % (total_blocks)
	hpos = 0
	vpos = 0
	printScreen(vpos,hpos,printText)
	apipath="/sys/block/"+basename+"/tier/show_blockinfo"
	blockstat = "blockstat.txt"
	if logs:
		logf = open(blockstat,"w+",0)
		logfString = "Blockstat,CTIME,%d\n" % (CTIME)
		logfString += "BlockNr,Device,Offset,Atime,Reads,Writes\n"
		logf.write(logfString) 

	maxdev = read_maxdev()
	blocksUsed = 0
	readcount = 0
	writecount = 0
	device = 0
	blockGrace = 0
	percentBlocks = int(total_blocks/100)
	printText =  "Maxdev: %d" % (maxdev)
	vpos = 1
	printScreen(vpos,hpos,printText)
	while device < maxdev+1:
		# initialize the arrays for general statistics
		tierUsedBlocks[device] = int(0)
		tierReadCounts[device] = int(0)
		tierWriteCounts[device] = int(0)
		device += 1

	#try:
	fp=open(apipath,"r+")
	curblock=0
	while curblock < total_blocks:
		fp.write(str(curblock)+" paged\n")
		if (curblock + 20) < total_blocks:
			maxIndex = 20
		else:
			maxIndex = total_blocks - curblock
		fp.seek(0)
		blockinfo=fp.read()
		pagedBinfo=blockinfo.split( '\n' )
		blockIndex = 0
		
		while blockIndex < maxIndex:
			binfo=pagedBinfo[blockIndex].split( ',' )
			device = int(binfo[0])
			# gather statistics for read/write averages per tier
			if device > -1:
				tierUsedBlocks[device] += 1
				tierReadCounts[device] += int(binfo[3])
				tierWriteCounts[device] += int(binfo[4])
				tierBlockDevice[curblock] = int(binfo[0])
				tierBlockOffset[curblock] = int(binfo[1])
				tierBlockAtime[curblock] = int(binfo[2])
				tierBlockReads[curblock] = int(binfo[3])
				tierBlockWrites[curblock] = int(binfo[4])
				tierBlockBusy = tierBlockReads[curblock] + tierBlockWrites[curblock]
				tierBlockList[blocksUsed] = int(curblock)
				blocksUsed += 1

				if sql:
					cur.execute("SELECT * FROM meta WHERE blocknr = ?",(curblock,))
					record = cur.fetchone()
					if record == None:
						# insert mode
						cur.execute("INSERT INTO meta VALUES(?, ?, ?,?,?)", \
							(curblock,tierBlockAtime[curblock],device,tierBlockBusy,0))
					else:
						# update mode
						freq = float((tierBlockBusy-record[3])/(CTIME-record[1]))
						# if the current block's grace flag is unset and it's frequency > 0
						# or if the block's freq is 0 and it's located on any non-last tier
						# then insert it into the blockmove table
						blockGrace = record[4]
						if logs:
							if device == 1:
								logfString ="Freq: %d -> %f,%d\n" % \
									(curblock,freq,blockGrace)
								logf.write(logfString)
						if blockGrace == 1 and ((tierBlockAtime[curblock]+blockGracePeriod) < CTIME):
							if logs:
								logString = "Graceflag deleted: %d\n" % curblock
								logf.write(logString)
							blockGrace = 0
							# update meta table
							cur.execute("UPDATE meta set grace=? where blocknr=?",(0,curblock))
						if blockGrace == 0: 
							if (freq > 0 and tierBlockDevice[curblock] > 0):
								# consider block for migration, put into blockmove table
								cur.execute("INSERT INTO blockmove VALUES(?, ? ,?)", \
									(curblock,freq,tierBlockDevice[curblock]))
								# update meta table
								cur.execute("UPDATE meta set atime=?, hits=?,grace=? \
									where blocknr=?",(tierBlockAtime[curblock],tierBlockBusy,\
									0, curblock))
							elif (freq == 0 and tierBlockDevice[curblock] < maxdev-1):
								# consider block for migration, put into blockmove table
								cur.execute("INSERT INTO blockmove VALUES(?, ? ,?)", \
									(curblock,freq,tierBlockDevice[curblock]))
								# update meta table
								cur.execute("UPDATE meta set atime=?, hits=?,grace=? \
									where blocknr=?",(tierBlockAtime[curblock],tierBlockBusy,\
									0, curblock))

				if logs:
					if device == 0 or device ==1:
						logfString = "%d,%d,%d,%d,%d,%d\n" % (curblock,device, \
						tierBlockOffset[curblock],CTIME-tierBlockAtime[curblock],tierBlockReads[curblock],tierBlockWrites[curblock])
						logf.write(logfString)
			if (percentBlocks != 0):
				if (curblock % percentBlocks == 0):
					printText =  "Processing... %d %% done" % (int(curblock/percentBlocks))
					vpos = 2
					printScreen(vpos,hpos,printText)

			curblock += 1
			blockIndex += 1

	fp.close()
	if sql:
		con.commit()
	device = 0
	maxdev = read_maxdev()
	while device < maxdev:
		vpos = 4 + (device * 5)
		printText =  "Tier %d statistics" % (device)
		printScreen(vpos, hpos,printText)
		vpos = 5 + (device * 5)
		printText =  "Blocks used   : %d" % (tierUsedBlocks[device])
		printScreen(vpos,hpos,printText)
		vpos = 6 + (device * 5)
		if ( tierUsedBlocks[device] != 0 ):
			AVGread[device] = int(tierReadCounts[device]/tierUsedBlocks[device])
		else:
			AVGread[device] = 0
		printText =  "Blocks read   : %d, avg: %d" % (tierReadCounts[device], \
			AVGread[device])
		printScreen(vpos,hpos,printText)
		vpos = 7 + (device * 5)
		if ( tierUsedBlocks[device] != 0 ):
			AVGwrite[device] = int(tierWriteCounts[device]/tierUsedBlocks[device])
		else:
			AVGwrite[device] = 0
		printText =  "Blocks written: %d, avg: %d " % (tierWriteCounts[device], \
			AVGwrite[device])
		printScreen(vpos,hpos,printText)
		device += 1

	if migrate:
		if sql:
			do_sql_migration()
		else:
			do_migration(blocksUsed)
	#except:
	#	print "Unexpected error: ", RET_SYSERR
	#	return RET_SYSERR
	#else:
	return RET_OK

#############################################
##            MAIN starts here             ##
#############################################

# check if there's another migration running
checkPID()

# parse command line
parser = OptionParser()
parser.add_option("-b","--basename", action="store", type="string",
	help="btier volume to work on")
parser.add_option("-m", "--migrate", action="store_true",
	help="enable migration on btier volume")
parser.add_option("-l", "--logs", action="store_true",
	help="enable logging for blockstat and migration")
parser.add_option("-v", "--verbose", action="store_true",
	help="print verbose statistics")
parser.add_option("-s", "--sql", action="store_true",
	help="use sql statistics")
parser.add_option("-r", "--resetDB", action="store_true",
	help="reset Sqlite DB")
parser.add_option("-f","--dbfile", action="store", type="string",
	help="Name of Sqlite DB file")

# parse cmd line arguments
(options, args) = parser.parse_args()

basename = options.basename
if not basename:
	#assume sdtiera
	basename = "sdtiera"

if not dbfile:
	dbfile = basename +".db"
	
# First check specified arguments
if "/dev/" in basename:
	print "Don't specify the path to the btier volume, but only the volume itself"
	exit(RET_OK)

if options.migrate:
	migrate = 1
else:
	migrate = 0

if options.logs:
	logs = 1
else:
	logs = 0
	
if options.verbose:
	verbose = 1
else:
	verbose = 0

if options.sql:
	sql = 1
else:
	sql = 0

if options.resetDB:
	resetDB = 1
else:
	resetDB = 0
	

# Check the number of blocks that make up the
# device
device="/dev/"+basename
check_valid_device(device)
total_blocks=read_total_blocks()

if 0 == total_blocks:
	print "Failed to retrieve device size from sysfs"
	exit(RET_SYSFS)

if verbose:
	# initialize curses
	screen = curses.initscr()
	curses.curs_set(0)
	curses.noecho()
	screen.keypad(1)
	curses.cbreak()

CTIME=get_ctime()

# if sql migration has been chosen, check if the db exists
if sql:
	con = con_open(dbfile)
	cur = sql_open(con)
	checkTable = sql_check_tables(cur)
	if checkTable:
		resetDB = 1
	sql_drop_create(cur)

retrieve_blockinfo(total_blocks)

exitScript()
# de-initilize curses
exit(RET_OK)


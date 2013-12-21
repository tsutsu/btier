#!/usr/bin/python
#############################################
# show_block_details.py                     #
#                                           #
# A simple python program that retrieves    #
# btier block placement metadata            #
# And optionally stores the data in sqlite  #
# EXAMPLE CODE                              #
#############################################
import os
import sys,errno
import stat
import sqlite3 as lite
import time
import datetime

from stat import *
from sys import argv

RET_OK=0
RET_NODEV=-1
RET_SYSFS=-2
RET_SYSERR=-3
RET_MIGERR=-4

# Move to a higher tier when the block is hit 1.5 times more then avg
THRESHOLD_UP=1.5    
# Move to a lower tier when the block is hit 0.5 times less then avg
THRESHOLD_DOWN=0.5

# MAXAGE in seconds
MAXAGE=14400

DB="btier.db"

def print_file(fp):
    fp.seek(0);
    print fp.read()

def usage():
    print "Usage : %s sdtierX [-sql] [-migrate]" % argv.pop(0)
    print "Please note : -migrate requires -sql"
    exit()

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
        return int(blocks)
    else:
        return int(blocks)

def sql_drop_create(cur):
    try:
        cur.execute("DROP TABLE IF EXISTS meta") 
        #BLOCKNR,DEVICE,OFFSET,ATIME,READCOUNT,WRITECOUNT
        cur.execute("CREATE TABLE meta(blocknr LONG LONG INT PRIMARY KEY, device INT, \
                     offset LONG LONG INT, atime UNSIGNED INT, \
                     readcount UNSIGNED INT, writecount UNSIGNED int)")
        cur.execute("CREATE INDEX meta_device_idx ON meta (device)")
    except:
        return
    else:
        return


def con_open():
    try:
        con = lite.connect(DB)
    except slite.Error, e:
        print "Error %s:" % e.args[0]
        exit(DB_ERR)
    else:
        return con


def sql_open(con):
    try:
        with con:
             cur = con.cursor()
        sql_drop_create(cur)
    except slite.Error, e:
        print "Error %s:" % e.args[0]
        exit(DB_ERR)
    else:
        return cur

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

def migrate_up(cur,blocknr,device):
    if device == 0:
       return -1
    cur.execute("SELECT blocknr FROM meta where blocknr = ?  \
                 AND device = ? AND writecount > ? \
                 * (SELECT AVG(writecount) from meta \
                 where device = ?)", (blocknr,device, THRESHOLD_UP, device))
    record = cur.fetchone()
    if record == None:
       return -1
    blocknr=int(record[0])
    newdev = int(device) - 1
    while True:
          res=migrate_block(blocknr, newdev)
          if res != errno.EAGAIN:
             break
          time.sleep (1/5)

    print "Migrated blocknr %d from device %d to device %d high hits"  \
           % (blocknr, device, newdev)
    return 0

def migrate_down(cur,blocknr,device):
    cur.execute("SELECT blocknr FROM meta WHERE blocknr = ? \
                AND device = ? AND writecount > ? \
                * (SELECT AVG(writecount) from meta \
                where device = ?)", (blocknr, device, THRESHOLD_DOWN, device))
    record = cur.fetchone()
    if record == None:
       return -1
    blocknr=int(record[0])
    newdev=device + 1
    while True:
          res=migrate_block(blocknr, newdev)
          if res != errno.EAGAIN:
             break
          time.sleep (1/5)

    print "Migrated blocknr %d from device %d to device %d low hits" \
           % (blocknr, device, newdev)
    return 0

def migrate_down_age(cur,blocknr,device):
    tdev=device + 1
    grace=CTIME-(MAXAGE*tdev)
    cur.execute("SELECT blocknr FROM meta WHERE blocknr = ? \
                 AND device = ? AND atime < ? ", (blocknr,device, grace))
    record = cur.fetchone()
    if record == None:
        return -1
    blocknr=record[0]
    migrate_block(blocknr, device + 1)
    print "Migrated blocknr %d from device %d to device %d because of age" \
           % (blocknr, device, device+1)
    return 0

def do_migration(cur):
    maxdev=read_maxdev()
    blocknr=0
    while blocknr < total_blocks:
        cur.execute("SELECT device FROM meta WHERE device != -1 \
                     AND blocknr = ? ",  [ blocknr ] )
        record = cur.fetchone()
        if record == None:
              blocknr+=1
              continue
        device=int(record[0])
        res=-1
        res=migrate_up(cur,blocknr,device)
        if res != 0:
           if device < maxdev - 1:
              res=migrate_down(cur,blocknr,device)
              if res != 0:
                 res=migrate_down_age(cur,blocknr,device)
        blocknr+=1

def write_sql(cur,blocknr,blockinfo):
    binfo=blockinfo.split( ',' )
    device=binfo.pop(0)
    offset=int(binfo.pop(0))
    atime=int(binfo.pop(0))
    readcount=int(binfo.pop(0))
    writecount=int(binfo.pop(0))
    cur.execute("INSERT INTO meta VALUES(?, ?, ?, ?, ?, ?)", \
               (blocknr,device,offset,atime,readcount,writecount))

def retrieve_blockinfo(total_blocks):
       apipath="/sys/block/"+basename+"/tier/show_blockinfo"
       con=con_open()
       cur=sql_open(con)
       try:
          fp=open(apipath,"r+")
          curblock=0
          while curblock < total_blocks:
               fp.write(str(curblock)+"\n")
               fp.seek(0)
               blockinfo=fp.read()
               if sql:
                  write_sql(cur,curblock,blockinfo)
               else:
                  print str(curblock)+" "+blockinfo,
               curblock=curblock + 1
          fp.close()
          con.commit()
          if migrate:
             do_migration(cur)
          con.close()
       except:
          return RET_SYSERR
       else:
          return RET_OK

#############################################
##            MAIN starts here             ##
#############################################
sql=0
migrate=0

t=datetime.datetime.now()
CTIME=time.mktime(t.timetuple())

argc=len(argv) - 1
if argc < 1:
   usage()

basename = argv.pop(1)

# First check specified arguments
if "/dev/" in basename:
   usage()

if argc >= 2:
   sqls=argv.pop(1)
   if "-sql" in sqls:
      sql=1
   else:
      usage()

if argc == 3:
   migs=argv.pop(1)
   if "-migrate" in migs:
       migrate=1
   else:
       usage()    

# Check the number of blocks that make up the
# device
device="/dev/"+basename
check_valid_device(device)
total_blocks=read_total_blocks()

if 0 == total_blocks:
   print "Failed to retrieve device size from sysfs"
   exit(RET_SYSFS) 

retrieve_blockinfo(total_blocks)

exit(RET_OK)

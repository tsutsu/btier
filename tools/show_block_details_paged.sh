#!/bin/bash

echo "********************************************************************"
echo "*        THIS SCRIPT SHOWS THE METADATA OF EVERY BLOCK             *"
echo "* DEVICE -1 means that this block has not yet been allocated       *"
echo "* OUTPUT FORMAT :BLOCKNRDEVICE,OFFSET,ATIME,READCOUNT,WRITECOUNT   *"
echo "********************************************************************"
n=0

PAGESIZE=20
lastblock=$(cat /sys/block/sdtiera/tier/size_in_blocks) >/dev/null
while [ $n -lt $lastblock ]
do
  echo "$n paged" >/sys/block/sdtiera/tier/show_blockinfo
  i=0
  line=$(cat /sys/block/sdtiera/tier/show_blockinfo) >/dev/null
  for l in $line
  do
      let nr=$n+$i
      [ $nr -eq $lastblock ] && break;
      echo $nr $l
      let i++
  done
  let n+=20
done

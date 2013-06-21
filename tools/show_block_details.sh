#!/bin/bash

echo "********************************************************************"
echo "*        THIS SCRIPT SHOWS THE METADATA OF EVERY BLOCK             *"
echo "* DEVICE -1 means that this block has not yet been allocated       *"
echo "* OUTPUT FORMAT :BLOCKNRDEVICE,OFFSET,ATIME,READCOUNT,WRITECOUNT   *"
echo "********************************************************************"
n=0
while [ $n -lt $(cat /sys/block/sdtiera/tier/size_in_blocks) ]
do
  echo $n >/sys/block/sdtiera/tier/show_blockinfo
  echo $n $(cat /sys/block/sdtiera/tier/show_blockinfo)
  let n++
done

#!/bin/sh
basedir=$(pwd)

usage()
{
   echo "Usage $0 /path/to/kernel_sources_to_patch"
   exit 1
}

if [ -z $1 ]; then usage; fi

dest=$1/drivers/block

if [ ! -d ${dest} ]
   then
      echo "Failed to stat ${dest}"
      exit 1
fi
make clean
cp -ar kernel/btier ${dest}
cp in-tree/Makefile ${dest}/btier
cp in-tree/Kconfig  ${dest}/btier
cd $1
patch -p0 <${basedir}/in-tree/Kconfig.patch
exit 0

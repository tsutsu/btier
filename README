The easiest way to install btier is to make sure that 
kernel sources have been installed on the system and
to simply type : make in the directory where you unpacked
the btier sources.

However it is also possible to patch your kernel sources
so that they contain btier. In this case 'make menuconfig'
will show btier as an option under:
Device drivers ->Block devices

To patch your kernel sources so that you can build btier
in-tree use patch-in-tree.sh as follows:

./patch-in-tree.sh /usr/src/linux-3.8

Change /usr/src/linux-3.8 to the path where your kernel
sources reside.

To build only the btier_setup cli:
make clionly

KDIR := /lib/modules/$(shell uname -r)/build
PWD := $(shell pwd)
CC := gcc -O2

default:
	$(MAKE) -Wall -C $(KDIR) M=$(PWD)/kernel/btier modules
	$(CC) -D_FILE_OFFSET_BITS=64 cli/btier_setup.c -o cli/btier_setup 
	$(CC) -D_FILE_OFFSET_BITS=64 cli/btier_inspect.c -o cli/btier_inspect 
	$(CC) tools/writetest.c -o tools/writetest
	$(CC) tools/show_block_details.c -o tools/show_block_details
clionly:
	$(CC) -D_FILE_OFFSET_BITS=64 cli/btier_setup.c -o cli/btier_setup 
clean:
	rm -f cli/btier_setup
	rm -f cli/btier_inspect
	rm -f kernel/btier/*.c~ kernel/btier/*.o kernel/btier/*.ko 
	rm -f kernel/btier/btier.mod.c kernel/btier/modules.order 
	rm -f kernel/btier/Module.symvers kernel/btier/.*.cmd 
	rm -f kernel/btier/btier.ko.unsigned
	rm -rf kernel/btier/.tmp_versions
	rm -f kernel/btier/Makefile.xen
	rm -f tools/btier.db
	rm -f tools/show_block_details
	rm -f tools/writetest
pretty: 
	cd kernel/btier;$(KDIR)/scripts/Lindent *.c
	cd kernel/btier;$(KDIR)/scripts/Lindent *.h
	cd kernel/btier;rm -f *.c~
	cd kernel/btier;rm -f *.h~
	cd cli;$(KDIR)/scripts/Lindent *.c
	cd cli;rm -f *.c~
install:
	install -D -m 755 kernel/btier/btier.ko $(DESTDIR)/lib/modules/`uname -r`/kernel/drivers/block/btier.ko
	install -D -m 755 -s cli/btier_setup $(DESTDIR)/sbin/btier_setup
	install -D -m 755 -s cli/btier_inspect $(DESTDIR)/sbin/btier_inspect
	install -D -m 755 rc/btier $(DESTDIR)/etc/init.d/btier
	install -D -m 600 rc/bttab $(DESTDIR)/etc/bttab_example
	gzip -c man/btier_setup.1 > man/btier_setup.1.gz
	gzip -c man/btier_inspect.1 > man/btier_inspect.1.gz
	install -D -m 644 man/btier_setup.1.gz $(DESTDIR)/usr/share/man/man1/btier_setup.1.gz
	install -D -m 644 man/btier_inspect.1.gz $(DESTDIR)/usr/share/man/man1/btier_inspect.1.gz
	depmod -a
uninstall:
	rm $(DESTDIR)/sbin/btier_setup
	rm $(DESTDIR)/etc/init.d/btier
	rm $(DESTDIR)/usr/share/man/man1/btier_setup.1.gz
	rm $(DESTDIR)/usr/share/man/man1/btier_inspect.1.gz
	rm $(DESTDIR)/lib/modules/`uname -r`/kernel/drivers/block/btier.ko
